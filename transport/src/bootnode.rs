use std::time::Duration;

use clap::Parser;
use env_logger::Env;
use futures::{stream::FusedStream, StreamExt};
use libp2p::quic::MtuDiscoveryConfig;
use libp2p::{
    autonat,
    gossipsub::{self, MessageAuthenticity},
    identify,
    kad::{self, store::MemoryStore, Mode},
    ping, relay,
    swarm::{dial_opts::DialOpts, SwarmEvent},
    PeerId, SwarmBuilder,
};
use libp2p_connection_limits::ConnectionLimits;
use libp2p_swarm_derive::NetworkBehaviour;
use tokio::signal::unix::{signal, SignalKind};

use subsquid_network_transport::transport::MTU_DISCOVERY_MAX;
use subsquid_network_transport::{
    cli::{BootNode, TransportArgs},
    util::{addr_is_reachable, get_keypair},
    Keypair,
};

#[derive(Parser)]
#[command(version, author)]
struct Cli {
    #[command(flatten)]
    transport: TransportArgs,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    relay: relay::Behaviour,
    gossipsub: gossipsub::Behaviour,
    ping: ping::Behaviour,
    autonat: autonat::Behaviour,
    conn_limits: libp2p_connection_limits::Behaviour,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logging and parse arguments
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let cli = Cli::parse().transport;
    let listen_addrs = cli.listen_addrs();
    let keypair = get_keypair(cli.key).await?;
    let local_peer_id = PeerId::from(keypair.public());
    log::info!("Local peer ID: {local_peer_id}");

    // Prepare behaviour & transport
    let autonat_config = autonat::Config {
        timeout: Duration::from_secs(60),
        throttle_clients_global_max: 64,
        throttle_clients_peer_max: 16,
        ..Default::default()
    };
    let behaviour = |keypair: &Keypair| Behaviour {
        identify: identify::Behaviour::new(
            identify::Config::new("/subsquid/0.0.1".to_string(), keypair.public())
                .with_interval(Duration::from_secs(60))
                .with_push_listen_addr_updates(true),
        ),
        kademlia: kad::Behaviour::with_config(
            local_peer_id,
            MemoryStore::new(local_peer_id),
            Default::default(), // Same here, this is the default IPFS network.
        ),
        relay: relay::Behaviour::new(local_peer_id, Default::default()),
        gossipsub: gossipsub::Behaviour::new(
            MessageAuthenticity::Signed(keypair.clone()),
            Default::default(),
        )
        .unwrap(),
        ping: ping::Behaviour::new(Default::default()),
        autonat: autonat::Behaviour::new(local_peer_id, autonat_config),
        conn_limits: libp2p_connection_limits::Behaviour::new(
            ConnectionLimits::default().with_max_established_per_peer(Some(3)),
        ),
    };

    // Start the swarm
    let mut mtu_config = MtuDiscoveryConfig::default();
    mtu_config.upper_bound(*MTU_DISCOVERY_MAX);
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_quic_config(|config| config.with_mtu_discovery_config(mtu_config))
        .with_dns()?
        .with_behaviour(behaviour)
        .expect("infallible")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    for listen_addr in listen_addrs {
        log::info!("Listening on {}", listen_addr);
        swarm.listen_on(listen_addr)?;
    }
    for public_addr in cli.p2p_public_addrs {
        log::info!("Adding public address {public_addr}");
        swarm.add_external_address(public_addr);
    }

    swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server)); // This is unnecessary if you are setting an external address.

    // Connect to other boot nodes
    for BootNode { peer_id, address } in
        cli.boot_nodes.into_iter().filter(|node| node.peer_id != local_peer_id)
    {
        log::info!("Connecting to boot node {peer_id} at {address}");
        swarm.behaviour_mut().kademlia.add_address(&peer_id, address.clone());

        // By default, the `Swarm` will consult all behaviours for addresses for a peer.
        // If you specify `.addresses` here, it won't.
        // Unless you want to be specific about _just_ using that one address, I'd leave it off and just specify the peer ID.
        swarm.dial(DialOpts::peer_id(peer_id).addresses(vec![address]).build())?;
    }

    // As mentioned elsewhere, bootstrap is now included automatically.
    if swarm.behaviour_mut().kademlia.bootstrap().is_err() {
        log::warn!("No peers connected. Cannot bootstrap kademlia.")
    }

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    // Have you looked at the implementation? :)
    // This statically returns `false` so you can also just use a `loop`!
    while !swarm.is_terminated() {
        let event = tokio::select! {
            event = swarm.select_next_some() => event,
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        };
        log::debug!("Swarm event: {event:?}");
        if let SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
            peer_id,
            info: identify::Info { listen_addrs, .. },
        })) = event
        {
            listen_addrs.into_iter().filter(addr_is_reachable).for_each(|addr| {
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            });
        }
    }

    log::info!("Bootnode stopped");
    Ok(())
}
