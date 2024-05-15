FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.66-rust-1.78-slim-bookworm AS chef
WORKDIR /app

FROM --platform=$BUILDPLATFORM chef AS planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY contract-client ./contract-client
COPY messages ./messages
COPY transport ./transport

RUN cargo chef prepare --recipe-path recipe.json

FROM --platform=$BUILDPLATFORM chef as builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml .
COPY Cargo.lock .
COPY contract-client ./contract-client
COPY transport ./transport

FROM builder as builder

RUN cargo build --release --bin bootnode --bin keygen

FROM --platform=$BUILDPLATFORM debian:bookworm-slim as base

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install ca-certificates net-tools

FROM --platform=$BUILDPLATFORM base as bootnode
COPY --from=builder /app/target/release/bootnode /usr/local/bin/bootnode
COPY --from=builder /app/target/release/keygen /usr/local/bin/keygen
CMD ["bootnode"]

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"

COPY p2p_healthcheck.sh ./healthcheck.sh
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh
