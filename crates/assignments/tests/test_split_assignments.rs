mod common;

#[cfg(feature = "builder")]
use rand::{rngs::StdRng, SeedableRng};
#[cfg(feature = "builder")]
use sqd_assignments::{
    WorkerAssignmentBuilder, WorkerAssignmentChunkBuilder, WorkerDatasetBuilder,
};

/// A deterministically seeded builder with no write schema registered yet.
#[cfg(feature = "builder")]
fn test_builder() -> WorkerAssignmentBuilder<StdRng> {
    WorkerAssignmentBuilder::new_with_rng("test-secret", StdRng::seed_from_u64(0))
}

/// The dataset the chunks below are staged into, named once rather than by every chunk.
#[cfg(feature = "builder")]
fn test_dataset(builder: &mut WorkerAssignmentBuilder<StdRng>) -> WorkerDatasetBuilder<'_, StdRng> {
    builder.new_dataset("s3://solana-mainnet-2", "https://solana-mainnet-2.sqd-datasets.io")
}

/// A chunk builder with everything but the write-schema fields set.
#[cfg(feature = "builder")]
fn staged_chunk<'a>(
    dataset: &'a mut WorkerDatasetBuilder<'_, StdRng>,
) -> WorkerAssignmentChunkBuilder<'a, StdRng> {
    dataset
        .new_chunk()
        .id("0221000000/0221000000-0221000649-BQJdx")
        .block_range(221000000..=221000649)
        .size(1000000)
        .worker_indexes(&[0])
}

#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_worker_assignment_round_trip() {
    let mut builder = test_builder().check_continuity(false);
    builder.register_write_schema(7, &["blocks", "logs", "transactions"]).unwrap();
    // Registered after 7 but sorts before it: rosters must reach the blob id-sorted, which is
    // what `lookup_by_key` binary-searches on.
    builder.register_write_schema(3, &["blocks", "traces"]).unwrap();

    let mut dataset = test_dataset(&mut builder);
    dataset
        .new_chunk()
        .id("0221000000/0221000000-0221000649-BQJdx")
        .block_range(221000000..=221000649)
        .size(1000000)
        .write_schema_id(7)
        .tables_present(&["blocks", "transactions"])
        .unwrap()
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset
        .new_chunk()
        .id("0221000000/0221000650-0221001549-AuRE1")
        .block_range(221000650..=221001549)
        .size(1000000)
        .write_schema_id(7)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    // Same tables as the first chunk: two bitmaps with identical bits, resolved independently.
    dataset
        .new_chunk()
        .id("0221000000/0221001550-0221001999-C7pQz")
        .block_range(221001550..=221001999)
        .size(1000000)
        .write_schema_id(7)
        .tables_present(&["blocks", "transactions"])
        .unwrap()
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset.finish().unwrap();

    let keypair = common::get_test_keypair();
    let peer_id = keypair.public().to_peer_id();
    let timestamp = 1750000000;
    builder.add_worker_with_timestamp(peer_id, sqd_assignments::WorkerStatus::Ok, timestamp);

    let bytes = builder.finish();
    let assignment = sqd_assignments::WorkerAssignment::from_owned(bytes).unwrap();

    let dataset = assignment.get_dataset("s3://solana-mainnet-2").unwrap();
    assert_eq!(dataset.last_block(), 221001999);
    assert_eq!(dataset.chunk_count(), 3);

    let worker = assignment.get_worker(&peer_id).unwrap();
    assert_eq!(worker.status(), sqd_assignments::WorkerStatus::Ok);
    let headers = worker.decrypt_headers(&keypair).unwrap();
    assert_eq!(headers.get("worker-id"), Some(&peer_id.to_string()));

    let chunks = worker.iter_chunks().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].id().unwrap(), "0221000000/0221000000-0221000649-BQJdx");
    assert_eq!(
        dataset.base_url(),
        "https://solana-mainnet-2.sqd-datasets.io",
        "the base url is the dataset's, named once"
    );
    assert_eq!(chunks[0].write_schema_id(), 7);
    assert_eq!(
        assignment.chunk_tables(chunks[0]).unwrap().collect::<Vec<_>>(),
        vec!["blocks", "transactions"]
    );
    assert!(chunks[1].tables_present().is_none(), "unset tables_present means all present");
    assert_eq!(
        assignment.chunk_tables(chunks[1]).unwrap().collect::<Vec<_>>(),
        vec!["blocks", "logs", "transactions"],
        "an unset bitmap resolves to the write schema's whole roster"
    );
    assert_eq!(
        assignment.chunk_tables(chunks[2]).unwrap().collect::<Vec<_>>(),
        vec!["blocks", "transactions"],
        "a chunk sharing another's bitmap resolves to the same tables"
    );
    assert_eq!(
        assignment.get_write_schema(7).unwrap().tables().iter().collect::<Vec<_>>(),
        vec!["blocks", "logs", "transactions"]
    );
    assert_eq!(
        assignment.get_write_schema(3).unwrap().tables().iter().collect::<Vec<_>>(),
        vec!["blocks", "traces"],
        "every registered roster is looked up by id, whatever order it was registered in"
    );
    assert!(assignment.get_write_schema(8).is_none());

    assert_eq!(chunks[0].version(), 0);
    assert!(
        dataset.generations().is_none(),
        "a dataset of version-0 chunks stores no prefixes"
    );
    assert_eq!(
        chunks[0].url().unwrap(),
        "https://solana-mainnet-2.sqd-datasets.io/0221000000/0221000000-0221000649-BQJdx"
    );
}

/// A batch job's rewrite of a chunk keeps the chunk id and swaps the prefix it hangs under.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_chunk_generations_round_trip() {
    let mut builder = test_builder().check_continuity(false);
    builder.register_write_schema(7, &["blocks"]).unwrap();

    // An untouched dataset, staged first because `get_dataset` binary-searches on id.
    let mut untouched =
        builder.new_dataset("s3://ethereum-mainnet", "https://ethereum-mainnet.sqd-datasets.io");
    untouched
        .new_chunk()
        .id("0000000000/0000000000-0000000999-274f02d8")
        .block_range(0..=999)
        .size(1000)
        .write_schema_id(7)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    untouched.finish().unwrap();

    // Generations are registered on the dataset they cover, so these reach this one only.
    let mut dataset = test_dataset(&mut builder);
    dataset.register_generation(4, "_bf/01HR2A9B4C6D8E0F2G4H6J8K0M").unwrap();
    // Registered after 4 but sorts before it: entries must reach the blob version-sorted, which is
    // what `lookup_by_key` binary-searches on.
    dataset.register_generation(2, "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9").unwrap();

    staged_chunk(&mut dataset).write_schema_id(7).finish().unwrap();
    staged_chunk(&mut dataset)
        .id("0221000000/0221000650-0221001549-AuRE1")
        .block_range(221000650..=221001549)
        .write_schema_id(7)
        .version(2)
        .finish()
        .unwrap();
    dataset.finish().unwrap();

    builder.add_worker_with_timestamp(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
        1750000000,
    );
    let assignment = sqd_assignments::WorkerAssignment::from_owned(builder.finish()).unwrap();

    let dataset = assignment.get_dataset("s3://solana-mainnet-2").unwrap();
    assert_eq!(dataset.get_generation(2).unwrap().base_url(), "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9");
    assert_eq!(
        dataset.get_generation(4).unwrap().base_url(),
        "_bf/01HR2A9B4C6D8E0F2G4H6J8K0M",
        "every registered generation is looked up by version, whatever order it was registered in"
    );
    assert!(dataset.get_generation(0).is_none(), "version 0 has no prefix");
    assert!(dataset.get_generation(3).is_none());

    // Version 0 still means the ingested copy in a dataset that has generations.
    assert_eq!(dataset.chunk(0).unwrap().version(), 0);
    assert_eq!(
        dataset.chunk(0).unwrap().url().unwrap(),
        "https://solana-mainnet-2.sqd-datasets.io/0221000000/0221000000-0221000649-BQJdx",
        "version 0 hangs straight off the dataset base url"
    );
    assert_eq!(
        dataset.chunk(1).unwrap().url().unwrap(),
        "https://solana-mainnet-2.sqd-datasets.io/_bf/01HQZK3M7X8P2NVWTC4RYFGDS9\
         /0221000000/0221000650-0221001549-AuRE1",
        "a non-zero version puts its generation's prefix in between"
    );

    // A worker's chunks arrive detached from their dataset; the ref is what traces them back.
    let worker = assignment.get_worker_by_index(0);
    let (chunk_ref, _) = worker
        .iter_chunks_with_ref()
        .find(|(_, chunk)| chunk.id().as_deref() == Some("0221000000/0221000650-0221001549-AuRE1"))
        .expect("the rewritten chunk is assigned to worker 0");
    assert_eq!(
        assignment.get_dataset_by_ref(chunk_ref).unwrap().id(),
        "s3://solana-mainnet-2",
        "a chunk's dataset is recovered from the ref, not from the chunk"
    );
    assert_eq!(
        assignment.chunk_url(chunk_ref).unwrap(),
        "https://solana-mainnet-2.sqd-datasets.io/_bf/01HQZK3M7X8P2NVWTC4RYFGDS9\
         /0221000000/0221000650-0221001549-AuRE1"
    );
    let (paired_dataset, paired_chunk) = worker
        .iter_chunks_with_dataset()
        .find(|(_, chunk)| chunk.id().as_deref() == Some("0221000000/0221000650-0221001549-AuRE1"))
        .expect("the same chunk, paired with its dataset");
    assert_eq!(paired_chunk.url().unwrap(), assignment.chunk_url(chunk_ref).unwrap());
    assert_eq!(paired_dataset.id(), "s3://solana-mainnet-2", "paired with its own dataset");

    let other = assignment.get_dataset("s3://ethereum-mainnet").unwrap();
    assert!(
        other.generations().is_none(),
        "generations are staged per dataset, not carried over by the builder"
    );
}

/// A dataset is emitted by `finish` and by nothing else. Dropping one, or having it rejected, must
/// leave no trace on the assignment or on whatever is opened next — the staged chunks go, and so
/// does the last block they set, which would otherwise break the next dataset's continuity.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_an_abandoned_dataset_emits_nothing() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks"]).unwrap();

    // Rejected: no chunk was staged.
    let empty = builder.new_dataset("s3://aaa-empty", "https://aaa-empty.sqd-datasets.io");
    let error = empty.finish().expect_err("a dataset with no chunks has no columns");
    assert!(error.to_string().contains("At least one chunk"), "unexpected error: {error}");

    // Abandoned: chunks staged, `finish` never called.
    {
        let mut dropped =
            builder.new_dataset("s3://bbb-dropped", "https://bbb-dropped.sqd-datasets.io");
        staged_chunk(&mut dropped).write_schema_id(7).finish().unwrap();
    }

    // Covers the same blocks as the abandoned dataset's chunk, so a leaked `last_block` would trip
    // continuity here rather than passing quietly.
    let mut dataset = test_dataset(&mut builder);
    staged_chunk(&mut dataset).write_schema_id(7).finish().unwrap();
    dataset.finish().unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::WorkerAssignment::from_owned(builder.finish()).unwrap();
    assert_eq!(assignment.datasets().len(), 1, "only the finished dataset reaches the blob");
    let dataset = assignment.get_dataset("s3://solana-mainnet-2").expect("the finished dataset");
    assert_eq!(dataset.chunk_count(), 1, "the abandoned dataset's chunk is not carried over");
}

#[cfg(feature = "builder")]
#[test]
fn test_register_generation_rejects_version_zero() {
    let mut builder = test_builder();
    let error = test_dataset(&mut builder)
        .register_generation(0, "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9")
        .expect_err("giving version 0 a prefix would contradict what makes a chunk ingested");

    assert!(
        error.to_string().contains("needs no generation entry"),
        "unexpected error: {error}"
    );
}

#[cfg(feature = "builder")]
#[test]
fn test_register_generation_rejects_conflicting_base_url() {
    let mut builder = test_builder();
    let mut dataset = test_dataset(&mut builder);
    dataset.register_generation(2, "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9").unwrap();

    let error = dataset
        .register_generation(2, "_bf/01HR2A9B4C6D8E0F2G4H6J8K0M")
        .expect_err("one version cannot name two prefixes");

    assert!(error.to_string().contains("different base url"), "unexpected error: {error}");
}

/// Without a matching entry the worker has no prefix to download from, so the version is dangling.
#[cfg(feature = "builder")]
#[test]
fn test_finish_rejects_unregistered_generation() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks"]).unwrap();

    let mut dataset = test_dataset(&mut builder);
    let error = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .version(2)
        .finish()
        .expect_err("a chunk may not reference a generation with no entry in its dataset");

    assert!(
        error.to_string().contains("generation 2 is not registered"),
        "unexpected error: {error}"
    );
}

#[cfg(feature = "builder")]
#[test]
fn test_tables_present_rejects_table_outside_write_schema() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks", "transactions"]).unwrap();

    let mut dataset = test_dataset(&mut builder);

    let Err(error) = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .tables_present(&["blocks", "traces"])
    else {
        panic!("a table outside the write schema's roster must be rejected");
    };

    assert!(
        error.to_string().contains("'traces' is absent from write schema 7's roster"),
        "unexpected error: {error}"
    );
}

#[cfg(feature = "builder")]
#[test]
fn test_tables_present_requires_write_schema_id_first() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks"]).unwrap();

    let mut dataset = test_dataset(&mut builder);

    let Err(error) = staged_chunk(&mut dataset).tables_present(&["blocks"]) else {
        panic!("tables_present encodes against the roster, so it needs the schema id");
    };

    assert!(
        error.to_string().contains("write_schema_id must be set"),
        "unexpected error: {error}"
    );
}

/// The roster defines the bit order, so unlike `tables_present` it is validated in release too.
#[cfg(feature = "builder")]
#[test]
fn test_unsorted_roster_is_rejected() {
    let mut builder = test_builder();

    let error = builder
        .register_write_schema(7, &["blocks", "transactions", "logs"])
        .expect_err("an unsorted roster must be rejected");
    assert!(error.to_string().contains("must be sorted"), "unexpected error: {error}");

    let error = builder
        .register_write_schema(7, &["blocks", "blocks"])
        .expect_err("a duplicate table would claim two bits for one table");
    assert!(error.to_string().contains("free of duplicates"), "unexpected error: {error}");
}

#[cfg(all(feature = "builder", debug_assertions))]
#[test]
#[should_panic(expected = "tables_present must be sorted")]
fn test_unsorted_tables_present_trips_debug_assertion() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks", "logs", "transactions"]).unwrap();

    let mut dataset = test_dataset(&mut builder);
    let _ = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .tables_present(&["transactions", "blocks"]);
}

/// Without the debug assertion the merge still rejects it: the roster cursor has already passed
/// the out-of-order name.
#[cfg(all(feature = "builder", not(debug_assertions)))]
#[test]
fn test_unsorted_tables_present_still_fails_without_debug_assertions() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks", "logs", "transactions"]).unwrap();

    let mut dataset = test_dataset(&mut builder);

    let Err(error) = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .tables_present(&["transactions", "blocks"])
    else {
        panic!("an unsorted tables_present must not yield a bitmap");
    };
    assert!(error.to_string().contains("'blocks' is absent"), "unexpected error: {error}");
}

/// A chunk holding every table never encodes a bitmap, so `finish` is the only place its schema
/// reference gets checked.
#[cfg(feature = "builder")]
#[test]
fn test_finish_rejects_unregistered_write_schema() {
    let mut builder = test_builder();

    let mut dataset = test_dataset(&mut builder);
    let error = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .finish()
        .expect_err("a chunk may not reference a write schema with no roster in the blob");

    assert!(
        error.to_string().contains("write schema 7 is not registered"),
        "unexpected error: {error}"
    );
}

/// A bitmap only means anything against the roster it was encoded from, so a later
/// `write_schema_id` must not silently repoint it at another schema's tables.
#[cfg(feature = "builder")]
#[test]
fn test_finish_rejects_write_schema_changed_after_tables_present() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks", "logs"]).unwrap();
    builder.register_write_schema(8, &["blocks", "traces"]).unwrap();

    let mut dataset = test_dataset(&mut builder);
    let error = staged_chunk(&mut dataset)
        .write_schema_id(7)
        .tables_present(&["logs"])
        .unwrap()
        .write_schema_id(8)
        .finish()
        .expect_err("the bitmap selects schema 7's 'logs', but schema 8 has 'traces' in that bit");

    assert!(
        error.to_string().contains("bitmap over write schema 7's roster"),
        "unexpected error: {error}"
    );
}

/// Taking the chunk by type, not just calling a method on it — a portal has to pass one around.
#[cfg(feature = "reader")]
fn version_of(chunk: sqd_assignments::PortalChunk<'_>) -> u32 {
    chunk.version()
}

#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_assignment_round_trip() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new().check_continuity(false);
    let mut dataset = builder.new_dataset("s3://solana-mainnet-2", 7);

    dataset
        .new_chunk()
        .id("0221000000/0221000000-0221000649-BQJdx")
        .block_range(221000000..=221000649)
        .last_block_timestamp(1696192039)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset
        .new_chunk()
        .id("0221000000/0221000650-0221001549-AuRE1")
        .block_range(221000650..=221001549)
        .last_block_timestamp(1696193050)
        .version(2)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset
        .finish(Some("0x9f2e1d4c7b8a35460f1e2d3c4b5a69788796a5b4c3d2e1f00123456789abcdef"))
        .unwrap();

    let keypair = common::get_test_keypair();
    let peer_id = keypair.public().to_peer_id();
    builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);

    let bytes = builder.finish();
    let assignment = sqd_assignments::PortalAssignment::from_owned(bytes).unwrap();

    let dataset = assignment.get_dataset("s3://solana-mainnet-2").unwrap();
    assert_eq!(dataset.last_block(), 221001549);
    assert_eq!(dataset.read_schema_id(), 7);
    assert_eq!(
        dataset.last_block_hash(),
        Some("0x9f2e1d4c7b8a35460f1e2d3c4b5a69788796a5b4c3d2e1f00123456789abcdef"),
        "the head hash is the full block hash, kept whole at the dataset level"
    );
    assert_eq!(
        dataset.chunk(dataset.chunk_count() as u32 - 1).unwrap().hash(),
        Some("AuRE1"),
        "the last chunk's short hash is a different value, and only builds its id"
    );

    assert_eq!(assignment.get_worker_id(0).unwrap(), peer_id);
    let worker = assignment.get_worker_by_index(0);
    assert_eq!(worker.peer_id().unwrap(), peer_id);
    assert_eq!(worker.status(), sqd_assignments::WorkerStatus::Ok);

    let chunk1 = assignment.find_chunk("s3://solana-mainnet-2", 221000000).unwrap();
    assert_eq!(
        chunk1.id().unwrap(),
        "0221000000/0221000000-0221000649-BQJdx",
        "the id is rebuilt from the columns it was split into"
    );
    assert_eq!(chunk1.first_block(), 221000000);
    assert_eq!(
        chunk1.last_block(),
        221000649,
        "the end comes from block_deltas, not the neighbour"
    );
    assert_eq!(chunk1.last_block_timestamp(), 1696192039);
    assert_eq!(chunk1.worker_indexes().collect::<Vec<_>>(), vec![0]);
    assert_eq!(version_of(chunk1), 0, "an unset version means the ingested copy");

    let chunk2 = assignment.find_chunk("s3://solana-mainnet-2", 221000650).unwrap();
    assert_eq!(chunk2.id().unwrap(), "0221000000/0221000650-0221001549-AuRE1");
    assert_eq!(chunk2.last_block_timestamp(), 1696193050);
    assert_eq!(version_of(chunk2), 2, "the portal sees the version but not its storage prefix");

    assert_eq!(
        assignment.find_chunk("s3://dummy", 0).unwrap_err(),
        sqd_assignments::ChunkNotFound::UnknownDataset
    );
    assert_eq!(
        assignment.find_chunk("s3://solana-mainnet-2", 220999999).unwrap_err(),
        sqd_assignments::ChunkNotFound::BeforeFirst
    );
    assert_eq!(
        assignment.find_chunk("s3://solana-mainnet-2", 221001550).unwrap_err(),
        sqd_assignments::ChunkNotFound::AfterLast
    );

    let by_ts = assignment.find_chunk_by_timestamp("s3://solana-mainnet-2", 1696192040).unwrap();
    assert_eq!(by_ts.index(), 1, "the first chunk at or after the timestamp");
    assert_eq!(
        assignment.find_chunk_by_timestamp("s3://solana-mainnet-2", 1).unwrap().index(),
        0,
        "a timestamp before the first chunk lands on it"
    );
    assert_eq!(
        assignment
            .find_chunk_by_timestamp("s3://solana-mainnet-2", u64::MAX)
            .unwrap_err(),
        sqd_assignments::ChunkNotFound::AfterLast
    );
}

/// A block between two chunks belongs to neither. Inferring an end from the next chunk's start
/// would hand back the earlier one, claiming blocks the dataset doesn't hold.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_find_chunk_reports_a_gap() {
    use sqd_assignments::PortalAssignmentBuilder;

    // The gap is why continuity has to be off — the check and the format disagree on whether
    // gaps are legal, and archive.py's `assert self.next_block <= first_block` says they are.
    let mut builder = PortalAssignmentBuilder::new().check_continuity(false);
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    dataset
        .new_chunk()
        .id("0000000000/0000000000-0000000099-274f02d8")
        .block_range(0..=99)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    // With the check off the chunk is still staged; the error is only reported, as
    // `check_continuity` documents.
    let gap = dataset
        .new_chunk()
        .id("0000000000/0000000200-0000000299-9QgFD")
        .block_range(200..=299)
        .worker_indexes(&[0])
        .finish();
    assert!(gap.is_err(), "the gap is still reported for logging");
    dataset.finish(None).unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::PortalAssignment::from_owned(builder.finish()).unwrap();

    assert_eq!(assignment.find_chunk("s3://ethereum-mainnet", 99).unwrap().index(), 0);
    assert_eq!(assignment.find_chunk("s3://ethereum-mainnet", 200).unwrap().index(), 1);
    assert_eq!(
        assignment.find_chunk("s3://ethereum-mainnet", 150).unwrap_err(),
        sqd_assignments::ChunkNotFound::InGap,
        "a block in the hole is not the previous chunk's"
    );
}

/// Tops collapse to runs, and hashes survive the fixed-width column at both ends of their range.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_tops_are_runs_and_hashes_keep_their_length() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    // Two chunks under one top, then a third that opens a new one.
    for (id, range) in [
        ("0000000000/0000000000-0000000099-abcde", 0..=99u64),
        ("0000000000/0000000100-0000000199-274f02d8", 100..=199),
        ("0000000200/0000000200-0000000299-ab_de123", 200..=299),
    ] {
        dataset
            .new_chunk()
            .id(id)
            .block_range(range)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
    }
    dataset.finish(None).unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::PortalAssignment::from_owned(builder.finish()).unwrap();
    let dataset = assignment.get_dataset("s3://ethereum-mainnet").unwrap();

    assert_eq!(dataset.tops().len(), 2, "one run per top directory, not one entry per chunk");
    let ids: Vec<_> = dataset.chunks().map(|chunk| chunk.id().unwrap()).collect();
    assert_eq!(
        ids,
        vec![
            "0000000000/0000000000-0000000099-abcde",
            "0000000000/0000000100-0000000199-274f02d8",
            "0000000200/0000000200-0000000299-ab_de123",
        ],
        "every chunk resolves its top through the run it falls in"
    );
    assert_eq!(dataset.chunk(0).unwrap().hash(), Some("abcde"), "5 characters, NUL-padded");
    assert_eq!(dataset.chunk(1).unwrap().hash(), Some("274f02d8"), "8 characters, unpadded");
    assert!(dataset.versions().is_none(), "a dataset no batch job touched carries no column");
    assert_eq!(
        dataset.timestamps().iter().collect::<Vec<_>>(),
        vec![0, 0, 0],
        "the column is required, so timestamps it was never given read as 0"
    );
    assert_eq!(dataset.chunk(2).unwrap().last_block_timestamp(), 0);
    assert!(dataset.chunk(3).is_none(), "past the end of the columns");
}

#[cfg(feature = "builder")]
#[test]
fn test_worker_accepts_descending_numeric_tops() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks"]).unwrap();
    let mut dataset = test_dataset(&mut builder);
    for (id, range) in [
        ("0000001000/0000000000-0000000099-abcde", 0..=99u64),
        ("0000000500/0000000100-0000000199-bcdef", 100..=199),
    ] {
        dataset
            .new_chunk()
            .id(id)
            .block_range(range)
            .size(1000)
            .write_schema_id(7)
            .finish()
            .unwrap();
    }

    dataset.finish().unwrap();
}

#[cfg(feature = "builder")]
#[test]
fn test_portal_accepts_descending_numeric_tops() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    for (id, range) in [
        ("0000001000/0000000000-0000000099-abcde", 0..=99u64),
        ("0000000500/0000000100-0000000199-bcdef", 100..=199),
    ] {
        dataset.new_chunk().id(id).block_range(range).finish().unwrap();
    }

    dataset.finish(None).unwrap();
}

/// The routing column is flattened and staged through a buffer the builder reuses, so a chunk
/// must neither inherit the previous chunk's workers nor spill into the next one's.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_worker_slices_stay_separate() {
    let mut builder = test_builder();
    builder.register_write_schema(7, &["blocks"]).unwrap();

    let mut dataset = test_dataset(&mut builder);
    // The middle chunk names no workers at all, which is the case that would inherit the previous
    // chunk's list rather than an empty one.
    for (id, range, workers) in [
        (
            "0221000000/0221000000-0221000649-BQJdx",
            221000000..=221000649u64,
            Some(&[0u16, 2][..]),
        ),
        ("0221000000/0221000650-0221001549-AuRE1", 221000650..=221001549, None),
        ("0221000000/0221001550-0221001999-C7pQz", 221001550..=221001999, Some(&[1][..])),
    ] {
        let staged = dataset.new_chunk().id(id).block_range(range).size(1000000).write_schema_id(7);
        let staged = match workers {
            Some(workers) => staged.worker_indexes(workers),
            None => staged,
        };
        staged.finish().unwrap();
    }
    dataset.finish().unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::WorkerAssignment::from_owned(builder.finish()).unwrap();
    let dataset = assignment.get_dataset("s3://solana-mainnet-2").unwrap();
    let slices: Vec<Vec<u16>> =
        dataset.chunks().map(|chunk| chunk.worker_indexes().collect()).collect();
    assert_eq!(
        slices,
        vec![vec![0, 2], vec![], vec![1]],
        "including the chunk that named no workers"
    );
}

/// The routing column is flattened, so the offsets are what keep chunks' worker lists apart.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_worker_slices_stay_separate() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    // The third chunk names no workers at all, which is the case that would inherit the previous
    // chunk's list rather than an empty one.
    for (id, range, workers) in [
        ("0000000000/0000000000-0000000099-abcde", 0..=99u64, Some(&[0u16, 2][..])),
        ("0000000000/0000000100-0000000199-bcdef", 100..=199, Some(&[][..])),
        ("0000000000/0000000200-0000000299-cdefa", 200..=299, Some(&[1][..])),
        ("0000000000/0000000300-0000000399-defab", 300..=399, None),
    ] {
        let staged = dataset.new_chunk().id(id).block_range(range);
        let staged = match workers {
            Some(workers) => staged.worker_indexes(workers),
            None => staged,
        };
        staged.finish().unwrap();
    }
    dataset.finish(None).unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::PortalAssignment::from_owned(builder.finish()).unwrap();
    let dataset = assignment.get_dataset("s3://ethereum-mainnet").unwrap();

    let slices: Vec<Vec<u16>> =
        dataset.chunks().map(|chunk| chunk.worker_indexes().collect()).collect();
    assert_eq!(
        slices,
        vec![vec![0, 2], vec![], vec![1], vec![]],
        "including the empty one in the middle and the chunk that named no workers"
    );
    assert_eq!(
        dataset.worker_offsets().len(),
        dataset.chunk_count() + 1,
        "one offset per chunk plus the closing end"
    );
}

#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_versions_column_appears_only_once_something_is_backfilled() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    dataset
        .new_chunk()
        .id("0000000000/0000000000-0000000099-abcde")
        .block_range(0..=99)
        .version(4)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset
        .new_chunk()
        .id("0000000000/0000000100-0000000199-bcdef")
        .block_range(100..=199)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset.finish(None).unwrap();
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::PortalAssignment::from_owned(builder.finish()).unwrap();
    let dataset = assignment.get_dataset("s3://ethereum-mainnet").unwrap();

    assert!(dataset.versions().is_some(), "one backfilled chunk brings the whole column");
    assert_eq!(
        dataset.chunks().map(|chunk| chunk.version()).collect::<Vec<_>>(),
        vec![4, 0],
        "dense, so an untouched chunk still occupies its slot"
    );
}

/// Ingest times most chunks and not others, so the column has to hold the mix. It says nothing
/// about a chunk it was never given beyond 0, which is where the legacy reader also puts one.
#[cfg(all(feature = "builder", feature = "reader"))]
#[test]
fn test_portal_untimed_chunks_read_as_zero() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    dataset
        .new_chunk()
        .id("0000000000/0000000000-0000000099-abcde")
        .block_range(0..=99)
        .last_block_timestamp(1696192039)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset
        .new_chunk()
        .id("0000000000/0000000100-0000000199-bcdef")
        .block_range(100..=199)
        .worker_indexes(&[0])
        .finish()
        .unwrap();
    dataset.finish(None).expect("a partly timed dataset still builds");
    builder.add_worker(
        common::get_test_keypair().public().to_peer_id(),
        sqd_assignments::WorkerStatus::Ok,
    );

    let assignment = sqd_assignments::PortalAssignment::from_owned(builder.finish()).unwrap();
    let dataset = assignment.get_dataset("s3://ethereum-mainnet").unwrap();
    assert_eq!(
        dataset.timestamps().iter().collect::<Vec<_>>(),
        vec![1696192039, 0],
        "dense, so an untimed chunk still occupies its slot"
    );
    assert_eq!(dataset.chunk(0).unwrap().last_block_timestamp(), 1696192039);
    assert_eq!(dataset.chunk(1).unwrap().last_block_timestamp(), 0);
}

#[cfg(feature = "builder")]
#[test]
fn test_portal_chunk_id_must_agree_with_the_block_range() {
    use sqd_assignments::PortalAssignmentBuilder;

    let mut builder = PortalAssignmentBuilder::new();
    let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
    let error = dataset
        .new_chunk()
        .id("0000000000/0000000000-0000000099-abcde")
        .block_range(0..=100)
        .worker_indexes(&[0])
        .finish()
        .expect_err("the id is rebuilt from the range, so the two must say the same thing");
    assert!(error.to_string().contains("names blocks"), "unexpected error: {error}");
}

#[cfg(feature = "builder")]
#[test]
fn test_portal_rejects_a_malformed_chunk_id() {
    use sqd_assignments::PortalAssignmentBuilder;

    for (id, expected) in [
        ("0000000000-0000000099-abcde", "no top directory"),
        ("0000000000/0000000000-abcde", "is not <top>"),
        ("0000000000/0000000000-0000000099-", "5 to 8 word characters"),
        ("0000000000/0000000000-0000000099-toolonghash", "5 to 8 word characters"),
        ("0000000000/0000000000-0000000099-abcd", "5 to 8 word characters"),
        ("0000000000/0000000000-0000000099-has-dash", "5 to 8 word characters"),
        ("000000000x/0000000000-0000000099-abcde", "non-numeric top"),
    ] {
        let mut builder = PortalAssignmentBuilder::new();
        let mut dataset = builder.new_dataset("s3://ethereum-mainnet", 1);
        let error = dataset
            .new_chunk()
            .id(id)
            .block_range(0..=99)
            .worker_indexes(&[0])
            .finish()
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "id '{id}': expected {expected:?}, got {error}"
        );
    }
}
