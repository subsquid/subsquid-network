//! Correction chunks can use top directories that decrease or recur in block order.

mod common;

#[cfg(all(feature = "builder", feature = "reader"))]
mod non_monotone_tops {
    use rand::{rngs::StdRng, SeedableRng};
    use sqd_assignments::{
        PortalAssignment, PortalAssignmentBuilder, WorkerAssignment, WorkerAssignmentBuilder,
        WorkerStatus,
    };

    use super::common;

    /// (id, first block, last block, version).
    type Chunk = (&'static str, u64, u64, u32);

    /// The new partition plus the head: contiguous blocks, tops 0, 16, 0.
    const APPLIED: [Chunk; 3] = [
        ("0000000000/0000000000-0000000015-rrrr1", 0, 15, 1),
        ("0000000016/0000000016-0000000020-rrrr2", 16, 20, 1),
        ("0000000000/0000000021-0000000030-ccccc", 21, 30, 0),
    ];

    /// Old and new partitions interleaved by first block, as a pending batch is published to
    /// workers: tops 0, 0, 0, 16, 0. Overlaps are legal there (continuity is off).
    const PENDING: [Chunk; 5] = [
        ("0000000000/0000000000-0000000010-aaaaa", 0, 10, 0),
        ("0000000000/0000000000-0000000015-rrrr1", 0, 15, 1),
        ("0000000000/0000000011-0000000020-bbbbb", 11, 20, 0),
        ("0000000016/0000000016-0000000020-rrrr2", 16, 20, 1),
        ("0000000000/0000000021-0000000030-ccccc", 21, 30, 0),
    ];

    /// With continuity off the builder appends a non-contiguous chunk and only reports it.
    fn appended(result: anyhow::Result<()>) -> anyhow::Result<()> {
        match result {
            Err(e) if e.to_string().starts_with("Chunks in the dataset must be contiguous") => {
                Ok(())
            }
            other => other,
        }
    }

    fn worker_bytes(chunks: &[Chunk]) -> anyhow::Result<Vec<u8>> {
        let mut builder =
            WorkerAssignmentBuilder::new_with_rng("test-secret", StdRng::seed_from_u64(0))
                .check_continuity(false);
        builder.register_write_schema(1, &["blocks"])?;
        let mut dataset = builder.new_dataset("s3://ds", "https://ds.example");
        dataset.register_generation(1, "https://ds.example/bf")?;
        for (id, first, last, version) in chunks {
            appended(
                dataset
                    .new_chunk()
                    .id(id)
                    .block_range(*first..=*last)
                    .size(1)
                    .write_schema_id(1)
                    .version(*version)
                    .worker_indexes(&[0])
                    .finish(),
            )?;
        }
        dataset.finish()?;
        builder.add_worker(common::get_test_keypair().public().to_peer_id(), WorkerStatus::Ok);
        Ok(builder.finish())
    }

    fn portal_bytes(chunks: &[Chunk]) -> anyhow::Result<Vec<u8>> {
        let mut builder = PortalAssignmentBuilder::new();
        let mut dataset = builder.new_dataset("s3://ds", 1);
        for (id, first, last, version) in chunks {
            dataset
                .new_chunk()
                .id(id)
                .block_range(*first..=*last)
                .version(*version)
                .worker_indexes(&[0])
                .finish()?;
        }
        dataset.finish(None)?;
        builder.add_worker(common::get_test_keypair().public().to_peer_id(), WorkerStatus::Ok);
        Ok(builder.finish())
    }

    fn ids(chunks: &[Chunk]) -> Vec<String> {
        chunks.iter().map(|(id, ..)| (*id).to_owned()).collect()
    }

    #[test]
    fn worker_assignment_accepts_tops_that_do_not_ascend() {
        for (label, chunks) in [("applied", &APPLIED[..]), ("pending", &PENDING[..])] {
            let bytes = worker_bytes(chunks).unwrap_or_else(|e| panic!("{label}: {e:#}"));
            let assignment = WorkerAssignment::from_owned(bytes).unwrap();
            let dataset = assignment.get_dataset("s3://ds").unwrap();
            let read: Vec<String> = dataset.chunks().map(|c| c.id().unwrap()).collect();
            assert_eq!(read, ids(chunks), "{label}: every chunk resolves its own top");
        }
    }

    #[test]
    fn portal_assignment_accepts_tops_that_do_not_ascend() {
        let bytes = portal_bytes(&APPLIED).unwrap_or_else(|e| panic!("applied: {e:#}"));
        let assignment = PortalAssignment::from_owned(bytes).unwrap();
        let dataset = assignment.get_dataset("s3://ds").unwrap();
        let read: Vec<String> = dataset.chunks().map(|c| c.id().unwrap()).collect();
        assert_eq!(read, ids(&APPLIED), "every chunk resolves its own top");
    }
}
