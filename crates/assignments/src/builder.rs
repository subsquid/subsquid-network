use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap},
    ops::RangeInclusive,
};

use anyhow::Context as _;
use crypto_box::{
    aead::{rand_core::CryptoRngCore, OsRng},
    SecretKey,
};
use flatbuffers::{self as fb, WIPOffset};
use libp2p_identity::PeerId;

use crate::{
    common,
    signatures::{encrypt_with_rng, timed_hmac},
};

use super::assignment_fb::{self, Assignment, WorkerId};

fn status_to_fb(status: common::WorkerStatus) -> assignment_fb::WorkerStatus {
    match status {
        crate::WorkerStatus::Ok => assignment_fb::WorkerStatus::Ok,
        crate::WorkerStatus::Unreliable => assignment_fb::WorkerStatus::Unreliable,
        crate::WorkerStatus::DeprecatedVersion => assignment_fb::WorkerStatus::DeprecatedVersion,
        crate::WorkerStatus::UnsupportedVersion => assignment_fb::WorkerStatus::UnsupportedVersion,
    }
}

pub struct AssignmentBuilder<Rng: CryptoRngCore> {
    builder: fb::FlatBufferBuilder<'static>,
    rng: Rng,
    files_list_offsets: FileListOffsets,
    all_chunks: Vec<fb::WIPOffset<assignment_fb::Chunk<'static>>>,
    last_block: Option<u64>,
    current_chunks: Vec<fb::WIPOffset<assignment_fb::Chunk<'static>>>,
    current_dataset_id_offset: Option<fb::WIPOffset<&'static str>>,
    all_datasets: Vec<fb::WIPOffset<assignment_fb::Dataset<'static>>>,
    worker_assignments: Vec<(WorkerId, fb::WIPOffset<assignment_fb::WorkerEntry<'static>>)>,
    last_peer_id: Option<PeerId>,
    cloudflare_storage_secret: String,
    common_identity: fb::WIPOffset<fb::Vector<'static, u8>>,
    common_secret_key: SecretKey,
    check_continuity: bool,
}

impl AssignmentBuilder<OsRng> {
    pub fn new(cloudflare_storage_secret: impl Into<String>) -> Self {
        Self::new_with_rng(cloudflare_storage_secret, OsRng)
    }
}

impl<Rng: CryptoRngCore> AssignmentBuilder<Rng> {
    pub fn new_with_rng(cloudflare_storage_secret: impl Into<String>, mut rng: Rng) -> Self {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let common_secret_key = SecretKey::generate(&mut rng);
        let common_public_key_bytes = *common_secret_key.public_key().as_bytes();
        let common_identity = builder.create_vector(&common_public_key_bytes);
        Self {
            builder,
            rng,
            files_list_offsets: HashMap::new(),
            all_chunks: Vec::new(),
            last_block: None,
            current_chunks: Vec::new(),
            current_dataset_id_offset: None,
            all_datasets: Vec::new(),
            worker_assignments: Vec::new(),
            last_peer_id: None,
            cloudflare_storage_secret: cloudflare_storage_secret.into(),
            common_identity,
            common_secret_key,
            check_continuity: true,
        }
    }

    /// If this check is enabled, the chunk breaking the continuity condition won't be added and an error will be returned.
    /// If it's disabled, the chunk will be added but the error will still be returned for logging purposes.
    /// Enabled by default.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn check_continuity(mut self, check: bool) -> Self {
        self.check_continuity = check;
        self
    }

    pub fn new_chunk(&mut self) -> ChunkBuilder<'_, Rng> {
        ChunkBuilder::new(self)
    }

    pub fn finish_dataset(&mut self) {
        let chunks = self.builder.create_vector(&self.current_chunks);
        let offset = assignment_fb::Dataset::create(
            &mut self.builder,
            &assignment_fb::DatasetArgs {
                id: self.current_dataset_id_offset.take(),
                chunks: Some(chunks),
                last_block: self
                    .last_block
                    .take()
                    .expect("At least one chunk should be present in the dataset"),
            },
        );
        self.all_datasets.push(offset);
        self.current_chunks.clear();
    }

    /// `chunk_indexes` is retained for API compatibility and ignored.
    pub fn add_worker(&mut self, id: PeerId, status: common::WorkerStatus, _chunk_indexes: &[u32]) {
        let timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs().try_into().unwrap();
        self.add_worker_with_timestamp(id, status, _chunk_indexes, timestamp);
    }

    pub fn add_worker_with_timestamp(
        &mut self,
        id: PeerId,
        status: common::WorkerStatus,
        _chunk_indexes: &[u32],
        timestamp: usize,
    ) {
        if let Some(last) = self.last_peer_id {
            assert!(last < id, "Workers must be added in ascending order of their PeerIDs");
        }
        self.last_peer_id = Some(id);

        let worker_id = WorkerId::from(id);
        let status = status_to_fb(status);

        let encrypted_headers = self
            .generate_encrypted_headers(&id, timestamp)
            .inspect_err(|e| {
                tracing::warn!("Failed to encrypt headers for worker {}: {}", id, e);
            })
            .ok();
        let offset = assignment_fb::WorkerEntry::create(
            &mut self.builder,
            &assignment_fb::WorkerEntryArgs {
                worker_id: Some(&worker_id),
                status,
                encrypted_headers,
            },
        );
        self.worker_assignments.push((worker_id, offset));
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let datasets = self.builder.create_vector(&self.all_datasets);

        let workers = self
            .builder
            .create_vector_from_iter(self.worker_assignments.iter().map(|(_, offset)| *offset));

        let assignment = Assignment::create(
            &mut self.builder,
            &assignment_fb::AssignmentArgs {
                datasets: Some(datasets),
                workers: Some(workers),
            },
        );

        self.builder.finish(assignment, None);
        self.builder.finished_data().to_vec()
    }

    fn add_chunk(
        &mut self,
        offset: fb::WIPOffset<assignment_fb::Chunk<'static>>,
        dataset: WIPOffset<&'static str>,
        block_range: RangeInclusive<u64>,
    ) -> anyhow::Result<()> {
        let result = match self.last_block {
            Some(last) if last + 1 != *block_range.start() => Err(anyhow::anyhow!(
                "Chunks in the dataset must be contiguous, got {} -> {}",
                last,
                block_range.start()
            )),
            _ => Ok(()),
        };
        if result.is_ok() || !self.check_continuity {
            self.all_chunks.push(offset);
            self.last_block = Some(*block_range.end());
            self.current_chunks.push(offset);
            self.current_dataset_id_offset = Some(dataset);
        }
        result
    }

    fn cache_files_list(
        &mut self,
        files: &[String],
    ) -> fb::WIPOffset<fb::Vector<'static, fb::ForwardsUOffset<assignment_fb::FileUrl<'static>>>>
    {
        match self.files_list_offsets.get(files) {
            Some(&offset) => offset,
            None => {
                let file_offsets: Vec<_> = files
                    .iter()
                    .map(|f| {
                        let name = self.builder.create_string(f);
                        assignment_fb::FileUrl::create(
                            &mut self.builder,
                            &assignment_fb::FileUrlArgs {
                                filename: Some(name),
                                url: Some(name),
                            },
                        )
                    })
                    .collect();
                let offset = self.builder.create_vector(&file_offsets);
                self.files_list_offsets.insert(files.to_vec(), offset);
                offset
            }
        }
    }

    fn generate_encrypted_headers(
        &mut self,
        peer_id: &PeerId,
        timestamp: usize,
    ) -> anyhow::Result<fb::WIPOffset<assignment_fb::EncryptedHeaders<'static>>> {
        let id = peer_id.to_string();
        let worker_signature = timed_hmac(&id, &self.cloudflare_storage_secret, timestamp);
        let plaintext =
            format!(r#"{{"worker-id":"{}","worker-signature":"{}"}}"#, id, worker_signature);

        let (ciphertext, nonce) = encrypt_with_rng(
            peer_id,
            &self.common_secret_key,
            plaintext.as_bytes(),
            &mut self.rng,
        )?;

        let ciphertext_offset = self.builder.create_vector(&ciphertext);
        let nonce_offset = self.builder.create_vector(&nonce);
        Ok(assignment_fb::EncryptedHeaders::create(
            &mut self.builder,
            &assignment_fb::EncryptedHeadersArgs {
                identity: Some(self.common_identity),
                nonce: Some(nonce_offset),
                ciphertext: Some(ciphertext_offset),
            },
        ))
    }
}

#[test]
fn test_json_formatting() {
    let s = format!(r#"{{"worker-id":"{}","worker-signature":"{}"}}"#, "test-id", "test-signature");
    assert_eq!(s, "{\"worker-id\":\"test-id\",\"worker-signature\":\"test-signature\"}");
}

type FileListOffsets = HashMap<
    Vec<String>,
    fb::WIPOffset<fb::Vector<'static, fb::ForwardsUOffset<assignment_fb::FileUrl<'static>>>>,
>;

pub struct ChunkBuilder<'b, Rng: CryptoRngCore> {
    p: &'b mut AssignmentBuilder<Rng>,

    block_range: Option<RangeInclusive<u64>>,
    id: Option<fb::WIPOffset<&'static str>>,
    dataset_id: Option<fb::WIPOffset<&'static str>>,
    size: Option<u32>,
    last_block_hash: Option<fb::WIPOffset<&'static str>>,
    last_block_timestamp: Option<u64>,
    dataset_base_url: Option<fb::WIPOffset<&'static str>>,
    files: Option<
        fb::WIPOffset<fb::Vector<'static, fb::ForwardsUOffset<assignment_fb::FileUrl<'static>>>>,
    >,
    worker_indexes: Option<fb::WIPOffset<fb::Vector<'static, u16>>>,
}

impl<'b, Rng: CryptoRngCore> ChunkBuilder<'b, Rng> {
    pub fn new(parent: &'b mut AssignmentBuilder<Rng>) -> Self {
        Self {
            p: parent,
            block_range: None,
            id: None,
            dataset_id: None,
            size: None,
            last_block_hash: None,
            last_block_timestamp: None,
            dataset_base_url: None,
            files: None,
            worker_indexes: None,
        }
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(self.p.builder.create_string(id));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn dataset_id(mut self, dataset_id: &str) -> Self {
        self.dataset_id = Some(self.p.builder.create_shared_string(dataset_id));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn block_range(mut self, range: RangeInclusive<u64>) -> Self {
        self.block_range = Some(range);
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn size(mut self, size: u32) -> Self {
        self.size = Some(size);
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn last_block_hash(mut self, hash: &str) -> Self {
        self.last_block_hash = Some(self.p.builder.create_string(hash));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn last_block_timestamp(mut self, timestamp: u64) -> Self {
        self.last_block_timestamp = Some(timestamp);
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn dataset_base_url(mut self, url: &str) -> Self {
        self.dataset_base_url = Some(self.p.builder.create_shared_string(url));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn worker_indexes(mut self, indexes: &[u16]) -> Self {
        self.worker_indexes = Some(self.p.builder.create_vector(indexes));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn files(mut self, files: &[String]) -> Self {
        self.files = Some(self.p.cache_files_list(files));
        self
    }

    pub fn finish(self) -> anyhow::Result<()> {
        let block_range = self.block_range.expect("Block range must be set");
        let offset = assignment_fb::Chunk::create(
            &mut self.p.builder,
            &assignment_fb::ChunkArgs {
                id: self.id,
                first_block: *block_range.start(),
                last_block_hash: self.last_block_hash,
                last_block_timestamp: self.last_block_timestamp,
                dataset_id: self.dataset_id,
                size: self.size.expect("Size must be set"),
                dataset_base_url: self.dataset_base_url,
                base_url: self.id,
                files: self.files,
                worker_indexes: self.worker_indexes,
            },
        );
        self.p
            .add_chunk(offset, self.dataset_id.expect("Dataset ID must be set"), block_range)
    }
}

// ===== Worker-facing assignment =====
//
// Chunks are stored as dataset columns.

/// The dataset being staged, taken by [`WorkerDatasetBuilder::finish`].
#[derive(Default)]
struct WorkerDatasetColumns {
    first_blocks: Vec<u64>,
    block_deltas: Vec<u32>,
    hashes: Vec<assignment_fb::ChunkHash>,
    /// One entry per run, appended only when a chunk's top differs from the previous chunk's.
    tops: Vec<assignment_fb::TopRun>,
    sizes: Vec<u32>,
    write_schema_ids: Vec<u32>,
    versions: Vec<u32>,
    tables_present: Vec<u8>,
    /// Where each chunk's bitmap ends; the emitted column is a leading 0 then these.
    tables_present_ends: Vec<u32>,
    /// Whether any chunk trimmed its tables; if none did, both columns are dropped.
    any_tables_trimmed: bool,
    /// Where each chunk's worker slice ends; the emitted CSR column is a leading 0 then these.
    worker_ends: Vec<u32>,
    worker_indexes: Vec<u16>,
}

pub struct WorkerAssignmentBuilder<Rng: CryptoRngCore> {
    builder: fb::FlatBufferBuilder<'static>,
    rng: Rng,
    last_block: Option<u64>,
    all_datasets: Vec<fb::WIPOffset<assignment_fb::WorkerAssignmentDataset<'static>>>,
    worker_entries: Vec<(WorkerId, fb::WIPOffset<assignment_fb::WorkerEntry<'static>>)>,
    last_peer_id: Option<PeerId>,
    cloudflare_storage_secret: String,
    /// Written when headers are first encrypted.
    common_identity: Option<fb::WIPOffset<fb::Vector<'static, u8>>>,
    common_secret_key: SecretKey,
    check_continuity: bool,
    /// `BTreeMap` so `finish` emits rosters id-sorted, as `TableRoster`'s `(key)` lookup requires.
    write_schemas: BTreeMap<u32, Vec<String>>,
    /// Per-dataset, unlike write schemas, so `finish_dataset` clears them. `BTreeMap` so they
    /// reach the blob version-sorted, as `GenerationEntry`'s `(key)` lookup requires.
    current_generations: BTreeMap<u32, String>,
    columns: WorkerDatasetColumns,
    /// The open chunk's worker indexes and tables bitmap. They live here, cleared by `new_chunk`,
    /// so staging a chunk reuses one allocation instead of making two.
    staged_worker_indexes: Vec<u16>,
    staged_tables_present: Vec<u8>,
}

impl WorkerAssignmentBuilder<OsRng> {
    pub fn new(cloudflare_storage_secret: impl Into<String>) -> Self {
        Self::new_with_rng(cloudflare_storage_secret, OsRng)
    }
}

impl<Rng: CryptoRngCore> WorkerAssignmentBuilder<Rng> {
    pub fn new_with_rng(cloudflare_storage_secret: impl Into<String>, mut rng: Rng) -> Self {
        let builder = flatbuffers::FlatBufferBuilder::new();
        let common_secret_key = SecretKey::generate(&mut rng);
        Self {
            builder,
            rng,
            last_block: None,
            all_datasets: Vec::new(),
            worker_entries: Vec::new(),
            last_peer_id: None,
            cloudflare_storage_secret: cloudflare_storage_secret.into(),
            common_identity: None,
            common_secret_key,
            check_continuity: true,
            write_schemas: BTreeMap::new(),
            current_generations: BTreeMap::new(),
            columns: WorkerDatasetColumns::default(),
            staged_worker_indexes: Vec::new(),
            staged_tables_present: Vec::new(),
        }
    }

    /// See [`AssignmentBuilder::check_continuity`].
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn check_continuity(mut self, check: bool) -> Self {
        self.check_continuity = check;
        self
    }

    /// The schema's full table list, which each chunk's `tables_present` selects from. Must be
    /// registered before any chunk referencing it, sorted and duplicate-free.
    ///
    /// # Errors
    ///
    /// If `tables` is not strictly ascending or the id has a different registered roster.
    pub fn register_write_schema<S: AsRef<str>>(
        &mut self,
        write_schema_id: u32,
        tables: &[S],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            tables.is_sorted_by(|a, b| a.as_ref() < b.as_ref()),
            "write schema {write_schema_id}'s roster must be sorted and free of duplicates"
        );
        match self.write_schemas.entry(write_schema_id) {
            Entry::Occupied(existing) => {
                let registered = existing.get();
                let unchanged = registered.len() == tables.len()
                    && std::iter::zip(registered, tables).all(|(old, new)| old == new.as_ref());
                anyhow::ensure!(
                    unchanged,
                    "write schema {write_schema_id} re-registered with a different table roster"
                );
            }
            Entry::Vacant(slot) => {
                slot.insert(tables.iter().map(|t| t.as_ref().to_owned()).collect());
            }
        }
        Ok(())
    }

    /// Opens the dataset that chunks are staged into. Chunks never name a dataset or a base url
    /// themselves — both belong to the dataset they were opened under — so neither can disagree
    /// with the dataset holding them.
    ///
    /// `base_url` is the dataset's storage root, which every chunk's download url extends.
    #[must_use = "a dataset is emitted by `finish`; a builder that is dropped emits nothing"]
    pub fn new_dataset(&mut self, id: &str, base_url: &str) -> WorkerDatasetBuilder<'_, Rng> {
        let id = self.builder.create_shared_string(id);
        let base_url = self.builder.create_shared_string(base_url);
        WorkerDatasetBuilder {
            p: self,
            id,
            base_url,
            emitted: false,
        }
    }

    /// Forgets everything staged for the open dataset, so the next one starts clean.
    fn discard_dataset(&mut self) {
        self.columns = WorkerDatasetColumns::default();
        self.last_block = None;
        self.current_generations.clear();
    }

    /// Emits the staged chunks as the dataset's columns.
    fn emit_dataset(
        &mut self,
        id: fb::WIPOffset<&'static str>,
        base_url: fb::WIPOffset<&'static str>,
    ) -> anyhow::Result<()> {
        let chunk_count = self.columns.first_blocks.len();
        anyhow::ensure!(chunk_count > 0, "At least one chunk should be present in the dataset");
        check_top_runs(&self.columns.tops)?;

        let columns = std::mem::take(&mut self.columns);
        // After the checks, so a rejected dataset doesn't strand its generation entries in the
        // buffer — nothing would reference them, and a retry would write them again.
        let generations = self.create_generations();
        let first_blocks = self.builder.create_vector(&columns.first_blocks);
        let block_deltas = self.builder.create_vector(&columns.block_deltas);
        let hashes = self.builder.create_vector(&columns.hashes);
        let tops = self.builder.create_vector(&columns.tops);
        let sizes = self.builder.create_vector(&columns.sizes);
        let write_schema_ids = self.builder.create_vector(&columns.write_schema_ids);
        let (tables_present_offsets, tables_present) = if columns.any_tables_trimmed {
            let mut offsets = Vec::with_capacity(columns.tables_present_ends.len() + 1);
            offsets.push(0);
            offsets.extend_from_slice(&columns.tables_present_ends);
            (
                Some(self.builder.create_vector(&offsets)),
                Some(self.builder.create_vector(&columns.tables_present)),
            )
        } else {
            (None, None)
        };
        let versions = columns
            .versions
            .iter()
            .any(|&version| version != 0)
            .then(|| self.builder.create_vector(&columns.versions));
        // A leading 0, so slot i is the start of chunk i's slice and slot i + 1 its end.
        let mut worker_offsets = Vec::with_capacity(columns.worker_ends.len() + 1);
        worker_offsets.push(0);
        worker_offsets.extend_from_slice(&columns.worker_ends);
        let worker_offsets = self.builder.create_vector(&worker_offsets);
        let worker_indexes = self.builder.create_vector(&columns.worker_indexes);

        let offset = assignment_fb::WorkerAssignmentDataset::create(
            &mut self.builder,
            &assignment_fb::WorkerAssignmentDatasetArgs {
                id: Some(id),
                last_block: self
                    .last_block
                    .take()
                    .expect("a staged chunk sets last_block, and the count was checked above"),
                base_url: Some(base_url),
                generations,
                first_blocks: Some(first_blocks),
                block_deltas: Some(block_deltas),
                hashes: Some(hashes),
                tops: Some(tops),
                sizes: Some(sizes),
                write_schema_ids: Some(write_schema_ids),
                tables_present_offsets,
                tables_present,
                versions,
                worker_offsets: Some(worker_offsets),
                worker_indexes: Some(worker_indexes),
            },
        );
        self.all_datasets.push(offset);
        self.current_generations.clear();
        Ok(())
    }

    /// `None` for a dataset whose chunks are all version 0, so the common case stores nothing.
    fn create_generations(
        &mut self,
    ) -> Option<
        fb::WIPOffset<
            fb::Vector<'static, fb::ForwardsUOffset<assignment_fb::GenerationEntry<'static>>>,
        >,
    > {
        if self.current_generations.is_empty() {
            return None;
        }
        // Destructured so the prefixes can be read while `builder` is mutably borrowed.
        let Self {
            builder,
            current_generations,
            ..
        } = self;
        let offsets: Vec<_> = current_generations
            .iter()
            .map(|(version, base_url)| {
                let base_url = builder.create_shared_string(base_url);
                assignment_fb::GenerationEntry::create(
                    builder,
                    &assignment_fb::GenerationEntryArgs {
                        version: *version,
                        base_url: Some(base_url),
                    },
                )
            })
            .collect();
        Some(builder.create_vector(&offsets))
    }

    pub fn add_worker(&mut self, id: PeerId, status: common::WorkerStatus) {
        let timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs().try_into().unwrap();
        self.add_worker_with_timestamp(id, status, timestamp);
    }

    pub fn add_worker_with_timestamp(
        &mut self,
        id: PeerId,
        status: common::WorkerStatus,
        timestamp: usize,
    ) {
        if let Some(last) = self.last_peer_id {
            assert!(last < id, "Workers must be added in ascending order of their PeerIDs");
        }
        self.last_peer_id = Some(id);

        let worker_id = WorkerId::from(id);
        let status = status_to_fb(status);
        let encrypted_headers = self
            .generate_encrypted_headers(&id, timestamp)
            .inspect_err(|e| {
                tracing::warn!("Failed to encrypt headers for worker {}: {}", id, e);
            })
            .ok();
        let offset = assignment_fb::WorkerEntry::create(
            &mut self.builder,
            &assignment_fb::WorkerEntryArgs {
                worker_id: Some(&worker_id),
                status,
                encrypted_headers,
            },
        );
        self.worker_entries.push((worker_id, offset));
    }

    /// Adds a worker with pre-encrypted headers.
    pub fn add_worker_with_sealed_headers(
        &mut self,
        id: PeerId,
        status: common::WorkerStatus,
        identity: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
    ) {
        if let Some(last) = self.last_peer_id {
            assert!(last < id, "Workers must be added in ascending order of their PeerIDs");
        }
        self.last_peer_id = Some(id);

        let identity = self.builder.create_vector(identity);
        let nonce = self.builder.create_vector(nonce);
        let ciphertext = self.builder.create_vector(ciphertext);
        let encrypted_headers = assignment_fb::EncryptedHeaders::create(
            &mut self.builder,
            &assignment_fb::EncryptedHeadersArgs {
                identity: Some(identity),
                nonce: Some(nonce),
                ciphertext: Some(ciphertext),
            },
        );

        let worker_id = WorkerId::from(id);
        let offset = assignment_fb::WorkerEntry::create(
            &mut self.builder,
            &assignment_fb::WorkerEntryArgs {
                worker_id: Some(&worker_id),
                status: status_to_fb(status),
                encrypted_headers: Some(encrypted_headers),
            },
        );
        self.worker_entries.push((worker_id, offset));
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let schemas = self.create_write_schema_rosters();
        let datasets = self.builder.create_vector(&self.all_datasets);
        let workers = self
            .builder
            .create_vector_from_iter(self.worker_entries.iter().map(|(_, offset)| *offset));

        let root = assignment_fb::WorkerAssignment::create(
            &mut self.builder,
            &assignment_fb::WorkerAssignmentArgs {
                datasets: Some(datasets),
                workers: Some(workers),
                schemas: Some(schemas),
            },
        );

        self.builder.finish(root, None);
        self.builder.finished_data().to_vec()
    }

    fn create_write_schema_rosters(
        &mut self,
    ) -> fb::WIPOffset<fb::Vector<'static, fb::ForwardsUOffset<assignment_fb::TableRoster<'static>>>>
    {
        // Destructured so the rosters can be read while `builder` is mutably borrowed.
        let Self {
            builder,
            write_schemas,
            ..
        } = self;
        let offsets: Vec<_> = write_schemas
            .iter()
            .map(|(write_schema_id, tables)| {
                let table_offsets: Vec<_> =
                    tables.iter().map(|t| builder.create_shared_string(t)).collect();
                let tables = builder.create_vector(&table_offsets);
                assignment_fb::TableRoster::create(
                    builder,
                    &assignment_fb::TableRosterArgs {
                        write_schema_id: *write_schema_id,
                        tables: Some(tables),
                    },
                )
            })
            .collect();
        builder.create_vector(&offsets)
    }

    fn generate_encrypted_headers(
        &mut self,
        peer_id: &PeerId,
        timestamp: usize,
    ) -> anyhow::Result<fb::WIPOffset<assignment_fb::EncryptedHeaders<'static>>> {
        let id = peer_id.to_string();
        let worker_signature = timed_hmac(&id, &self.cloudflare_storage_secret, timestamp);
        let plaintext =
            format!(r#"{{"worker-id":"{}","worker-signature":"{}"}}"#, id, worker_signature);

        let (ciphertext, nonce) = encrypt_with_rng(
            peer_id,
            &self.common_secret_key,
            plaintext.as_bytes(),
            &mut self.rng,
        )?;

        let ciphertext_offset = self.builder.create_vector(&ciphertext);
        let nonce_offset = self.builder.create_vector(&nonce);
        let identity = match self.common_identity {
            Some(identity) => identity,
            None => {
                let bytes = *self.common_secret_key.public_key().as_bytes();
                *self.common_identity.insert(self.builder.create_vector(&bytes))
            }
        };
        Ok(assignment_fb::EncryptedHeaders::create(
            &mut self.builder,
            &assignment_fb::EncryptedHeadersArgs {
                identity: Some(identity),
                nonce: Some(nonce_offset),
                ciphertext: Some(ciphertext_offset),
            },
        ))
    }
}

/// One dataset of a [`WorkerAssignmentBuilder`], opened by
/// [`new_dataset`](WorkerAssignmentBuilder::new_dataset).
#[must_use = "a dataset is emitted by `finish`; a builder that is dropped emits nothing"]
pub struct WorkerDatasetBuilder<'b, Rng: CryptoRngCore> {
    p: &'b mut WorkerAssignmentBuilder<Rng>,
    id: fb::WIPOffset<&'static str>,
    base_url: fb::WIPOffset<&'static str>,
    /// Set by `finish`, so `Drop` only cleans up after a dataset that was never emitted.
    emitted: bool,
}

impl<Rng: CryptoRngCore> WorkerDatasetBuilder<'_, Rng> {
    /// The prefix a batch job wrote a generation under, relative to this dataset's base url (e.g.
    /// `_bf/01HQZK3M7X8P2NVWTC4RYFGDS9`). Must precede the chunks carrying that version, and is
    /// scoped to this dataset — a generation covering several datasets is registered on each.
    ///
    /// # Errors
    ///
    /// If `version` is 0 — the ingested layout, defined by having no entry — or if the version was
    /// already registered here with a different prefix.
    pub fn register_generation(&mut self, version: u32, base_url: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            version != 0,
            "version 0 is the ingested layout, which needs no generation entry"
        );
        match self.p.current_generations.entry(version) {
            Entry::Occupied(existing) => anyhow::ensure!(
                existing.get() == base_url,
                "generation {version} re-registered with a different base url"
            ),
            Entry::Vacant(slot) => {
                slot.insert(base_url.to_owned());
            }
        }
        Ok(())
    }

    pub fn new_chunk(&mut self) -> WorkerAssignmentChunkBuilder<'_, Rng> {
        WorkerAssignmentChunkBuilder::new(self.p)
    }

    /// Emits the dataset.
    ///
    /// # Errors
    ///
    /// If no chunk was staged, or a run column doesn't start at chunk 0 and ascend — which staging
    /// already guarantees, but the reader's search depends on it. Either way the dataset is closed
    /// and its chunks and generations are released, so a rejection can't leak into whatever is
    /// opened next.
    pub fn finish(mut self) -> anyhow::Result<()> {
        self.emitted = true;
        let result = self.p.emit_dataset(self.id, self.base_url);
        if result.is_err() {
            self.p.discard_dataset();
        }
        result
    }
}

impl<Rng: CryptoRngCore> Drop for WorkerDatasetBuilder<'_, Rng> {
    fn drop(&mut self) {
        if !self.emitted {
            self.p.discard_dataset();
        }
    }
}

/// Top runs must start at chunk 0 and strictly ascend by `first_chunk_index` so the reader can
/// find the last run at or before a chunk. Top directory values may decrease or recur.
fn check_top_runs(runs: &[assignment_fb::TopRun]) -> anyhow::Result<()> {
    let Some(first) = runs.first() else {
        anyhow::bail!("a dataset with chunks must have at least one top run");
    };
    anyhow::ensure!(first.first_chunk_index() == 0, "the first top run must start at chunk 0");
    for pair in runs.windows(2) {
        anyhow::ensure!(
            pair[0].first_chunk_index() < pair[1].first_chunk_index(),
            "top runs must strictly ascend by first_chunk_index"
        );
    }
    Ok(())
}

pub struct WorkerAssignmentChunkBuilder<'b, Rng: CryptoRngCore> {
    p: &'b mut WorkerAssignmentBuilder<Rng>,

    block_range: Option<RangeInclusive<u64>>,
    /// Parsed by [`Self::id`], since nothing downstream wants the string back. The error is held
    /// rather than returned so it still surfaces at `finish`.
    parsed_id: Option<anyhow::Result<ParsedChunkId>>,
    size: Option<u32>,
    version: u32,
    write_schema_id: Option<u32>,
    /// The write schema the parent's staged bitmap was encoded against — the two can diverge if
    /// `write_schema_id` is set again afterwards, which `finish` rejects.
    tables_present: Option<u32>,
}

impl<'b, Rng: CryptoRngCore> WorkerAssignmentChunkBuilder<'b, Rng> {
    fn new(parent: &'b mut WorkerAssignmentBuilder<Rng>) -> Self {
        parent.staged_worker_indexes.clear();
        parent.staged_tables_present.clear();
        Self {
            p: parent,
            block_range: None,
            parsed_id: None,
            size: None,
            version: 0,
            write_schema_id: None,
            tables_present: None,
        }
    }

    /// e.g. `"0221000000/0221000000-0221000649-9QgFD"`. Split into the `tops`, `first_blocks`,
    /// `block_deltas` and `hashes` columns and rebuilt on read, so it must parse — here rather
    /// than at `finish`, which keeps the string out of the staged chunk. A malformed id is
    /// reported by [`Self::finish`] all the same.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn id(mut self, id: &str) -> Self {
        self.parsed_id = Some(parse_chunk_id(id));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn block_range(mut self, range: RangeInclusive<u64>) -> Self {
        self.block_range = Some(range);
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn size(mut self, size: u32) -> Self {
        self.size = Some(size);
        self
    }

    /// Defaults to 0, the ingested copy; anything else must be registered with
    /// [`WorkerAssignmentBuilder::register_generation`] for this dataset.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// The schema covering every table and column physically in the chunk. Must be registered
    /// with [`WorkerAssignmentBuilder::register_write_schema`].
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn write_schema_id(mut self, write_schema_id: u32) -> Self {
        self.write_schema_id = Some(write_schema_id);
        self
    }

    /// The subset of the write schema's tables this chunk holds, as a bitmap over its roster.
    /// Omit when every table is present. `tables` must be sorted like the roster so the two merge
    /// in one pass; only debug builds assert it, but an unsorted list errors below rather than
    /// yielding a wrong bitmap.
    ///
    /// # Errors
    ///
    /// If [`Self::write_schema_id`] is unset or unregistered, or a table is absent from its
    /// roster — meaning chunk and schema disagree.
    pub fn tables_present<S: AsRef<str>>(mut self, tables: &[S]) -> anyhow::Result<Self> {
        debug_assert!(
            tables.is_sorted_by(|a, b| a.as_ref() < b.as_ref()),
            "tables_present must be sorted and free of duplicates"
        );
        let write_schema_id = self
            .write_schema_id
            .context("write_schema_id must be set before tables_present")?;
        // Destructured so the roster can be read while the staged bitmap is written.
        let WorkerAssignmentBuilder {
            write_schemas,
            staged_tables_present: bits,
            ..
        } = &mut *self.p;
        let roster = write_schemas
            .get(&write_schema_id)
            .with_context(|| format!("write schema {write_schema_id} is not registered"))?;
        bits.clear();
        bits.resize(roster.len().div_ceil(8), 0);
        // Both sides are sorted, so one pass over each suffices: the roster cursor only ever
        // moves forward.
        let mut index = 0;
        for table in tables {
            let name = table.as_ref();
            while index < roster.len() && roster[index].as_str() < name {
                index += 1;
            }
            anyhow::ensure!(
                roster.get(index).is_some_and(|t| t == name),
                "table '{name}' is absent from write schema {write_schema_id}'s roster"
            );
            bits[index / 8] |= 1u8 << (index % 8);
            index += 1;
        }
        self.tables_present = Some(write_schema_id);
        Ok(self)
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn worker_indexes(self, indexes: &[u16]) -> Self {
        self.p.staged_worker_indexes.clear();
        self.p.staged_worker_indexes.extend_from_slice(indexes);
        self
    }

    /// # Panics
    ///
    /// If `block_range` or `size` was never set — those are API misuse, not bad input.
    ///
    /// # Errors
    ///
    /// If the id is unset or malformed, or disagrees with `block_range`; if `write_schema_id` is
    /// unset or names an unregistered schema; if a table isn't in that schema's roster; if a
    /// non-zero `version` has no generation registered for this dataset; if the chunk names a
    /// different base url than the dataset's other chunks; or if it breaks block continuity (see
    /// [`WorkerAssignmentBuilder::check_continuity`]).
    pub fn finish(self) -> anyhow::Result<()> {
        let block_range = self.block_range.expect("Block range must be set");
        let parsed = self.parsed_id.context("Chunk id must be set")??;
        anyhow::ensure!(
            parsed.first_block == *block_range.start() && parsed.last_block == *block_range.end(),
            "chunk id names blocks {}-{}, but the chunk covers {}-{}",
            parsed.first_block,
            parsed.last_block,
            block_range.start(),
            block_range.end()
        );
        let write_schema_id = self.write_schema_id.context("write_schema_id must be set")?;
        // Chunks with every table present never call `tables_present`, so this is the only place
        // their schema reference gets checked.
        anyhow::ensure!(
            self.p.write_schemas.contains_key(&write_schema_id),
            "write schema {write_schema_id} is not registered"
        );
        anyhow::ensure!(
            self.version == 0 || self.p.current_generations.contains_key(&self.version),
            "generation {} is not registered for this dataset",
            self.version
        );
        // A chunk that never called `tables_present` leaves the staged bitmap empty, which is how
        // "every table present" travels.
        if let Some(encoded_against) = self.tables_present {
            anyhow::ensure!(
                encoded_against == write_schema_id,
                "tables_present is a bitmap over write schema {encoded_against}'s roster, \
                 but the chunk declares write schema {write_schema_id}"
            );
        }

        self.p.push_chunk(PushChunk {
            top: parsed.top,
            block_range,
            hash: parsed.hash,
            size: self.size.expect("Size must be set"),
            write_schema_id,
            version: self.version,
        })
    }
}

/// Values staged for one worker chunk.
struct PushChunk {
    top: u64,
    block_range: RangeInclusive<u64>,
    hash: assignment_fb::ChunkHash,
    size: u32,
    write_schema_id: u32,
    version: u32,
}

impl<Rng: CryptoRngCore> WorkerAssignmentBuilder<Rng> {
    /// With [`AssignmentBuilder::check_continuity`] off, a gap still appends and only reports.
    fn push_chunk(&mut self, chunk: PushChunk) -> anyhow::Result<()> {
        let continuity = match self.last_block {
            Some(last) if last + 1 != *chunk.block_range.start() => Err(anyhow::anyhow!(
                "Chunks in the dataset must be contiguous, got {} -> {}",
                last,
                chunk.block_range.start()
            )),
            _ => Ok(()),
        };
        if continuity.is_err() && self.check_continuity {
            return continuity;
        }

        let index: u32 = self
            .columns
            .first_blocks
            .len()
            .try_into()
            .context("a dataset may hold at most u32::MAX chunks")?;
        let delta: u32 = (chunk.block_range.end() - chunk.block_range.start())
            .try_into()
            .context("a chunk may span at most u32::MAX blocks")?;

        if self.columns.tops.last().is_none_or(|run| run.top() != chunk.top) {
            self.columns.tops.push(assignment_fb::TopRun::new(index, chunk.top));
        }
        self.columns.first_blocks.push(*chunk.block_range.start());
        self.columns.block_deltas.push(delta);
        self.columns.hashes.push(chunk.hash);
        self.columns.sizes.push(chunk.size);
        self.columns.write_schema_ids.push(chunk.write_schema_id);
        self.columns.versions.push(chunk.version);
        let Self {
            columns,
            staged_tables_present,
            staged_worker_indexes,
            ..
        } = self;
        columns.any_tables_trimmed |= !staged_tables_present.is_empty();
        columns.push_bitmap(staged_tables_present);
        columns.worker_indexes.extend_from_slice(staged_worker_indexes);
        let end = self
            .columns
            .worker_indexes
            .len()
            .try_into()
            .context("a dataset may hold at most u32::MAX worker references")?;
        self.columns.worker_ends.push(end);

        self.last_block = Some(*chunk.block_range.end());
        continuity
    }
}

impl WorkerDatasetColumns {
    /// An empty bitmap keeps an empty slice, which is how "every table present" travels.
    fn push_bitmap(&mut self, bits: &[u8]) {
        self.tables_present.extend_from_slice(bits);
        self.tables_present_ends.push(self.tables_present.len() as u32);
    }
}

// ===== Portal-facing assignment =====
//
// Chunks are stored as dataset columns.

/// The dataset being staged, taken by [`PortalDatasetBuilder::finish`].
#[derive(Default)]
struct PortalDatasetColumns {
    first_blocks: Vec<u64>,
    block_deltas: Vec<u32>,
    hashes: Vec<assignment_fb::ChunkHash>,
    /// Appended only when a chunk's top differs from the previous one's, which is what makes the
    /// runs start at 0 and strictly ascend by `first_chunk_index`, regardless of top values.
    tops: Vec<assignment_fb::TopRun>,
    /// Absolute epoch milliseconds, as given; 0 for a chunk that was staged without one, which is
    /// the only thing a column can say about a timestamp it never got.
    timestamps: Vec<u64>,
    versions: Vec<u32>,
    /// Where each chunk's worker slice ends; the emitted column is a leading 0 then these.
    worker_ends: Vec<u32>,
    worker_indexes: Vec<u16>,
}

#[derive(Default)]
pub struct PortalAssignmentBuilder {
    builder: fb::FlatBufferBuilder<'static>,
    last_block: Option<u64>,
    all_datasets: Vec<fb::WIPOffset<assignment_fb::PortalAssignmentDataset<'static>>>,
    worker_entries: Vec<(WorkerId, fb::WIPOffset<assignment_fb::PortalEntry<'static>>)>,
    last_peer_id: Option<PeerId>,
    check_continuity: bool,
    columns: PortalDatasetColumns,
    /// The open chunk's worker indexes, cleared by `new_chunk`, so staging a chunk reuses one
    /// allocation instead of making one per chunk.
    staged_worker_indexes: Vec<u16>,
}

impl PortalAssignmentBuilder {
    pub fn new() -> Self {
        Self {
            builder: flatbuffers::FlatBufferBuilder::new(),
            check_continuity: true,
            ..Default::default()
        }
    }

    /// See [`AssignmentBuilder::check_continuity`]. Note that a gap only trips this check; it is
    /// no longer something the reader can misread, since `block_deltas` carries each chunk's end.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn check_continuity(mut self, check: bool) -> Self {
        self.check_continuity = check;
        self
    }

    /// Opens the dataset that chunks are staged into. Chunks never name a dataset themselves — they
    /// belong to the one they were opened under — so nothing can file a chunk under a dataset it
    /// doesn't cover.
    ///
    /// `read_schema_id` is a client-facing view that may hide tables, in a separate id space from
    /// [`WorkerAssignmentChunkBuilder::write_schema_id`]. Datasets are emitted in the order they
    /// are opened.
    #[must_use = "a dataset is emitted by `finish`; a builder that is dropped emits nothing"]
    pub fn new_dataset(&mut self, id: &str, read_schema_id: u32) -> PortalDatasetBuilder<'_> {
        let id = self.builder.create_shared_string(id);
        PortalDatasetBuilder {
            p: self,
            id,
            read_schema_id,
            emitted: false,
        }
    }

    /// Forgets everything staged for the open dataset, so the next one starts clean.
    fn discard_dataset(&mut self) {
        self.columns = PortalDatasetColumns::default();
        self.last_block = None;
    }

    /// Emits the staged chunks as the dataset's columns. `last_block_hash` is the head block's full
    /// hash — the `hashes` column holds truncated ones, so it can't stand in.
    fn emit_dataset(
        &mut self,
        id: fb::WIPOffset<&'static str>,
        read_schema_id: u32,
        last_block_hash: Option<&str>,
    ) -> anyhow::Result<()> {
        let chunk_count = self.columns.first_blocks.len();
        anyhow::ensure!(chunk_count > 0, "At least one chunk should be present in the dataset");
        check_top_runs(&self.columns.tops)?;

        let columns = std::mem::take(&mut self.columns);
        let last_block_hash = last_block_hash.map(|hash| self.builder.create_string(hash));
        let first_blocks = self.builder.create_vector(&columns.first_blocks);
        let block_deltas = self.builder.create_vector(&columns.block_deltas);
        let hashes = self.builder.create_vector(&columns.hashes);
        let tops = self.builder.create_vector(&columns.tops);
        let timestamps = self.builder.create_vector(&columns.timestamps);
        let versions = columns
            .versions
            .iter()
            .any(|&version| version != 0)
            .then(|| self.builder.create_vector(&columns.versions));
        // A leading 0, so slot i is the start of chunk i's slice and slot i + 1 its end.
        let mut worker_offsets = Vec::with_capacity(columns.worker_ends.len() + 1);
        worker_offsets.push(0);
        worker_offsets.extend_from_slice(&columns.worker_ends);
        let worker_offsets = self.builder.create_vector(&worker_offsets);
        let worker_indexes = self.builder.create_vector(&columns.worker_indexes);

        let offset = assignment_fb::PortalAssignmentDataset::create(
            &mut self.builder,
            &assignment_fb::PortalAssignmentDatasetArgs {
                id: Some(id),
                last_block: self
                    .last_block
                    .take()
                    .expect("a staged chunk sets last_block, and the count was checked above"),
                read_schema_id,
                last_block_hash,
                first_blocks: Some(first_blocks),
                block_deltas: Some(block_deltas),
                hashes: Some(hashes),
                tops: Some(tops),
                timestamps: Some(timestamps),
                versions,
                worker_offsets: Some(worker_offsets),
                worker_indexes: Some(worker_indexes),
            },
        );
        self.all_datasets.push(offset);
        Ok(())
    }

    pub fn add_worker(&mut self, id: PeerId, status: common::WorkerStatus) {
        if let Some(last) = self.last_peer_id {
            assert!(last < id, "Workers must be added in ascending order of their PeerIDs");
        }
        self.last_peer_id = Some(id);

        let worker_id = WorkerId::from(id);
        let offset = assignment_fb::PortalEntry::create(
            &mut self.builder,
            &assignment_fb::PortalEntryArgs {
                worker_id: Some(&worker_id),
                status: status_to_fb(status),
            },
        );
        self.worker_entries.push((worker_id, offset));
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let datasets = self.builder.create_vector(&self.all_datasets);
        let workers = self
            .builder
            .create_vector_from_iter(self.worker_entries.iter().map(|(_, offset)| *offset));

        let root = assignment_fb::PortalAssignment::create(
            &mut self.builder,
            &assignment_fb::PortalAssignmentArgs {
                datasets: Some(datasets),
                workers: Some(workers),
            },
        );

        self.builder.finish(root, None);
        self.builder.finished_data().to_vec()
    }

    /// Appends one row across every column. With [`AssignmentBuilder::check_continuity`] off, a
    /// gap still appends and only reports.
    fn push_chunk(
        &mut self,
        top: u64,
        block_range: RangeInclusive<u64>,
        hash: assignment_fb::ChunkHash,
        version: u32,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        let continuity = match self.last_block {
            Some(last) if last + 1 != *block_range.start() => Err(anyhow::anyhow!(
                "Chunks in the dataset must be contiguous, got {} -> {}",
                last,
                block_range.start()
            )),
            _ => Ok(()),
        };
        if continuity.is_err() && self.check_continuity {
            return continuity;
        }

        let index: u32 = self
            .columns
            .first_blocks
            .len()
            .try_into()
            .context("a dataset may hold at most u32::MAX chunks")?;
        let delta: u32 = (block_range.end() - block_range.start())
            .try_into()
            .context("a chunk may span at most u32::MAX blocks")?;

        if self.columns.tops.last().is_none_or(|run| run.top() != top) {
            self.columns.tops.push(assignment_fb::TopRun::new(index, top));
        }
        self.columns.first_blocks.push(*block_range.start());
        self.columns.block_deltas.push(delta);
        self.columns.hashes.push(hash);
        self.columns.versions.push(version);
        self.columns.timestamps.push(timestamp);
        let Self {
            columns,
            staged_worker_indexes,
            ..
        } = self;
        columns.worker_indexes.extend_from_slice(staged_worker_indexes);
        let end = self
            .columns
            .worker_indexes
            .len()
            .try_into()
            .context("a dataset may hold at most u32::MAX worker references")?;
        self.columns.worker_ends.push(end);

        self.last_block = Some(*block_range.end());
        continuity
    }
}

/// One dataset of a [`PortalAssignmentBuilder`], opened by
/// [`new_dataset`](PortalAssignmentBuilder::new_dataset).
#[must_use = "a dataset is emitted by `finish`; a builder that is dropped emits nothing"]
pub struct PortalDatasetBuilder<'b> {
    p: &'b mut PortalAssignmentBuilder,
    id: fb::WIPOffset<&'static str>,
    read_schema_id: u32,
    /// Set by `finish`, so `Drop` only cleans up after a dataset that was never emitted.
    emitted: bool,
}

impl PortalDatasetBuilder<'_> {
    pub fn new_chunk(&mut self) -> PortalAssignmentChunkBuilder<'_> {
        PortalAssignmentChunkBuilder::new(self.p)
    }

    /// Emits the dataset. `last_block_hash` is the head block's full hash, which ingest records on
    /// the last chunk, so it arrives here rather than at `new_dataset`.
    ///
    /// # Errors
    ///
    /// If no chunk was staged. Either way the dataset is closed and its chunks are released, so a
    /// rejection can't leak into whatever is opened next.
    pub fn finish(mut self, last_block_hash: Option<&str>) -> anyhow::Result<()> {
        self.emitted = true;
        let result = self.p.emit_dataset(self.id, self.read_schema_id, last_block_hash);
        if result.is_err() {
            self.p.discard_dataset();
        }
        result
    }
}

impl Drop for PortalDatasetBuilder<'_> {
    fn drop(&mut self) {
        if !self.emitted {
            self.p.discard_dataset();
        }
    }
}

/// The pieces a chunk id is made of; the reader reassembles them the same way.
struct ParsedChunkId {
    top: u64,
    first_block: u64,
    last_block: u64,
    hash: assignment_fb::ChunkHash,
}

/// Splits `"0221000000/0221000000-0221000649-9QgFD"` into the parts the columns hold.
fn parse_chunk_id(id: &str) -> anyhow::Result<ParsedChunkId> {
    let (top, rest) = id
        .split_once('/')
        .with_context(|| format!("chunk id '{id}' has no top directory"))?;
    let mut parts = rest.splitn(3, '-');
    let (Some(first_block), Some(last_block), Some(hash)) =
        (parts.next(), parts.next(), parts.next())
    else {
        anyhow::bail!("chunk id '{id}' is not <top>/<first_block>-<last_block>-<hash>");
    };
    // Portal chunk ids require a 5-to-8 character hash.
    anyhow::ensure!(
        (5..=8).contains(&hash.len())
            && hash.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "chunk id '{id}' has a hash that is not 5 to 8 word characters"
    );
    let mut bytes = [0u8; 8];
    bytes[..hash.len()].copy_from_slice(hash.as_bytes());
    Ok(ParsedChunkId {
        top: top.parse().with_context(|| format!("chunk id '{id}' has a non-numeric top"))?,
        first_block: first_block
            .parse()
            .with_context(|| format!("chunk id '{id}' has a non-numeric first block"))?,
        last_block: last_block
            .parse()
            .with_context(|| format!("chunk id '{id}' has a non-numeric last block"))?,
        hash: assignment_fb::ChunkHash::new(&bytes),
    })
}

pub struct PortalAssignmentChunkBuilder<'b> {
    p: &'b mut PortalAssignmentBuilder,

    block_range: Option<RangeInclusive<u64>>,
    /// Parsed by [`Self::id`], since nothing downstream wants the string back. The error is held
    /// rather than returned so it still surfaces at `finish`.
    parsed_id: Option<anyhow::Result<ParsedChunkId>>,
    version: u32,
    last_block_timestamp: u64,
}

impl<'b> PortalAssignmentChunkBuilder<'b> {
    fn new(parent: &'b mut PortalAssignmentBuilder) -> Self {
        parent.staged_worker_indexes.clear();
        Self {
            p: parent,
            block_range: None,
            parsed_id: None,
            version: 0,
            last_block_timestamp: 0,
        }
    }

    /// The chunk id, e.g. `"0221000000/0221000000-0221000649-9QgFD"`. Not stored as such: it is
    /// split into the `tops`, `first_blocks`, `block_deltas` and `hashes` columns and reassembled
    /// on read, so it must parse. Parsing happens here rather than at `finish`, which keeps the
    /// string out of the staged chunk; a malformed id is still reported by [`Self::finish`].
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn id(mut self, id: &str) -> Self {
        self.parsed_id = Some(parse_chunk_id(id));
        self
    }

    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn block_range(mut self, range: RangeInclusive<u64>) -> Self {
        self.block_range = Some(range);
        self
    }

    /// Absolute epoch milliseconds. Defaults to 0 when absent.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn last_block_timestamp(mut self, timestamp: u64) -> Self {
        self.last_block_timestamp = timestamp;
        self
    }

    /// Which copy of the chunk workers serve. Defaults to 0, the ingested one; must match the
    /// version the same chunk carries in the worker assignment (see
    /// [`WorkerAssignmentChunkBuilder::version`]).
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Confirmed routing (which workers portals should route to), not the raw ideal placement.
    #[must_use = "a chunk is staged by `finish`; a builder that is dropped stages nothing"]
    pub fn worker_indexes(self, indexes: &[u16]) -> Self {
        self.p.staged_worker_indexes.clear();
        self.p.staged_worker_indexes.extend_from_slice(indexes);
        self
    }

    /// # Panics
    ///
    /// If `block_range` was never set — API misuse, not bad input.
    ///
    /// # Errors
    ///
    /// If the id is unset or malformed, if it disagrees with `block_range` — the two encode the
    /// same block numbers, and the id is rebuilt from the range on read — or if the chunk breaks
    /// block continuity (see [`PortalAssignmentBuilder::check_continuity`]).
    pub fn finish(self) -> anyhow::Result<()> {
        let block_range = self.block_range.expect("Block range must be set");
        let parsed = self.parsed_id.context("Chunk id must be set")??;
        anyhow::ensure!(
            parsed.first_block == *block_range.start() && parsed.last_block == *block_range.end(),
            "chunk id names blocks {}-{}, but the chunk covers {}-{}",
            parsed.first_block,
            parsed.last_block,
            block_range.start(),
            block_range.end()
        );
        self.p.push_chunk(
            parsed.top,
            block_range,
            parsed.hash,
            self.version,
            self.last_block_timestamp,
        )
    }
}
