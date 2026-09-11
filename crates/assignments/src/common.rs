use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum WorkerStatus {
    Ok,
    Unreliable,
    DeprecatedVersion,
    UnsupportedVersion,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkAssignment {
    /// Deprecated: use `fb_url_v1` instead.
    #[deprecated(note = "use fb_url_v1 instead")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Deprecated: use `fb_url_v1` instead.
    #[deprecated(note = "use fb_url_v1 instead")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fb_url: Option<String>,
    pub fb_url_v1: Option<String>,
    pub id: String,
    pub effective_from: u64,
}

/// Where a split assignment blob lives. Replaces [`NetworkAssignment`] for the split blobs: one
/// required url plus the format version of what is at it, rather than a field per format.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NetworkAssignmentV2 {
    pub id: String,
    pub fb_url: String,
    /// Format of the blob at `fb_url`, not of this struct. Free-form until the format settles.
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_from: Option<u64>,
}

/// Where to fetch the schema content that assignments only reference by id — a worker chunk's
/// `write_schema_id`, a portal dataset's `read_schema_id`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    /// Content hash of the bundle at `url`, so a cached copy can be reused across states.
    pub hash: String,
    pub url: String,
}

/// Which of a state's assignments a consumer should read. A migrating state publishes both shapes
/// at once, so which blobs are present says nothing about which are authoritative.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AssignmentType {
    /// Also what an absent `assignment_type` means: such a state predates the split.
    #[default]
    Legacy,
    Split,
}

impl std::fmt::Display for AssignmentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Legacy => "legacy",
            Self::Split => "split",
        })
    }
}

/// The published network state.
///
/// Migrating to split assignments walks a state through three shapes: `assignment` alone, then
/// `assignment` alongside the split blobs while consumers switch over, then the split blobs
/// alone. The middle shape serves both generations at once, so every blob is optional and
/// publishing both sets is expected — [`Self::assignment_type`] is what picks between them.
#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkState {
    pub network: String,

    /// Which of the assignments below to read. Absent means [`AssignmentType::Legacy`], so states
    /// published before this field existed still read correctly.
    #[serde(default)]
    pub assignment_type: AssignmentType,

    /// The combined assignment, served identically to workers and portals. Stays published
    /// alongside the split blobs until nothing reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<NetworkAssignment>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_assignment: Option<NetworkAssignmentV2>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portal_assignment: Option<NetworkAssignmentV2>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_bundle: Option<SchemaBundle>,
}

/// The assignments an [`AssignmentType`] names, taken out of a [`NetworkState`].
#[derive(Debug)]
pub enum ResolvedAssignments {
    Legacy(NetworkAssignment),
    Split {
        worker: NetworkAssignmentV2,
        portal: NetworkAssignmentV2,
        schema_bundle: SchemaBundle,
    },
}

impl NetworkState {
    /// Takes the assignments named by `assignment_type`, falling back to [`Self::assignment_type`]
    /// when it is `None`. The override lets a consumer pin a shape regardless of what the state
    /// says, which is how one gets switched over ahead of — or held back during — the migration.
    ///
    /// Consuming: what it leaves behind belongs to consumers on the other side of the migration.
    ///
    /// # Errors
    ///
    /// [`InvalidNetworkState`] if the state does not publish the assignments named.
    pub fn resolve(
        self,
        assignment_type: Option<AssignmentType>,
    ) -> Result<ResolvedAssignments, InvalidNetworkState> {
        let assignment_type = assignment_type.unwrap_or(self.assignment_type);
        let missing = |missing| InvalidNetworkState {
            assignment_type,
            missing,
        };

        match assignment_type {
            AssignmentType::Legacy => self
                .assignment
                .map(ResolvedAssignments::Legacy)
                .ok_or_else(|| missing("assignment")),
            AssignmentType::Split => {
                match (self.worker_assignment, self.portal_assignment, self.schema_bundle) {
                    (Some(worker), Some(portal), Some(schema_bundle)) => {
                        Ok(ResolvedAssignments::Split {
                            worker,
                            portal,
                            schema_bundle,
                        })
                    }
                    (None, ..) => Err(missing("worker_assignment")),
                    (_, None, _) => Err(missing("portal_assignment")),
                    _ => Err(missing("schema_bundle")),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("assignment_type is \"{assignment_type}\" but {missing} is not published")]
pub struct InvalidNetworkState {
    pub assignment_type: AssignmentType,
    pub missing: &'static str,
}
