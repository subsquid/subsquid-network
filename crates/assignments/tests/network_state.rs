use sqd_assignments::{
    AssignmentType, NetworkAssignmentV2, NetworkState, ResolvedAssignments, SchemaBundle,
};

const LEGACY_STATE: &str = r#"{
  "network": "testnet",
  "assignment": {
    "url": "",
    "fb_url": "https://example.test/legacy.fb.0.gz",
    "fb_url_v1": "https://example.test/legacy.fb.1.gz",
    "id": "2026-06-09T12:00:00_LEGACY",
    "effective_from": 1781000000
  }
}"#;

/// The migration's end state: the legacy blob is gone and the split pair is the only source.
const SPLIT_ONLY_STATE: &str = r#"{
  "network": "testnet",
  "assignment_type": "split",
  "worker_assignment": {
    "fb_url": "https://example.test/worker.fb.1.gz",
    "id": "worker",
    "version": "1"
  },
  "portal_assignment": {
    "fb_url": "https://example.test/portal.fb.1.gz",
    "id": "portal",
    "version": "1",
    "effective_from": 1781000000
  },
  "schema_bundle": {
    "hash": "a1b2c3",
    "url": "https://example.test/schema.bundle.gz"
  }
}"#;

#[test]
fn legacy_network_state_deserializes() {
    let state: NetworkState = serde_json::from_str(LEGACY_STATE).unwrap();

    assert_eq!(state.network, "testnet");
    assert_eq!(state.assignment.unwrap().id, "2026-06-09T12:00:00_LEGACY");

    assert!(state.worker_assignment.is_none());
    assert!(state.portal_assignment.is_none());
    assert!(state.schema_bundle.is_none());
}

#[allow(deprecated)]
#[test]
fn deprecated_assignment_urls_default_and_skip_when_absent() {
    let state: NetworkState = serde_json::from_str(
        r#"{
          "network": "testnet",
          "assignment": {
            "fb_url_v1": "https://example.test/legacy.fb.1.gz",
            "id": "legacy",
            "effective_from": 1781000000
          }
        }"#,
    )
    .unwrap();

    let assignment = state.assignment.as_ref().unwrap();
    assert!(assignment.url.is_none());
    assert!(assignment.fb_url.is_none());

    let serialized = serde_json::to_value(state).unwrap();
    assert!(serialized["assignment"].get("url").is_none());
    assert!(serialized["assignment"].get("fb_url").is_none());
}

/// The middle of the migration: both shapes published at once, `assignment_type` picking between
/// them so consumers that predate the split keep reading `assignment`.
#[test]
fn split_network_state_deserializes() {
    let state: NetworkState = serde_json::from_str(
        r#"{
          "network": "testnet",
          "assignment_type": "split",
          "assignment": {
            "url": "",
            "fb_url": "https://example.test/legacy.fb.0.gz",
            "fb_url_v1": "https://example.test/legacy.fb.1.gz",
            "id": "legacy",
            "effective_from": 1781000000
          },
          "worker_assignment": {
            "fb_url": "https://example.test/worker.fb.1.gz",
            "id": "worker",
            "version": "1"
          },
          "portal_assignment": {
            "fb_url": "https://example.test/portal.fb.1.gz",
            "id": "portal",
            "version": "1"
          },
          "schema_bundle": {
            "hash": "a1b2c3",
            "url": "https://example.test/schema.bundle.gz"
          }
        }"#,
    )
    .unwrap();

    assert_eq!(state.assignment_type, AssignmentType::Split);
    assert_eq!(state.assignment.as_ref().unwrap().id, "legacy");
    assert_eq!(
        state.worker_assignment.clone().unwrap(),
        NetworkAssignmentV2 {
            id: "worker".to_owned(),
            fb_url: "https://example.test/worker.fb.1.gz".to_owned(),
            version: "1".to_owned(),
            effective_from: None,
        }
    );
    assert_eq!(state.portal_assignment.as_ref().unwrap().id, "portal");

    assert_eq!(
        state.schema_bundle,
        Some(SchemaBundle {
            hash: "a1b2c3".to_owned(),
            url: "https://example.test/schema.bundle.gz".to_owned(),
        })
    );
}

#[test]
fn split_only_network_state_deserializes() {
    let state: NetworkState = serde_json::from_str(SPLIT_ONLY_STATE).unwrap();

    assert!(state.assignment.is_none(), "a migrated network publishes no legacy assignment");
    assert_eq!(state.worker_assignment.unwrap().id, "worker");
    assert_eq!(state.portal_assignment.as_ref().unwrap().id, "portal");
    assert_eq!(state.portal_assignment.as_ref().unwrap().effective_from, Some(1781000000));
    assert!(state.schema_bundle.is_some());
}

/// Parsing still models the wire format rather than the publisher's rules; `validate` is what
/// refuses a state whose declared type has nothing behind it.
#[test]
fn network_state_without_any_assignment_is_accepted() {
    let state: NetworkState = serde_json::from_str(r#"{"network": "testnet"}"#).unwrap();

    assert!(state.assignment.is_none());
    assert!(state.worker_assignment.is_none());
    assert!(state.portal_assignment.is_none());
    assert!(state.schema_bundle.is_none());

    assert_eq!(state.resolve(None).unwrap_err().missing, "assignment");
}

#[test]
fn schema_bundle_requires_url() {
    for bundle in [r#"{"hash": "a1b2c3"}"#, r#"{"hash": "a1b2c3", "url": null}"#] {
        let json = format!(
            r#"{{
              "network": "testnet",
              "assignment": {{
                "fb_url_v1": "https://example.test/legacy.fb.1.gz",
                "id": "legacy",
                "effective_from": 1781000000
              }},
              "schema_bundle": {bundle}
            }}"#
        );

        serde_json::from_str::<NetworkState>(&json)
            .expect_err(&format!("schema bundle without a url must be rejected: {bundle}"));
    }
}

#[test]
fn schema_bundle_round_trips_through_json() {
    let bundle = SchemaBundle {
        hash: "a1b2c3".to_owned(),
        url: "https://example.test/schema.bundle.gz".to_owned(),
    };

    let serialized = serde_json::to_value(&bundle).unwrap();
    assert_eq!(serialized["hash"], "a1b2c3");
    assert_eq!(serialized["url"], "https://example.test/schema.bundle.gz");

    let round_tripped: SchemaBundle = serde_json::from_value(serialized).unwrap();
    assert_eq!(round_tripped, bundle);
}

#[test]
fn absent_split_assignments_stay_omitted_after_json_round_trip() {
    let state: NetworkState = serde_json::from_str(LEGACY_STATE).unwrap();

    let serialized = serde_json::to_string(&state).unwrap();
    let round_tripped: NetworkState = serde_json::from_str(&serialized).unwrap();
    let value = serde_json::to_value(round_tripped).unwrap();

    assert!(value.get("assignment").is_some());
    assert!(value.get("worker_assignment").is_none());
    assert!(value.get("portal_assignment").is_none());
    assert!(value.get("schema_bundle").is_none());
}

#[test]
fn absent_legacy_assignment_stays_omitted_after_json_round_trip() {
    let state: NetworkState = serde_json::from_str(SPLIT_ONLY_STATE).unwrap();

    let serialized = serde_json::to_string(&state).unwrap();
    let round_tripped: NetworkState = serde_json::from_str(&serialized).unwrap();
    let value = serde_json::to_value(round_tripped).unwrap();

    assert!(
        value.get("assignment").is_none(),
        "a null legacy assignment must not be emitted"
    );
    assert!(value.get("worker_assignment").is_some());
    assert!(value.get("portal_assignment").is_some());
    assert!(value.get("schema_bundle").is_some());
}

const LEGACY: &str = r#""assignment": {"fb_url_v1": "l", "id": "l", "effective_from": 1}"#;
const WORKER: &str = r#""worker_assignment": {"fb_url": "w", "id": "w", "version": "1"}"#;
const PORTAL: &str = r#""portal_assignment": {"fb_url": "p", "id": "p", "version": "1"}"#;
const BUNDLE: &str = r#""schema_bundle": {"hash": "h", "url": "u"}"#;

fn state(assignment_type: &str, fields: &[&str]) -> NetworkState {
    let json = format!(
        r#"{{"network": "t", "assignment_type": "{assignment_type}", {}}}"#,
        fields.join(", ")
    );
    serde_json::from_str(&json).unwrap()
}

#[test]
fn absent_assignment_type_means_legacy() {
    let state: NetworkState = serde_json::from_str(LEGACY_STATE).unwrap();

    assert_eq!(state.assignment_type, AssignmentType::Legacy);
    assert_eq!(serde_json::to_value(&state).unwrap()["assignment_type"], "legacy");
    assert!(state.resolve(None).is_ok());
}

#[test]
fn resolve_requires_the_blobs_the_assignment_type_names() {
    for (assignment_type, fields, missing) in [
        ("legacy", vec![WORKER, PORTAL, BUNDLE], Some("assignment")),
        ("split", vec![PORTAL, BUNDLE], Some("worker_assignment")),
        ("split", vec![WORKER, BUNDLE], Some("portal_assignment")),
        ("split", vec![WORKER, PORTAL], Some("schema_bundle")),
        ("legacy", vec![LEGACY, WORKER], None),
        ("split", vec![LEGACY, WORKER, PORTAL, BUNDLE], None),
    ] {
        let resolved = state(assignment_type, &fields).resolve(None);

        assert_eq!(resolved.err().map(|e| e.missing), missing, "{fields:?}");
    }
}

#[test]
fn malformed_assignment_types_and_v2_blobs_are_rejected() {
    let bad_type = [r#""LEGACY""#, r#""combined""#, "null", "0"];
    for value in bad_type {
        let json = format!(r#"{{"network": "t", "assignment_type": {value}}}"#);
        serde_json::from_str::<NetworkState>(&json).expect_err(&format!("bad type: {value}"));
    }

    // A url is required and non-null, unlike the optional urls on the legacy assignment.
    for blob in [
        r#"{"id": "w", "version": "1"}"#,
        r#"{"id": "w", "fb_url": null, "version": "1"}"#,
        r#"{"id": "w", "fb_url": "w"}"#,
    ] {
        let json = format!(r#"{{"network": "t", "worker_assignment": {blob}}}"#);
        serde_json::from_str::<NetworkState>(&json).expect_err(&format!("bad blob: {blob}"));
    }
}

/// The override wins over the state's own type, so a consumer can be switched over ahead of the
/// network — or held back — without the publisher changing anything.
#[test]
fn resolve_prefers_the_override_over_the_states_own_type() {
    let joint = || state("legacy", &[LEGACY, WORKER, PORTAL, BUNDLE]);

    assert!(
        matches!(joint().resolve(None).unwrap(), ResolvedAssignments::Legacy(a) if a.id == "l")
    );
    assert!(matches!(
        joint().resolve(Some(AssignmentType::Split)).unwrap(),
        ResolvedAssignments::Split { worker, .. } if worker.id == "w"
    ));

    // An override still has to be backed by published blobs, and the error names the override.
    let error = state("split", &[WORKER, PORTAL, BUNDLE])
        .resolve(Some(AssignmentType::Legacy))
        .unwrap_err();
    assert_eq!(error.assignment_type, AssignmentType::Legacy);
    assert_eq!(error.missing, "assignment");
}
