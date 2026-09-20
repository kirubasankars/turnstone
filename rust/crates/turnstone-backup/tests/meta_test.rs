use turnstone_backup::{validate_restore_chain, Meta, TypeDifferential, TypeFull};

use time::OffsetDateTime;

fn sample_meta(ty: &str, base: u64, end: u64, sha: &str, parent: &str) -> Meta {
    Meta {
        timestamp: OffsetDateTime::now_utc(),
        database: "1".into(),
        ty: ty.into(),
        base_lsn: base,
        end_lsn: end,
        parent_sha256: parent.into(),
        compressed: false,
        sha256: sha.into(),
    }
}

#[test]
fn validate_restore_chain_ok() {
    let full = sample_meta(TypeFull, 0, 100, "aaa", "");
    let diff = sample_meta(TypeDifferential, 100, 250, "bbb", "aaa");
    validate_restore_chain(&[full]).unwrap();
    validate_restore_chain(&[sample_meta(TypeFull, 0, 100, "aaa", ""), diff]).unwrap();
}

#[test]
fn validate_restore_chain_errors() {
    let full = sample_meta(TypeFull, 0, 100, "aaa", "");
    assert!(validate_restore_chain(&[]).is_err());

    let mut bad = sample_meta(TypeDifferential, 99, 250, "bbb", "aaa");
    assert!(validate_restore_chain(&[full.clone(), bad.clone()]).is_err());

    bad.parent_sha256 = "wrong".into();
    bad.base_lsn = 100;
    assert!(validate_restore_chain(&[full.clone(), bad]).is_err());

    let bad_first = sample_meta(TypeDifferential, 0, 100, "aaa", "");
    assert!(validate_restore_chain(&[bad_first]).is_err());
}
