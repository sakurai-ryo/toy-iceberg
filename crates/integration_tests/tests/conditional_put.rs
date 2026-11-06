//! The catalog depends on these object store semantics. Requires RustFS (`mise run up`).

use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use toy_iceberg_core::catalog::{Catalog, S3Config};

#[tokio::test]
async fn rustfs_enforces_if_none_match_and_if_match() {
    let catalog = Catalog::s3(&S3Config::from_env()).unwrap();
    let store = catalog.store();
    let path = Path::from(format!("conditional-put/{}", uuid::Uuid::new_v4()));
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    let first = store.put_opts(&path, "a".into(), create()).await.unwrap();
    let again = store.put_opts(&path, "b".into(), create()).await;
    assert!(matches!(
        again,
        Err(object_store::Error::AlreadyExists { .. })
    ));

    let version = UpdateVersion {
        e_tag: first.e_tag,
        version: None,
    };
    let update = |v: UpdateVersion| PutOptions {
        mode: PutMode::Update(v),
        ..Default::default()
    };
    store
        .put_opts(&path, "c".into(), update(version.clone()))
        .await
        .unwrap();
    let stale = store.put_opts(&path, "d".into(), update(version)).await;
    assert!(matches!(
        stale,
        Err(object_store::Error::Precondition { .. })
    ));

    assert_eq!(store.get(&path).await.unwrap().bytes().await.unwrap(), "c");
}
