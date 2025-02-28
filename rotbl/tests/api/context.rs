use std::path::Path;
use std::sync::Arc;

use rotbl::storage::impls::fs::FsStorage;
use rotbl::storage::Storage;
use rotbl::v001::Config;
use rotbl::v001::DB;
use tempfile::TempDir;

pub struct TestContext<S>
where S: Storage
{
    #[allow(dead_code)]
    config: Config,
    db: Arc<DB>,

    temp_dir: TempDir,
    storage: S,
}

impl TestContext<FsStorage> {
    pub fn new() -> anyhow::Result<Self> {
        let mut config = Config::default();
        config.block_config.max_items = Some(3);

        Self::with_config(config)
    }

    pub fn with_config(config: Config) -> anyhow::Result<Self> {
        let db = DB::open(config.clone())?;
        let temp_dir = tempfile::tempdir()?;

        let storage = FsStorage::new(temp_dir.path().to_path_buf());

        Ok(TestContext {
            config,
            db,
            temp_dir,
            storage,
        })
    }
}

impl<S> TestContext<S>
where S: Storage
{
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }

    pub fn base_dir(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn storage(&self) -> S {
        self.storage.clone()
    }
}
