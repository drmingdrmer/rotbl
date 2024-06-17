use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use rotbl::v001::BlockCacheConfig;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;

#[derive(Clone, Debug, PartialEq, Eq, clap::Parser)]
#[clap(about = "dump Rotbl v001 data", author)]
pub struct Args {
    #[arg(value_name = "PATH")]
    path: PathBuf,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let config = Config::default().with_block_cache_config(
        BlockCacheConfig::default().with_max_items(100).with_capacity(256 * 1024 * 1024),
    );

    let r = Rotbl::open(config, args.path).unwrap();
    let r = Arc::new(r);

    for s in r.dump() {
        println!("{}", s?);
    }
    Ok(())
}
