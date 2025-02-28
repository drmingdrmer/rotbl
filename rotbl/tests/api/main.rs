use libtest_mimic::Arguments;
use libtest_mimic::Trial;
use rotbl::storage::Storage;
use rotbl::v001::Config;

use crate::context::TestContext;
use crate::utils::NewContext;
use crate::utils::CONTEXT_INFO;

pub mod context;
pub mod temp_table;
pub mod utils;

pub mod test_create_open;
pub mod test_dump;
pub mod test_rotbl_block;
pub mod test_rotbl_cache_stat;
pub mod test_rotbl_read;

fn main() -> anyhow::Result<()> {
    let args = Arguments::from_args();

    let new_fs_ctx = || {
        let mut config = Config::default();
        config.block_config.max_items = Some(3);

        TestContext::new_fs(config)
    };

    let mut tests = Vec::new();

    collect_trials(&mut tests, "fs", new_fs_ctx);

    // Don't init logging while building operator which may break cargo
    // nextest output
    // let _ = tracing_subscriber::fmt()
    //     .pretty()
    //     .with_test_writer()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .try_init();

    let conclusion = libtest_mimic::run(&args, tests);

    conclusion.exit()
}

fn collect_trials<S>(tests: &mut Vec<Trial>, ctx_name: &'static str, new_ctx: impl NewContext<S>)
where S: Storage {
    CONTEXT_INFO.with_borrow_mut(|name| {
        *name = ctx_name;
    });

    test_create_open::tests(new_ctx.clone(), tests);
    test_dump::tests(new_ctx.clone(), tests);
    test_rotbl_block::tests(new_ctx.clone(), tests);
    test_rotbl_cache_stat::tests(new_ctx.clone(), tests);
    test_rotbl_read::tests(new_ctx.clone(), tests);
}
