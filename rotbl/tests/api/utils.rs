use std::cell::RefCell;
use std::future::Future;

use libtest_mimic::Failed;
use libtest_mimic::Trial;
use rotbl::storage::Storage;

use crate::context::TestContext;

/// Create a string
pub(crate) fn ss(x: impl ToString) -> String {
    x.to_string()
}

/// Create a byte vector
pub(crate) fn bb(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}

pub trait NewContext<S>
where
    S: Storage,
    Self: Fn() -> anyhow::Result<TestContext<S>> + Send + Clone + 'static,
{
}

impl<S, T> NewContext<S> for T
where
    S: Storage,
    T: Fn() -> anyhow::Result<TestContext<S>> + Send + Clone + 'static,
{
}

thread_local! {
    /// Define a thread-local variable to store the context info.
    ///
    /// It stores the name of the storage engine.
    /// This context info is used to build the name of the test case.
    pub(crate) static CONTEXT_INFO: RefCell<&'static str> = const { RefCell::new("") };
}

fn get_trial_name(case_name: &str) -> String {
    let ctx_info = CONTEXT_INFO.with_borrow_mut(|name| *name);
    format!("api::{case_name}/ctx:{ctx_info}")
}

/// Build a Trial instance that runs an async test.
///
/// The Trial build a new tokio runtime and a new test context, then run the test in it.
pub fn build_trial<S, F>(name: &str, new_ctx: impl NewContext<S>, f: F) -> Trial
where
    S: Storage,
    F: FnOnce(TestContext<S>) -> anyhow::Result<()> + Send + 'static,
{
    Trial::test(get_trial_name(name), move || {
        let ctx = new_ctx()?;

        f(ctx).map_err(|err| Failed::from(err.to_string()))?;
        Ok(())
    })
}

/// Build a Trial instance that runs an async test.
///
/// The Trial build a new tokio runtime and a new test context, then run the test in it.
pub fn build_async_trial<S, F, Fut>(name: &str, new_ctx: impl NewContext<S>, f: F) -> Trial
where
    S: Storage,
    F: FnOnce(TestContext<S>) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>>,
{
    Trial::test(get_trial_name(name), move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| Failed::from(err.to_string()))?;

        let ctx = new_ctx()?;

        rt.block_on(f(ctx)).map_err(|err| Failed::from(err.to_string()))?;
        Ok(())
    })
}

/// Build a list of Trial instances that runs a list of async tests.
#[macro_export]
macro_rules! trials {
    ($new_ctx:ident, $($test:ident),*) => {
        vec![$(
            $crate::utils::build_trial(stringify!($test), $new_ctx.clone(), $test),
        )*]
    };
}

/// Build a list of Trial instances that runs a list of async tests.
#[macro_export]
macro_rules! async_trials {
    ($new_ctx:ident, $($test:ident),*) => {
        vec![$(
            $crate::utils::build_async_trial(stringify!($test), $new_ctx.clone(), $test),
        )*]
    };
}
