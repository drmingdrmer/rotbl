use std::ops::RangeBounds;

/// A helper trait to accept a range argument: `RangeBounds<T> + Clone + Send + 'static`.
///
/// Range arguments used by Rotbl implements this trait,
/// such as `Rotbl::range(impl RangeArg<String>)`.
pub trait RangeArg<T = String>: RangeBounds<T> + Clone + Send + 'static {}

impl<T, R> RangeArg<T> for R where R: RangeBounds<T> + Clone + Send + 'static {}
