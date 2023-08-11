mod array;
mod builder;
mod payload;

#[cfg(test)]
mod array_codec_test;
#[cfg(test)]
mod builder_test;

pub use array::RawVLArray;
pub use builder::RawVLArrayBuilder;
