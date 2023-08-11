use pretty_assertions::assert_eq;

use crate::v00x::RawVLArrayBuilder;

#[test]
fn test_build() -> anyhow::Result<()> {
    let b = RawVLArrayBuilder::new(None);

    let vla = b.build(vec!["hello", "world", "foo", "bar"]);

    assert_eq!(4, vla.len());
    assert_eq!(b"hello", vla.get(0).unwrap());
    assert_eq!(b"world", vla.get(1).unwrap());
    assert_eq!(b"foo", vla.get(2).unwrap());
    assert_eq!(b"bar", vla.get(3).unwrap());
    assert_eq!(None, vla.get(4));

    Ok(())
}
