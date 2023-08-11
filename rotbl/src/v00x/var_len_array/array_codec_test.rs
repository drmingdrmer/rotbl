use pretty_assertions::assert_eq;

use crate::codec::Codec;
use crate::v00x::RawVLArray;
use crate::v00x::RawVLArrayBuilder;

#[test]
fn test_build() -> anyhow::Result<()> {
    let b = RawVLArrayBuilder::new(None);

    let vla = b.build(vec!["hello", "world", "foo", "bar"]);

    let mut buf = Vec::new();
    vla.encode(&mut buf)?;

    let mut want: Vec<u8> = vec![
        b'v', b'l', b'a', 0, 0, 0, 0, 0, // type
        0, 0, 0, 0, 0, 0, 0, 1, // version
        0, 0, 0, 0, 0, 0, 0, 4, // len
        0, 0, 0, 0, // offset[0]
        0, 0, 0, 5, // offset[1]
        0, 0, 0, 10, // offset[2]
        0, 0, 0, 13, // offset[3]
        0, 0, 0, 16, // offset[4]
    ];

    want.extend_from_slice(b"hello");
    want.extend_from_slice(b"world");
    want.extend_from_slice(b"foo");
    want.extend_from_slice(b"bar");

    assert_eq!(want, buf);

    let got = RawVLArray::decode(&buf[..])?;

    assert_eq!(vla, got);

    Ok(())
}
