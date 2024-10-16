use codeq::Decode;
use codeq::Encode;

use crate::typ::Type;

#[test]
fn test_codec() -> anyhow::Result<()> {
    let x = Type::VLArray;
    let want = b"vla\0\0\0\0\0";

    let mut buf = Vec::new();
    x.encode(&mut buf)?;

    assert_eq!(8, buf.len());
    assert_eq!(want, &buf[..]);

    let got = Type::decode(&buf[..])?;
    assert_eq!(x, got);
    Ok(())
}
