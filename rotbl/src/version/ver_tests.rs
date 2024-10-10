use codeq::Codec;
use pretty_assertions::assert_eq;

use crate::version::Version;

#[test]
fn codec() -> anyhow::Result<()> {
    let v1 = Version::V001;

    let mut buf = Vec::new();
    v1.encode(&mut buf)?;
    assert_eq!(vec![0, 0, 0, 0, 0, 0, 0, 1], buf);

    let v1_got = Version::decode(&buf[..])?;

    assert_eq!(v1, v1_got);

    Ok(())
}
