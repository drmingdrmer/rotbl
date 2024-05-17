// use borsh::from_slice;
// use borsh::to_vec;
// use msgpacker::Packable;
// use msgpacker::Unpackable;

#[derive(Debug, Clone)]
// #[derive(bincode::Decode, bincode::Encode)]
#[derive(serde::Serialize, serde::Deserialize)]
// #[derive(borsh::BorshSerialize, borsh::BorshDeserialize)]
// #[derive(msgpacker::MsgPacker)]
struct Foo2 {
    a: u32,
    b: u32,
}

#[derive(Debug, Clone)]
// #[derive(bincode::Decode, bincode::Encode)]
#[derive(serde::Serialize, serde::Deserialize)]
// #[derive(borsh::BorshSerialize, borsh::BorshDeserialize)]
// #[derive(msgpacker::MsgPacker)]
struct Foo3 {
    a: u32,
    b: u32,
    // #[serde(skip_deserializing)]
    // #[borsh(skip)]
    c: Option<u64>,
}

#[ignore]
#[test]
fn test_add_field() -> anyhow::Result<()> {
    let f = Foo2 { a: 1, b: 2 };

    let v = bincode::serde::encode_to_vec(f, bincode_config()).unwrap();
    let (foo3, _): (Foo3, _) = bincode::serde::decode_from_slice(&v[..], bincode_config())?;

    // let v = borsh::to_vec(&foo).unwrap();
    // let foo3: Foo3 = borsh::from_slice(&v[..])?;

    // let mut buf = Vec::new();
    // foo.pack(&mut buf);
    // let (n, foo3) = Foo3::unpack(&buf)?;

    // let v = rmp_serde::to_vec_named(&foo)?;
    // let foo3: Foo3 = rmp_serde::from_slice(&v)?;

    println!("{:?}", foo3);
    Ok(())
}

pub fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard().with_big_endian().with_variable_int_encoding()
}
