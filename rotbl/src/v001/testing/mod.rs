#![allow(dead_code)]

use std::any::type_name;
use std::fmt::Debug;

use crate::codec::Codec;

/// Test decoding from correct data and corrupted data.
pub(crate) fn test_codec<D: Codec + PartialEq + Debug>(encoded_bytes: &[u8], v: &D) -> anyhow::Result<()> {
    // convert `correct` to string if possible
    let correct_str = String::from_utf8_lossy(encoded_bytes);
    println!("correct data: {}", correct_str);

    let mes = format!("Type: {} encoded data: {:?}", type_name::<D>(), correct_str);

    // Test encoding
    {
        let mut b = Vec::new();
        let n = v.encode(&mut b)?;
        assert_eq!(n, b.len(), "output len, {}", &mes);
        assert_eq!(b, encoded_bytes, "output data, {}", &mes);
    }

    // Assert the input is correct

    {
        let b = encoded_bytes.to_vec();
        let decoded = D::decode(&mut b.as_slice())?;
        assert_eq!(v, &decoded, "decode, {}", &mes);
    }

    // Assert corrupted data returns error
    for i in 0..encoded_bytes.len() {
        let mut corrupted = encoded_bytes.to_vec();
        corrupted[i] = corrupted[i].wrapping_add(1);

        let res = D::decode(&mut corrupted.as_slice());
        assert!(
            res.is_err(),
            "change {}-th byte for type {}; the correct encoded data is: {}",
            i,
            type_name::<D>(),
            correct_str
        );
    }

    Ok(())
}

/// Create a string
pub(crate) fn ss(x: impl ToString) -> String {
    x.to_string()
}

/// Create a String vector from multiple strings
pub(crate) fn ss_vec(x: impl IntoIterator<Item = impl ToString>) -> Vec<String> {
    let r = x.into_iter().map(|x| x.to_string());
    r.collect()
}

/// Create a byte vector
pub(crate) fn bb(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}

/// Create a byte vector from multiple strings
pub(crate) fn bbs(x: impl IntoIterator<Item = impl ToString>) -> Vec<u8> {
    let r = x.into_iter().map(|x| x.to_string().into_bytes());
    vec_chain(r)
}

pub(crate) fn vec_chain<T>(vectors: impl IntoIterator<Item = Vec<T>>) -> Vec<T> {
    let mut r = vec![];
    for v in vectors {
        r.extend(v);
    }
    r
}
