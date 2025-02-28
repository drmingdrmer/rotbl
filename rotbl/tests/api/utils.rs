/// Create a string
pub(crate) fn ss(x: impl ToString) -> String {
    x.to_string()
}

/// Create a byte vector
pub(crate) fn bb(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}
