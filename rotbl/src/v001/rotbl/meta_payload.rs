use std::fmt;

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RotblMetaPayload {
    /// The global seq number of keys
    pub(crate) seq: u64,
    pub(crate) user_data: String,
}

impl RotblMetaPayload {
    pub fn new(seq: u64, user_data: impl ToString) -> Self {
        Self {
            seq,
            user_data: user_data.to_string(),
        }
    }
}

impl fmt::Display for RotblMetaPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{seq: {}, user_data: {}}}", self.seq, self.user_data)
    }
}
