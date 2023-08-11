/// Create a uninitialized `Vec<u8>` filled with undefined data.
///
/// Safety: The caller must ensure that the returned `Vec<u8>` is not used before initialized.
#[allow(clippy::uninit_vec)]
pub(crate) fn new_uninitialized(size: usize) -> Vec<u8> {
    let mut b = Vec::with_capacity(size);
    unsafe {
        b.set_len(size);
    }

    b
}
