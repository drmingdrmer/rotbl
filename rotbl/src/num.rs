/// Format number in Rust style: `1_000_000`.
pub fn format_num(n: u64) -> String {
    use num_format::Buffer;
    use num_format::CustomFormat;
    use num_format::Grouping;

    let format = CustomFormat::builder().grouping(Grouping::Standard).separator("_").build();

    let format = match format {
        Ok(x) => x,
        Err(_e) => return n.to_string(),
    };

    let mut buf = Buffer::new();
    buf.write_formatted(&n, &format);
    buf.to_string()
}

#[cfg(test)]
mod tests {
    use super::format_num;

    #[test]
    fn test_format_num() {
        assert_eq!(format_num(1000000), "1_000_000");
        assert_eq!(format_num(100000), "100_000");
    }
}
