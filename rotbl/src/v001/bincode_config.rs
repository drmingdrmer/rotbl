pub fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard().with_big_endian().with_variable_int_encoding()
}
