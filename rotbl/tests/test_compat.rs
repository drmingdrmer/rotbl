use std::fs;
use std::sync::Arc;

use rotbl::v001::Builder;
use rotbl::v001::Config;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;

const COMPAT_DIR: &str = "tests/compat";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[test]
#[ignore]
fn generate_data() -> anyhow::Result<()> {
    let mut config = Config::default();
    config.block_config.max_items = Some(20);

    let version_dir = get_version_dir(CURRENT_VERSION);
    let db_dir = format!("{}/db", version_dir);
    let rotbl_path = format!("{}/db/x.rot", version_dir);
    let dump_path = format!("{}/dump.txt", version_dir);

    fs::create_dir_all(&db_dir)?;

    let n_keys = 512;
    let mut key = ss("aaa");
    let rotbl_meta = RotblMeta::new(5, "hello");

    let mut builder = Builder::new(config, rotbl_path)?;
    for i in 0..n_keys {
        let v = if i % 2 == 0 {
            SeqMarked::new_tombstone(i)
        } else {
            let value = key.clone().into_bytes();
            SeqMarked::new_normal(i, value)
        };
        builder.append_kv(&key, v)?;

        key = next(&key);
    }

    let t = builder.commit(rotbl_meta)?;
    let t = Arc::new(t);

    let dump = t.dump().collect::<Result<Vec<_>, _>>()?;
    fs::write(dump_path, dump.join("\n"))?;

    Ok(())
}

#[test]
fn test_compat() -> anyhow::Result<()> {
    let version_dir = get_version_dir(CURRENT_VERSION);
    let rotbl_path = format!("{}/db/x.rot", version_dir);
    let dump_path = format!("{}/dump.txt", version_dir);

    let config = Config::default();

    let t = Arc::new(Rotbl::open(config, rotbl_path)?);
    let data = t.dump().collect::<Result<Vec<_>, _>>()?;
    let data = data.join("\n");

    // compare with the dump file
    let dump = fs::read_to_string(dump_path)?;
    assert_eq!(data, dump);

    Ok(())
}

fn get_version_dir(version: &str) -> String {
    format!("{}/{}", COMPAT_DIR, version)
}

fn ss(x: impl ToString) -> String {
    x.to_string()
}

/// Next permutation of the string of the same length.
///
/// Replace the last character with next character in the alphabet.
/// If the last character is 'z', replace it with 'a' and replace the second last character with
/// next character in the alphabet.
pub fn next(k: &str) -> String {
    let mut chars: Vec<char> = k.chars().collect();

    // Iterate from the end towards the beginning
    for i in (0..chars.len()).rev() {
        if chars[i] == 'z' {
            chars[i] = 'a';
            if i == 0 {
                unreachable!("exhausted");
            }
        } else {
            chars[i] = (chars[i] as u8 + 1) as char;
            break;
        }
    }

    // Collect the characters back into a String
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::next;

    #[test]
    fn test_simple_increment() {
        assert_eq!(next("aaaaa"), "aaaab");
        assert_eq!(next("aaaay"), "aaaaz");
    }

    #[test]
    fn test_wrap_around() {
        assert_eq!(next("aaaaz"), "aaaba");
    }

    #[test]
    fn test_multiple_wrap_around() {
        assert_eq!(next("aaazz"), "aabaa");
        assert_eq!(next("aazzz"), "abaaa");
    }

    #[test]
    fn test_no_initial_z() {
        assert_eq!(next("abcde"), "abcdf");
        assert_eq!(next("asdfg"), "asdfh");
    }

    #[test]
    fn test_3_chars() {
        assert_eq!(next("abc"), "abd");
    }
}
