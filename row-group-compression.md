# Row-group compression design

Rotbl V002 compressed an entire block as one zstd frame. That reduced file size, but a point lookup had to read, decompress, and deserialize every row in the block before it could inspect one key. The block cache then retained the fully decoded `BTreeMap`.

V003 keeps compression while making point reads proportional to a row group instead of a whole block.

## Layout

Each V003 block has this payload layout:

```text
[directory size: u64]
[directory]
    common key prefix
    Vec<RowGroupMeta> {
        first_suffix
        last_suffix
        offset
        size
        item_count
    }
[directory checksum]
[compressed row group 0][group checksum]
[compressed row group 1][group checksum]
...
```

The usual block header, `BlockEncodingMeta`, and outer block checksum remain around this payload. A row group is a sorted `Vec<(suffix, SeqMarked)>` encoded with bincode and compressed as an independent zstd frame.

`RowGroupMeta` is deliberately a named struct rather than a tuple: its key boundaries locate a group, and its offset and size locate the compressed bytes without decoding another group.

## Write path

The builder still splits tables into blocks with `BlockConfig.max_items`. It then splits each block into independently compressed groups with `BlockConfig.row_group_max_items`; the default is 64 rows.

```rust
let block_config = BlockConfig::default()
    .with_max_items(8 * 1024)
    .with_row_group_max_items(64);
```

Smaller groups reduce worst-case point-read decompression. Larger groups preserve more cross-row compression and improve range-scan throughput. The current limit is row-count based, so one very large record can still make a group large.

## Point reads

For `Rotbl::get(key)`, Rotbl:

1. Uses the table block index to find the containing block.
2. Reads and validates the V003 fixed prefix and directory.
3. Binary-searches the directory by suffix-key range.
4. Reads the selected compressed group only.
5. Validates and decompresses that group, then binary-searches its rows.

The cold path normally needs a directory read followed by one group read. It avoids reading, decompressing, and allocating the rest of the block. A cached directory skips the first read; a cached group skips disk I/O but is still decompressed only when its rows are accessed.

## Cache model

One capacity-bounded Moka cache stores two V003 entry types:

- A decoded `RowGroupDirectory`, containing only the prefix and group metadata.
- The compressed bytes of each accessed row group.

The entries share the configured `BlockCacheConfig.capacity`, so directory metadata and compressed data cannot independently exceed the cache budget. Decoded row groups are intentionally not retained; that is what prevents the cache from expanding back to full decoded-block size.

`load_block()` remains the explicit whole-block API: it loads and decompresses every group to build an in-memory `Block`. `get_block()` returns a V003 block only after every group is already cached.

## Integrity

The existing outer checksum verifies a complete block decode. V003 adds checksums to the directory and every row group, allowing partial reads to validate exactly the bytes they consume. A corrupt unaccessed group is detected when that group is read rather than when the directory is loaded.

## Range reads

The directory identifies only groups whose key intervals overlap the requested range. Rotbl decompresses those groups in order and filters rows using the existing prefix/range logic. A full scan therefore decompresses every group, while a narrow scan avoids unrelated groups.

## Compatibility

New blocks are written as V003. V001 and V002 blocks remain readable and are decoded through the legacy whole-block path. Older Rotbl releases do not understand V003 blocks.

## Trade-offs

Row groups reduce point-read memory and decompression work, but they have costs:

- Independent zstd frames reduce compression ratio compared with one block-wide frame.
- A cold point read performs directory and group reads rather than one contiguous block read.
- Repeated access to a cached group still decompresses it each time, by design.

The row-group size is therefore the main tuning knob: choose it from observed point-read latency, range-scan volume, data entropy, and cache pressure.
