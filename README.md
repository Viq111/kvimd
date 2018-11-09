# kvimd

A (fast) disk K/V store for immutable data

# Constraints

- [x] Key / Value is immutable (we assume for a key, the value is unique and always the same)
- [x] We don't care about disk space, we care about speed
- [x] Random access is as cheap as continuous access (disk == SSD/NVMe)
- [x] Key size is constant

# File structure

For a given root path of `/kvimd_db/`:
- `/kvimd_db/db#.hashdisk` is the hashmap of key -> (valuesDisk file id, offset in file)
- `/kvimd_db/db#.valuesdisk` is the file containing the values.

## `hash.x`

- [x] Currently use linear probing
- [ ] (Future) Use [RobinHood](https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/) hashing



# Improvements:

## HashDisk

- [ ] Use robin hood hashing instead of linear probling
- [ ] Use type casting / whatever instead of bytes.Equal to find zero-value slice (5.81 ns/op vs 2.27 ns/op)

## ValuesDisk

- [ ] Do a dicotomy to know what offset to restart on (or read length). This is bc if we crash loop, we will create A LOT of (large) files
- [ ] Add test for `Load()`

## Main DB

- [ ] Check that if key size is given at DB creation and not const it's fine
- [ ] Add test for `rotate()`
- [ ] There is a log of recent entries (for replay)
- [ ] Possibility to snapshot / lock the database (then everything is appended to log instead)
