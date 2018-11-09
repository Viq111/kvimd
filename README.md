# kvimd

kvimd - KV IM-mutable D-atabase
A (fast) disk K/V store for immutable data

# Requirements

- [x] Key / Value is immutable (we assume for a key, the value is unique and always the same) (kvimd == reverse lookup database)
- [x] We don't care about disk space, we care about speed
- [x] Random access is as cheap as continuous access (disk == SSD/NVMe)
- [x] Key size is constant

# File structure

For a given root path of `/kvimd_db/`:
- `/kvimd_db/db#.hashdisk` is a disk hashmap mapping key -> (`valuesDisk` file id, offset in file)
- `/kvimd_db/db#.valuesdisk` is the file containing the values. (Seeking with offset, you get back a value)

## `db#.hashdisk`

It is a non-sparse file where all values are encoded as follow:
- On write, we ask the DB to reserve us space of `len(value)` + size of the varint to encode the value
- The data is written as `length_as_varint + data`. We use `uint32` for this file (so file max of 4Gb) so the varint can be up to 5 bytes

## `db#.valuesdisk`

It is a sparse disk file that is mmapped.
A cell is of size `len(key) + 4 + 4` (4 for `uint32` which is the `valuesDisk` file id + 4 for `uint32` which is the offset in that file)
This imposes a limitation on `kvimd` that the database will not hold more than `4Gb*4Gb = 1<<60 = 1<<42 exabytes`

Currently use linear probing, in the future we might want to implement [RobinHood](https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/) hashing to increase the load factor to 0.9 or 0.95

# Improvements:

## HashDisk

- [ ] Use robin hood hashing instead of linear probling
- [ ] Use type casting / whatever instead of bytes.Equal to find zero-value slice (5.81 ns/op vs 2.27 ns/op)

## ValuesDisk

- [ ] Do a dichotomy to know what offset to restart on (or read length). This is bc if we crash loop, we will create A LOT of (large) files
- [ ] Add test for `Load()`
- [ ] Optional value compression

## Main DB

- [ ] Check that if key size is given at DB creation and not const it's fine (benchmark)
- [ ] Add test for `rotate()`
- [ ] There is a log of recent entries (for replay)
- [ ] Possibility to snapshot / lock the database (then everything is appended to log instead)
