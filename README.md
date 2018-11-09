# kvimd

A (fast) disk K/V store for immutable data

# Constraints

- [x] Key / Value is immutable (we assume for a key, the value is unique and always the same)
- [x] We don't care about disk space, we care about speed
- [x] Random access is as cheap as continuous access (disk == SSD/NVMe)
- [x] Key size is constant

Possible future additions:
- [ ] There is a log of recent entries (for replay)


# File structure

For a given root path of `/kvimd_db/`:
- `/kvimd_db/metadata` is a very small file that stores metadata about the database. Just a JSON
- `/kvimd_db/hash.x` (where x is a number starting at 0) is the hashmap of key -> (values_id, offset)
- `/kvimd_db/values.x` is the file containing the values. There are # of workers created per `hash.x` file

## `hash.x`

- [x] Currently use linear probing
- [ ] (Future) Use [RobinHood](https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/) hashing



# Improvements:

## HashDisk

- [ ] Use robin hood hashing instead of linear probling
- [ ] Use type casting / whatever instead of bytes.Equal to find zero-value slice (5.81 ns/op vs 2.27 ns/op)

## ValuesDisk

- [ ] Do a dicotomy to know what offset to restart on (or read length). This is bc if we crash loop, we will create A LOT of (large) files

## Main DB

- [ ] Once we start being a bit full on any DB (close to max size), start creating a new file in the background then swap it
