# bitcack
A *cack-handed\** implementation of a Riak/Bitcask style immutable append-only Key-Value database.

Written in Clojure on the JVM.

\**Informal, British.*
inept; clumsy.

## Why does this exist?
To play around with Clojure and learn about Hash Indexing, and maybe LSM trees.

## Key Features
- Keys are stored in memory for fast look-ups.
- Writes are append-only, removing disk seeks.
- Reads happen in constant time.
- Older values are compacted to free up space.

## Feature Roadmap
- ~Read/writes to/from file system.~
- ~Basic hash index.~
- ~Compact & merge segment files.~
- ~Deletions via tombstoning.~
- Hint files (cached versions of the index created during compaction)
- ~Upgrade CSV format to something binary.~
- ~Type hints.~ (Now supports Clojure primitives thanks to Nippy serialisation lib).

## General Todo list
- Spec public functions.
- ~Wrap Java interop calls with Clojure wrapper fns~.
- Add tests.

# Thanks
This is entirely possible thanks to Martin Kleppmann's **[Designing Data-Intensive Applications](https://dataintensive.net/)**, Chapter 3.
