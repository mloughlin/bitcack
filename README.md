# bitcack
A *cack-handed\** implementation of a Riak/Bitcask style immutable append-only Key-Value database.

Written in Clojure on the JVM.

\**Informal, British.*
inept; clumsy.

## Key Features
- Keys are stored in memory for fast look-ups.
- Writes are append-only, removing disk seeks.
- Reads happen in constant time.
- Older values are compacted to free up space.

# Thanks
This is entirely possible thanks to Martin Kleppmann's **[Designing Data-Intensive Applications](https://dataintensive.net/)**, Chapter 3.
