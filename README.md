# tapi-rs

Rust implementation of [TAPIR](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf)

- [x] Safe, `async` Rust
- [ ] IR
  - [x] Unlogged
  - [x] Inconsistent
  - [x] Consensus
  - [x] View changes
  - [x] Recovery
  - [ ] Epoch changes
  - [ ] Garbage collection of record
  - [ ] Real network transport
- [ ] TAPIR
  - [x] Get
  - [x] Put
  - [x] Prepare
  - [x] Commit
  - [x] Abort
  - [x] Optional linearizability
  - [x] IR sync & merge
  - [ ] Sharding
  - [ ] Cooperative termination
  - [ ] Garbage collection of OCC versions
  - [ ] Persistent storage (e.g. `sled`)
  - [ ] Pessimistic read only transactions
- [ ] Planned extensions
  - [x] Delete key operation
  - [ ] Quorum range scan
  - [ ] Automatic shard balancing
- [ ] Testing
  - [x] IR lock server (very simple)
  - [x] TAPIR-KV (very simple)
  - [ ] TAPIR-KV (randomized with fault-injection)