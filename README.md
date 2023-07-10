# tapi-rs

Rust implementation of [TAPIR](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf)

- [x] Safe, `async` Rust
- [ ] IR
  - [x] Unlogged
  - [x] Inconsistent
  - [x] Consensus
  - [x] View changes
  - [x] Recovery
  - [x] Membership change
  - [ ] Real network transport
- [ ] TAPIR
  - [x] Get
  - [x] Put
  - [x] Prepare
  - [x] Commit
  - [x] Abort
  - [x] Optional linearizability
  - [x] IR sync & merge
  - [x] Prepare retries
  - [x] Coordinator recovery
  - [ ] Sharding
  - [ ] Persistent storage (e.g. `sled`)
  - [ ] Pessimistic read only transactions
- [ ] Planned extensions
  - [x] Delete key operation
  - [ ] Garbage collection
  - [ ] Quorum range scan
  - [ ] Automatic shard balancing
- [ ] Testing
  - [x] IR lock server (very simple)
  - [x] TAPIR-KV (simple)
  - [x] TAPIR-KV (maelstrom)
  - [ ] Exhaustive state enumeration
- [ ] Optimizations
  - [ ] Reduce `clone`
  - [ ] Reduce allocations
  - [ ] Reduce temporary unavailability

  ## Acknowledgement

  Thank you to the TAPIR authors for answering my questions about the paper!