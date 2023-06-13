# tapi-rs

Rust implementation of [TAPIR](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf)

- [ ] IR
  - [x] Unlogged
  - [x] Inconsistent
  - [x] Consensus
  - [x] View changes
  - [ ] Epoch changes
  - [ ] Garbage collection of log
- [ ] TAPIR
  - [x] Optional linearizability
  - [ ] Sync & merge
  - [ ] Sharding
  - [ ] Cooperative termination
  - [ ] Garbage collection of OCC versions
- [ ] Testing
  - [x] IR lock server (very simple)
  - [x] TAPIR-KV (very simple)
  - [ ] TAPIR-KV (randomized with fault-injection)