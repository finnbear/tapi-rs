test:
	clear && cargo test --release -- # --nocapture

coordinator_failure_stress_test_3:
	clear && cargo test --release -- coordinator_recovery_3_loop --nocapture --include-ignored

coordinator_failure_stress_test_7:
	clear && cargo test --release -- coordinator_recovery_7_loop --nocapture --include-ignored

bench:
	clear && cargo test throughput_3_ser --release -- --nocapture

maelstrom:
	cargo build --release --features maelstrom --bin maelstrom
	maelstrom test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20
