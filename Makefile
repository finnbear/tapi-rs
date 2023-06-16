test:
	clear && cargo test test_kv --release -- --nocapture

bench:
	clear && cargo test throughput_3_ser --release -- --nocapture

maelstrom:
	cargo build --release --features maelstrom --bin maelstrom
	maelstrom test -w lin-kv --bin target/release/maelstrom --latency 10 --rate 100 --time-limit 15 --concurrency 10
