test:
	clear && cargo test test_kv --release -- --nocapture

bench:
	clear && cargo test throughput_3_ser --release -- --nocapture

maelstrom:
	cargo build --release --features maelstrom --bin maelstrom
	maelstrom test -w lin-kv --bin target/release/maelstrom --latency 0 --rate 10 --time-limit 90 --concurrency 20 --nemesis partition --nemesis-interval 20
