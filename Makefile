test:
	clear && cargo test test_kv --release -- --nocapture

bench:
	clear && cargo test throughput_3_ser --release -- --nocapture
