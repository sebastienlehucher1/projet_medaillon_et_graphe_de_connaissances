seed:
	python3 scripts/generate_sample_data.py --out_dir data/raw --nodes 1000000 --edges 5000000

bronze:
	python3 scripts/to_parquet.py --in_dir data/raw --out_dir data/bronze

silver:
	python3 quality/gx_checkpoint.py --in_dir data/bronze --out_dir data/silver
	python3 scripts/partition_edges.py --in_dir data/silver --partitions 8

import:
	python3 scripts/neo4j_bulk_import.py

up:
	docker compose up -d

down:
	docker compose down -v

run_pipeline:
	seed bronze silver import