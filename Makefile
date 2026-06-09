.PHONY: help install run generate test analyze clean

help:
	@echo "make install   - install dependencies"
	@echo "make run       - run the full medallion pipeline end-to-end"
	@echo "make analyze   - run sample business queries on the warehouse"
	@echo "make test      - run the test suite"
	@echo "make clean     - remove generated data & warehouse"

install:
	pip install -r requirements.txt
	pip install -e .

run:
	python -m ecommerce_pipeline.pipeline

generate:
	python -m ecommerce_pipeline.ingestion.generate_data

analyze:
	python -m ecommerce_pipeline.analyze

test:
	pytest -q

clean:
	rm -rf data/lakehouse data/landing/*.csv data/landing/*.json
	rm -rf spark-warehouse metastore_db derby.log
