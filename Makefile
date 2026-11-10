.PHONY: setup download profile simulate ingest silver gold evaluate export run test lint bench sensitivity publish api site-serve clean

setup:        ## install pinned dependencies (uv.lock)
	uv sync --frozen
download:     ## fetch both Zenodo datasets (md5-verified, idempotent)
	uv run secondeng download
profile:      ## docs/data_profile.md
	uv run secondeng profile
simulate:     ## replay the fleet as satellite batches into landing/
	uv run secondeng simulate
ingest:       ## landing -> bronze
	uv run secondeng ingest
silver:       ## bronze -> silver (incremental)
	uv run secondeng silver
gold:         ## dbt build: gold marts + tests
	uv run secondeng gold
evaluate:     ## docs/results.md
	uv run secondeng evaluate
export:       ## gold -> site/data
	uv run secondeng export
run:          ## everything: download, simulate, ingest, silver, gold, evaluate, export
	uv run secondeng run
test:
	uv run pytest -q
lint:
	uv run ruff check . && uv run ruff format --check .
bench:        ## docs/benchmarks.md (needs a completed run)
	uv run secondeng bench
sensitivity:  ## docs/sensitivity.md (5 re-runs of the injected vessel)
	uv run secondeng sensitivity
publish:      ## copy the served gold tables to Postgres (set PUBLISH_DATABASE_URL, loader role)
	uv run secondeng publish
api:          ## owner-scoped API on :8000 (set DATABASE_URL to the api_reader role, and API_KEYS)
	uv run secondeng api
site-serve:   ## dashboard on http://localhost:8080
	cd site && python3 -m http.server 8080
clean:        ## remove generated data (keeps data/raw downloads)
	rm -rf landing lake site/data data/bench data/sensitivity data/ground_truth dbt/target
