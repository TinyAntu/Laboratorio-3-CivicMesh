.PHONY: test build up down clean slurm-submit

test:
	python -m pytest -v tests/

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	rm -rf __pycache__ .pytest_cache tests/__pycache__ network/__pycache__ *.pyc

slurm-submit:
	sbatch scripts/slurm/run_civicmesh.sbatch