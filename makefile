# Makefile
.PHONY: setup
setup:
	@echo "Setting up the project..."
	@pip install uv
	@uv pip install -r requirements/dev.txt
	@pre-commit install
	@pre-commit install --hook-type pre-push
	@echo "Pre-commit hooks installed."