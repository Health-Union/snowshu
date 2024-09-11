# Makefile
.PHONY: setup
setup:
	@echo "Setting up the project..."
	@pip install uv
	@uv pip install -r requirements/dev.txt
	@pre-commit install
	@echo "Pre-commit hooks installed."