#!/bin/sh

# Upload package to PyPI using github secrets
twine upload -u __token__ -p "$TWINE_PASSWORD" dist/*