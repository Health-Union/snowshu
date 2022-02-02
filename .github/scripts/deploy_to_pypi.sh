#!/bin/sh

# Upload package to PyPI using github secrets
twine upload -u "$TWINE_USERNAME" -p "$TWINE_PASSWORD" dist/*