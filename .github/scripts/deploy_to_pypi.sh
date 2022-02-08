#!/bin/sh

action=$1
# Upload package to PyPI using github secrets
if [[ $action == "live_deploy" ]]; then
    twine upload -u __token__ -p "$PYPI_KEY" dist/*
else
    twine upload -u __token__ -p "$TEST_PYPI_KEY" --repository testpypi dist/*
fi