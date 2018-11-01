#!/bin/bash
set -xe
rm -rf $1/$2/
mkdir -p $1/$2/
cd $1/$2/
touch start.timestamp
curl --location https://pypi.org/pypi/$2/json > metadata.json
export PIP_NO_CACHE_DIR=false
export PIPENV_VENV_IN_PROJECT=1
pipenv install --pre --three $2 || (rm -rf .venv Pipfile* && pipenv install --pre --two $2)
pipenv graph --json-tree > dependencies.json
rm -rf .venv
touch end.timestamp
