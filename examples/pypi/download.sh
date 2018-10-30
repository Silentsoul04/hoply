#!/bin/bash
set -xe
mkdir -p $HOME/pypi/$1/
cd $HOME/pypi/$1/
touch start.timestamp
curl --location https://pypi.org/pypi/$1/json > metadata.json
export PIP_NO_CACHE_DIR=false
export PIPENV_VENV_IN_PROJECT=1
pipenv install --three $1 || (rm -rf .venv Pipfile* && pipenv install --two $1)
pipenv graph --json-tree > dependencies.json
rm -rf .venv
touch end.timestamp
