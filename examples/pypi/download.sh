#!/bin/bash
set -xe
rm -rf $HOME/pypi/$1/
mkdir -p $HOME/pypi/$1/
cd $HOME/pypi/$1/
touch start.timestamp
curl --location https://pypi.org/pypi/$1/json > metadata.json
export PIP_NO_CACHE_DIR=false
export PIPENV_VENV_IN_PROJECT=1
pipenv install --pre --three $1 || (rm -rf .venv Pipfile* && pipenv install --pre --two $1)
pipenv graph --json-tree > dependencies.json
rm -rf .venv
touch end.timestamp
