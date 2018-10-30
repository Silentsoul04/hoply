#!/bin/bash
set -xe
mkdir -p $HOME/pypi/$1/
cd $HOME/pypi/$1/
touch start.timestamp
curl --location https://pypi.org/pypi/$1/json > metadata.json
export PIPENV_VENV_IN_PROJECT=1
pipenv run --three pip install $1 || (rm -rf .venv && pipenv run --two pip install $1)
pipenv run pip freeze > requirements.txt
rm -rf .venv
touch end.timestamp
