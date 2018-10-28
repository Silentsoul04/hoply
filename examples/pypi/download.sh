#!/bin/bash
set -xe
mkdir -p $HOME/pypi/
mkdir -p $HOME/packages/$1
cd $HOME/packages/$1
pipenv run --three pip install $1 || (rm -rf $HOME/.local/share/virtualenvs/$1-* && pipenv run --two pip install $1)
pipenv run pip freeze > $HOME/pypi/$1.txt
curl https://pypi.org/pypi/$1/json > $HOME/pypi/$1.json
rm -rf $HOME/.local/share/virtualenvs/$1-*
rm -rf $HOME/packages
