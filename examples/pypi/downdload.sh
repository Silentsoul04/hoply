#!/bin/bash
set -xe
mkdir -p $HOME/dependencies/
mkdir -p $HOME/packages/$1
cd $HOME/packages/$1
pipenv run pip install $1
pipenv run pip freeze > $HOME/dependencies/$1.txt
rm -rf $HOME/.local/share/virtualenvs/$1-*
rm -rf $HOME/packages
