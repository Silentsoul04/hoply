.PHONY: help doc

all: help
	@echo "\nTry something...\n"

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

wiredtiger:
	wget https://github.com/wiredtiger/wiredtiger/releases/download/3.0.0/wiredtiger-3.0.0.tar.bz2
	tar xf wiredtiger-3.0.0.tar.bz2
	cd wiredtiger-3.0.0 && ./configure && make && sudo make install
	touch wiredtiger

pyenv:
	git clone https://github.com/pyenv/pyenv.git

install: wiredtiger pyenv ## Prepare the ubuntu host sytem for development
	pip3 install pipenv --upgrade
	PYENV_ROOT=$(PWD)/pyenv PATH=$(PWD)/pyenv/bin:$(HOME)/.local/bin:$(PATH) pipenv install --dev --skip-lock
	pipenv run python setup.py develop

check: ## Run tests
	pipenv run py.test -vv --capture=no tests.py
	pipenv check
	pipenv run bandit --skip=B101 hoply
	@echo "\033[95m\n\nYou may now run 'make lint' or 'make check-with-coverage'.\n\033[0m"

check-with-coverage: ## Code coverage
	pipenv run py.test -vv --cov-config .coveragerc --cov-report term --cov-report html --cov-report xml --cov=hoply tests.py

lint: ## Lint the code
	pipenv run pylint hoply.py

clean: ## Clean up
	git clean -fXd

todo: ## Things that should be done
	@grep -nR --color=always TODO hoply.py

xxx: ## Things that require attention
	@grep -nR --color=always --before-context=2  --after-context=2 XXX hoply.py

publish: check ## Publish to pypi.org
	pipenv run python setup.py sdist upload
