help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

pyenv:
	git clone https://github.com/pyenv/pyenv.git

dev: pyenv ## Prepare the ubuntu host sytem for development
	pip3 install pipenv --user --upgrade
	PYENV_ROOT=$(PWD)/pyenv PATH=$(PWD)/pyenv/bin:$(HOME)/.local/bin:$(PATH) pipenv install --dev --skip-lock
	pipenv run python setup.py develop
	pipenv run pre-commit install --hook-type pre-push

check: ## Run tests
	pipenv run py.test -vvv --cov-config .coveragerc --cov-report html --cov-report xml --cov=hoply tests.py
	pipenv check
	pipenv run bandit --skip=B101 hoply/
	@echo "\033[95m\n\nYou may now run 'make lint'.\n\033[0m"

lint: ## Lint the code
	pipenv run pylama hoply/

clean: ## Clean up
	git clean -fXd

todo: ## Things that should be done
	@grep -nR --color=always TODO hoply.py

xxx: ## Things that require attention
	@grep -nR --color=always --before-context=2  --after-context=2 XXX hoply.py

publish: check ## Publish to pypi.org
	pipenv run python setup.py sdist upload
