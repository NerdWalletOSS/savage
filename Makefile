ENV := $(shell uname)
PIPENV := $(shell command -v pipenv 2> /dev/null)

default: install lint tests

# ---- Install ----

pipenv:
ifndef PIPENV
ifeq ($(ENV),Darwin)
	@brew install pipenv
else
	@pip install pipenv
endif
endif

install: pipenv
ifdef CI
	echo $TRAVIS_PYTHON_VERSION
	@pipenv install --dev
else
	@pipenv install --three --dev -e .
endif

clean: clean-pyc
	@-pipenv --rm

clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

# ---- Tests ----

lint:
	@pipenv run pylint --py3k .
	@pipenv run flake8
	@pipenv run isort --check-only -rc -p savage -p tests .
ifneq ($(TRAVIS_PYTHON_VERSION),2.7)
	@pipenv run black --line-length=100 --check .
endif

tests:
	@pipenv run pytest --cov=. tests

# --- Formatting ---

format: isort black autopep8

autopep8:
	@pipenv run autopep8 --in-place --recursive .

black:
    @pipenv run black --line-length=100 .

isort:
	@pipenv run isort -rc -p savage -p tests .

# --- Tools ---

console:
	@pipenv run ipython

pg_shell:
	@docker-compose run --rm postgres /usr/bin/psql -h postgres -U postgres

.PHONY: install clean clean-pyc lint tests autopep8 black isort console pg_shell
