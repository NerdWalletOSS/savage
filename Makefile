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
	@pipenv install --dev
else
	@pipenv install -e --dev .
endif

clean: clean-pyc
	@-pipenv --rm

clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

# ---- Tests ----
lint:
	@pipenv run flake8

tests:
	@pipenv run pytest --cov=. tests

# --- Formatting ---

format: isort autopep8

autopep8:
	@pipenv run autopep8 --in-place --recursive .

isort:
	@pipenv run isort -rc -p savage -p tests .

# --- Tools ---

console:
	@pipenv run ipython

pg_shell:
	@docker-compose run --rm postgres /usr/bin/psql -h postgres -U postgres

.PHONY: install clean clean-pyc lint tests autopep8 isort console pg_shell
