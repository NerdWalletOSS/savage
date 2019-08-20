ENV := $(shell uname)
VENV_DIR=venv
VENV_BIN_DIR=${VENV_DIR}/bin
VENV_PYTHON=$(VENV_BIN_DIR)/python
VENV_RUN=$(VENV_PYTHON) ${VENV_BIN_DIR}/

default: install lint tests

# ---- Install ----

venv:
ifdef CI
	@echo "Current Python version: $(TRAVIS_PYTHON_VERSION)";
	virtualenv --no-site-packages venv
else
	virtualenv --no-site-packages --python=python3.7 venv
endif

install: venv
	@$(VENV_RUN)pip install -r requirements.txt -e .[dev]

deps: install
	@$(VENV_RUN)pip-compile --annotate --no-header --no-index --output-file requirements.txt

clean: clean-pyc
	@rm -fr ${VENV_DIR}

clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

# ---- Tests ----

lint:
	@$(VENV_RUN)pylint --py3k .
	@$(VENV_RUN)flake8
	@$(VENV_RUN)isort --check-only -rc -p savage -p tests .
ifneq ($(TRAVIS_PYTHON_VERSION),2.7)
	@$(VENV_RUN)black --exclude=$(VENV_DIR) --line-length=100 --check .
endif

tests:
	@$(VENV_RUN)pytest --cov=. tests

# --- Formatting ---

format: isort black autopep8

autopep8:
	@$(VENV_RUN)autopep8 --in-place --recursive --exclude=${VENV_DIR} .

black:
	@$(VENV_RUN)black --exclude=$(VENV_DIR) --line-length=100 .

isort:
	@$(VENV_RUN)isort -rc -p savage -p tests .

# --- Tools ---

console:
	@$(VENV_RUN)ipython

pg_shell:
	@docker-compose run --rm postgres /usr/bin/psql -h postgres -U postgres

.PHONY: install clean clean-pyc lint tests autopep8 black isort console pg_shell
