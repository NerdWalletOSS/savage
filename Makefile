VENV_DIR=venv
VENV_BIN_DIR=${VENV_DIR}/bin
VENV_PYTHON=$(VENV_BIN_DIR)/python
VENV_RUN=$(VENV_PYTHON) ${VENV_BIN_DIR}/
BLACK_ARGS=--exclude=$(VENV_DIR) --line-length=100 .
ISORT_ARGS=-rc -p savage -p tests .

default: install lint tests

# ---- Install ----

venv:
	# Create a local virtual environment in `venv/`
	python -mvenv $(VENV_DIR)

install: venv
	# Install Python dev dependencies into local venv
	@$(VENV_RUN)pip install -r requirements.txt -e .[dev]

deps: install
	# Regenerate `requirements.txt` from `setup.py` using `pip-compile`
	@$(VENV_RUN)pip-compile --annotate --no-header --no-index --output-file requirements.txt

clean: clean-pyc
	@rm -fr ${VENV_DIR}

clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

.PHONY: venv install deps clean clean-pyc

# ---- Tests ----

lint:
	# Check Python 2/3 compatibility
	@$(VENV_RUN)pylint --py3k .
	# Check Python style
	@$(VENV_RUN)flake8
	# Check import sorting
	@$(VENV_RUN)isort --check-only $(ISORT_ARGS)
	# Check Black formatting
ifneq ($(TRAVIS_PYTHON_VERSION),2.7)
	@$(VENV_RUN)black --check $(BLACK_ARGS)
endif

tests:
	# Run pytest with coverage
	@SQLALCHEMY_WARN_20=1 $(VENV_RUN)pytest --cov=. tests

.PHONY: lint tests

# --- Formatting ---

format: isort black autopep8

autopep8:
	@$(VENV_RUN)autopep8 --in-place --recursive --exclude=${VENV_DIR} .

black:
	@$(VENV_RUN)black $(BLACK_ARGS)

isort:
	@$(VENV_RUN)isort $(ISORT_ARGS)

.PHONY: format autopep8 black isort

# --- Tools ---

console:
	@$(VENV_RUN)ipython

pg_shell:
	@docker-compose run --rm postgres /usr/bin/psql -h postgres -U postgres

.PHONY: console pg_shell
