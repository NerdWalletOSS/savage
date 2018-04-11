default: clean install lint tests

# ---- Install ----
$(VENV_ACTIVATE):
	@pipenv shell
install: $(VENV_ACTIVATE)
	@pipenv install -e --dev .
clean: clean-pyc
	@pipenv --rm
clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

# ---- Tests ----
lint:
	@pipenv run flake8
tests:
	@pipenv run pytest --cov=.

# --- Formatting ---

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
