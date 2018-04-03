# NOTE: This assumes a single virtualenv setup under the `venv` directory, and bash availability
#
SHELL:=/bin/bash
VENV_DIR=venv
VENV_ACTIVATE=$(VENV_DIR)/bin/activate

default: clean install

# ---- Install ----
$(VENV_ACTIVATE):
	@virtualenv venv
install: $(VENV_ACTIVATE)
	@test -d venv || virtualenv venv
	@. $(VENV_ACTIVATE); pip install -r requirements.txt
clean: clean-pyc
	@rm -rf venv
clean-pyc:
	@ find ./savage -name "*.pyc" -exec rm -rf {} \;

# --- Formatting ---

isort:
	@. $(VENV_ACTIVATE); isort -rc -p savage -p tests .

# --- Tools ---

console:
	@. $(VENV_ACTIVATE); ipython
pg_shell:
	@docker-compose run --rm postgres /usr/bin/psql -h postgres -U postgres

.PHONY: install clean clean-pyc isort console pg_shell
