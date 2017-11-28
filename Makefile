TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv doc docs html api coala pylint pydocstyle pytest

install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
	cat ${TEMPFILE} | xargs rm -rf && \
	rm -f ${TEMPFILE}

devenv:
	@echo "Installing the latest development requirements"
	pip3 install -U -r dev_requirements.txt
	@echo "Installing the latest Selinon sources from GitHub repo"
	pip3 install -U --force-reinstall git+https://github.com/selinon/selinon@master

pytest:
	@# set timeout so we do not wait in infinite loops and such
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	@echo ">>> Executing testsuite"
	PYTHONPATH="test/:${PYTHONPATH}" python3 -m pytest -s -vvl --timeout=2 -p no:celery test/

pylint:
	@echo ">>> Running PyLint"
	pylint selinonlib

coala:
	@# We need to run coala in a virtual env due to dependency issues
	echo ">>> Preparing virtual environment for coala" &&\
	@# setuptools is pinned due to dependency conflict &&\
	[ -d venv-coala ] || virtualenv -p python3 venv-coala && . venv-coala/bin/activate && pip3 install coala-bears "setuptools>=17.0" &&\
	echo ">>> Running coala" &&\
	venv-coala/bin/python3 venv-coala/bin/coala --non-interactive

pydocstyle:
	@echo ">>> Running pydocstyle"
	find selinonlib/ -name '*.py' -and ! -name 'test_*.py' -and ! -name 'codename.py' -and ! -name 'version.py' ! -path 'selinonlib/predicates/*' -print0 | xargs -0 pydocstyle {} \; 

check: pytest pylint pydocstyle coala

venv:
	virtualenv -p python3 venv && source venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' -print0 | xargs -0 rm -rf
	rm -rf venv docs/ build dist selinonlib.egg-info

api:
	@sphinx-apidoc -e -f -o docs.source/selinonlib/doc selinonlib -f

doc:
	@make -f Makefile.docs html
	@echo "Documentation available at 'docs/index.html'"

docs: doc
html: doc
test: check
