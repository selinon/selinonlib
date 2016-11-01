TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv doc docs html

install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
	cat ${TEMPFILE} | xargs rm -rf && \
	rm -f ${TEMPFILE}

check:
	@# set timeout so we do not wait in infinite loops and such
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	py.test -vvl --timeout=2 test -p no:celery

venv:
	virtualenv -p python3 venv && source venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' -print0 | xargs -0 rm -rf
	rm -rf venv docs.source/api docs/build/

doc:
	@sphinx-apidoc -e -o docs.source/api selinonlib -f
	@make -f Makefile.docs docs html
	@echo "Documentation available at 'docs/index.html'"

docs: doc
html: doc

