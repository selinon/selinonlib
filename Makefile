TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv

install:
	./bump-version.sh
	python setup.py install

uninstall:
	python setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

venv:
	virtualenv -p python3 venv && source venv/bin/activate && pip install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' -delete
	rm -rf venv
