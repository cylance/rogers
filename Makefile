NAME := rogers
VERSION := $(shell cat VERSION)
SOURCES := $(shell find src -name '*.py')
TARBALL := dist/$(NAME)-$(VERSION).tar.gz

all: compile test clean-build build

compile:
	protoc -I=proto/ --python_out=src/rogers/generated proto/*.proto

test: env
	env/bin/pytest tests

# re-run tests when changes are made
watch: env
	source env/bin/activate && env/bin/ptw

$(TARBALL): $(SOURCES) env
	env/bin/python setup.py bdist_wheel
	@ls dist/* | tail -n 1 | xargs echo "Created source tarball"

build: $(TARBALL)

install: env
	env/bin/python setup.py install

env: requirements.txt dev-requirements.txt | env/tools-installed.flag
	env/bin/pip install -r dev-requirements.txt
	env/bin/pip install -r requirements.txt
	env/bin/pip install -e .
	@touch env

force-env: clean-env env

clean: clean-tarball
	rm -rf bin/
	rm -rf dist/
	rm -rf coverage.xml
	rm -rf nosetests.xml
	rm -rf build/

clean-env:
	rm -rf env/

clean-build:
	rm -rf dist/

clean-tarball:
	rm -f $(TARBALL)

# set aliases
dev: env

env/tools-installed.flag:
	python3 -m venv --copies env
	env/bin/pip install -U pip setuptools
	git submodule update --init --recursive
	@touch env/tools-installed.flag

.PHONY: test test-integration watch clean clean-build clean-env clean-tarball force-env
