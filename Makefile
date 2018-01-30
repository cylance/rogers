.PHONY: build test pylint clean env compile

all : env test pylint build

NAME := rogers
PWD := $(shell pwd)
VERSION := $(shell cat VERSION)


env:
	python3 -m venv --copies env
	env/bin/pip install -U pip
	env/bin/pip install -r dev-requirements.txt
	env/bin/pip install -r requirements.txt
	env/bin/pip install -e .
	git submodule update --init --recursive

compile:
	protoc -I=proto/ --python_out=src/rogers/data proto/features.proto

test:
	env/bin/nosetests tests/

pylint:
	env/bin/pylint --reports=y --disable=mixed-indentation,line-too-long,missing-docstring,too-many-public-methods,too-few-public-methods,import-error,no-name-in-module,not-callable,locally-disabled,duplicate-code,file-ignored --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" $(NAME) > pylint.out & exit 0

build:
	env/bin/python setup.py bdist_wheel

install:
	env/bin/python setup.py install

clean-env:
	rm -rf env/

clean:
	rm -rf bin/
	rm -rf dist/
	rm -rf coverage.xml
	rm -rf nosetests.xml
	rm -rf pylint.out
	rm -rf build/
