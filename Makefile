.PHONY: test train

TODO: Makefile for the zanasonic project
help:
	@echo "init - initialise the project"
	@echo "test - run pytest"
	@echo "train - train the model"

init:
	echo "to be decided"

test:
	@tox -e test_package

train:
	@tox -e train
