
.PHONY: help release build clean

LIBRARY := pydatastream

# taken from: https://gist.github.com/prwhite/8168133
help:		## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'


build:		## Make distribution package
	python setup.py sdist


release:	## Make package; upload to pypi and clean after`
	@echo Do you really want to release the package to PyPi?
	@echo "Type library name for release ($(LIBRARY)):"
	@read -p"> " line; if [[ $$line != "$(LIBRARY)" ]]; then echo "Aborting. We'll release that later"; exit 1 ; fi
	@echo "Starting the release..."
	python setup.py sdist
	twine upload dist/*
	rm MANIFEST
	rm -rf dist


clean:		## Erase distribution package
	rm MANIFEST
	rm -rf dist
