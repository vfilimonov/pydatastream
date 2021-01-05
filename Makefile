
.PHONY: help release build clean

LIBRARY := pydatastream

# taken from: https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.DEFAULT_GOAL := help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


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
