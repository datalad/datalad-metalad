# simple makefile to simplify repetetive build env management tasks under posix
# Ideas borrowed from scikit-learn's and PyMVPA Makefiles  -- thanks!

PYTHON ?= python
HANDBOOK_NAME = archived-study-metadata-handbook

all: clean doc

clean:
	rm -rf build
	-find . -name '*.pyc' -delete
	-find . -name '__pycache__' -type d -delete

doc:
	mkdir -p build/docs
	$(PYTHON) -m markdown docs/$(HANDBOOK_NAME).md >build/docs/$(HANDBOOK_NAME).html
