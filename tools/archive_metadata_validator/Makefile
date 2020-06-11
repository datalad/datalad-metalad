# simple makefile to simplify repetetive build env management tasks under posix
# Ideas borrowed from scikit-learn's and PyMVPA Makefiles  -- thanks!

PYTHON ?= python
HANDBOOK_NAME = archived-study-metadata-handbook

clean:
	rm -rf build
	-find . -name '*.pyc' -delete
	-find . -name '__pycache__' -type d -delete
