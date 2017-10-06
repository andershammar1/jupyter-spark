.PHONY: install build uninstall enable clean dev

install: build
	jupyter serverextension enable --py jupyter_spark
	jupyter nbextension install --py jupyter_spark
	jupyter nbextension enable --py jupyter_spark

uninstall:
	jupyter serverextension disable --py jupyter_spark
	jupyter nbextension disable --py jupyter_spark
	jupyter nbextension uninstall --py jupyter_spark
	pip uninstall -y jupyter-spark

clean:
	rm -rf dist/

build: clean
	python setup.py sdist
	pip install dist/*.tar.gz

dev: build install

notebook:
	jupyter notebook
