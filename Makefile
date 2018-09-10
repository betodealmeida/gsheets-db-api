init: requirements
	pip install -r requirements.txt

test:
	nosetests tests

requirements:
	pipreqs --force druiddb --savepath requirements.txt

.PHONY: init test requirements
