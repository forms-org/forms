init:
	pip install -r requirements.txt
test:
	python -m pytest tests/df_tests
	python -m pytest tests/db_tests
reformat:
	black .
.PHONY: init test reformat
