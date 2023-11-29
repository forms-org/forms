init:
	pip install -r requirements.txt
test:
	black --check .
	python -m pytest tests/df_tests
reformat:
	black .
.PHONY: init test reformat
