init:
	pip install -r requirements.txt
test:
	black --check .
reformat:
	black .
.PHONY: init test reformat
