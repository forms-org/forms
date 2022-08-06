init:
	pip install -r requirements.txt
test:
	black --check .
.PHONY: init test
