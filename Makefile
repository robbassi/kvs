test:
	# Lint, typecheck, test
	# pipenv run flake8 kvs --count --select=E9,F63,F7,F82 --show-source --statistics
	# mypy kvs
	pipenv run pytest