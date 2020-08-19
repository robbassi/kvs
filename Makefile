test:
	# Lint, typecheck, test
	# pipenv run flake8 kvs --count --select=E9,F63,F7,F82 --show-source --statistics
	# mypy kvs
	pipenv run pytest

.PHONY: service
service:
	${OPEN} http://localhost:8000/docs
	uvicorn service.main:app --port ${PORT} --reload
