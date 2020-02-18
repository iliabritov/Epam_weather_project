FROM python:3.6.9

COPY ./poetry.lock ./poetry.lock
COPY ./pyproject.toml ./pyproject.toml

RUN mkdir data

RUN pip install --upgrade pip && \
	pip install poetry && \
	poetry export -f requirements.txt --without-hashes -o requirements.txt && \
	pip install -r requirements.txt && \
	pip uninstall -y poetry

COPY ./ui ./ui
WORKDIR /ui

EXPOSE 80 5000

CMD ["flask", "run", "--host=0.0.0.0"]

