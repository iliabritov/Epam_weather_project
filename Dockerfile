FROM python:3.6.9

COPY ./app ./app
COPY ./requirements.txt ./requirements.txt

RUN mkdir data
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /app

EXPOSE 5000

ENTRYPOINT ["python3"]

CMD ["app.py"]

