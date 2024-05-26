FROM python:3.8

WORKDIR /app

COPY . /app

RUN pip install confluent-kafka

CMD ["python", "./producer.py"]