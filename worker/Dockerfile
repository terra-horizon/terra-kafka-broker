FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python pymongo python-dotenv

COPY worker.py .

CMD ["python", "-u", "worker.py"]
