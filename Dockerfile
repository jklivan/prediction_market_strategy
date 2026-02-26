FROM python:3.12-slim

WORKDIR /app

COPY strategy.py .

CMD ["python3", "strategy.py"]
