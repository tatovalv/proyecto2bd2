FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas.py loader.py ./

ENV PARQUET_PATH=/data/parquet
ENV RAW_RETENTION_HOURS=1

CMD ["python", "loader.py"]
