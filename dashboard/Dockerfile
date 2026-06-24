FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV MONGO_URI=mongodb://admin:gdelt2026@mongodb:27017/gdelt_analytics?authSource=admin

EXPOSE 5000
CMD ["python", "app.py"]
