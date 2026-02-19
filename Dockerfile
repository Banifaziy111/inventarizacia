# Образ для веб-приложения инвентаризации (Flask + Gunicorn)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Зависимости системы для psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

# Порт приложения
EXPOSE 8001

# Gunicorn: 1 воркер для начала (для БД без пула можно увеличить осторожно)
CMD ["gunicorn", "--bind", "0.0.0.0:8001", "--workers", "1", "--threads", "4", "--timeout", "120", "app:app"]
