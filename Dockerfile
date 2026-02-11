FROM python:3.12-alpine

# Instalamos dependencias del sistema incluyendo build-base (más completo que gcc)
RUN apk add --no-cache \
    gcc \
    musl-dev \
    bash \
    librdkafka-dev \
    build-base

WORKDIR /app

# Actualizamos pip antes de instalar nada
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .