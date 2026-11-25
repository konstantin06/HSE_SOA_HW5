FROM python:3.12.10-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /cinema
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
COPY schemas schemas
COPY sql sql
COPY tests tests
