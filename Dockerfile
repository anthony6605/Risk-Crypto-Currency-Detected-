FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Dagster and dbt for the dagster service (Step 46)
COPY requirements-orchestration.txt .
RUN pip install --no-cache-dir -r requirements-orchestration.txt

COPY . .

# Dagster builds its asset graph from dbt's manifest. dbt parse writes it without
# connecting to the database, so the password only has to be set, not correct.
RUN cd transform && POSTGRESQL_PASSWORD=unused dbt parse --profiles-dir .
