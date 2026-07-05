# Dockerfile
# Custom Airflow image for the Banking Pipeline project.
# Extends the official Airflow image with:
#   - OpenJDK (required by PySpark/Delta Lake for bronze + silver)
#   - Project Python dependencies (PySpark, delta-spark, dbt-duckdb, ...)
FROM apache/airflow:2.8.1-python3.10

# ── System dependencies (as root) ──────────────────────────────────────────
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Python dependencies (as airflow user) ──────────────────────────────────
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

# Airflow + its providers are already pinned and installed by the base
# image — reinstalling apache-airflow from requirements.txt here would
# fight that pin instead of complementing it. Strip those two lines for
# the in-container install; requirements.txt itself stays unchanged so
# it still works as-is for a plain (non-Docker) Windows/venv install.
RUN grep -v -E "^apache-airflow(-providers)?" /opt/airflow/requirements.txt \
        > /opt/airflow/requirements.docker.txt \
    && pip install --no-cache-dir \
        -r /opt/airflow/requirements.docker.txt --no-deps \
    && pip install py4j==0.10.9.7

WORKDIR /opt/airflow
