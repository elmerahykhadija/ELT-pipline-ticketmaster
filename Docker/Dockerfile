# Utilisation de l'image officielle Airflow
FROM apache/airflow:2.8.1-python3.11

USER root

# Installation de curl pour le healthcheck et des outils de base
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copie du fichier requirements.txt
COPY requirements.txt /requirements.txt

# Correction : on utilise && et le backslash \ pour bien séparer
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-snowflake