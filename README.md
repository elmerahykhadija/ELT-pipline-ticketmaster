# 🎟️ End-to-End ELT Pipeline - Ticketmaster

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red.svg)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Message%20Broker-FF6600.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue.svg)
![dbt](https://img.shields.io/badge/dbt-Transformation-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED.svg)
![AWS](https://img.shields.io/badge/AWS-EC2-orange.svg)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-blue.svg)
![ELT Pipeline](https://img.shields.io/badge/Pipeline-ELT-green.svg)
---
## 📌 Project Overview

Ce projet consiste à construire un **pipeline ELT end-to-end scalable** qui collecte et traite les données d’événements mondiaux provenant de la **Ticketmaster Discovery API** pour les années 2026–2027.

Il met en œuvre une architecture moderne de Data Engineering combinant :
- Orchestration avec **Apache Airflow**
- Ingestion asynchrone via **RabbitMQ**
- Stockage et analyse dans **Snowflake**
- Transformations et modélisation avec **dbt**
## 🏗️ Technical Architecture

![Architecture](img/architecture.png)

Le flux de données suit ces étapes principales :

- **Extraction** : Scripts Python (Producers) récupèrent les données JSON de l’API Ticketmaster.
- **Transit** : Les données passent par une file d’attente RabbitMQ (`events_queue`) pour découpler l’extraction de l’ingestion.
- **Loading** : Le Consumer lit les messages et insère les données brutes dans la table `EVENTS_RAW` de Snowflake.
- **Transformation** : dbt nettoie, transforme et modélise les données en **Star Schema**.

## 🛠️ Technology Stack

- **Langage** : Python 3.12
- **Orchestrateur** : Apache Airflow
- **Message Broker** : RabbitMQ
- **Data Warehouse** : Snowflake
- **Transformation** : dbt (Data Build Tool)
- **Containerisation** : Docker & Docker Compose
- **Cloud / Déploiement** : AWS EC2
- **CI/CD** : GitHub Actions
- **Source** : Ticketmaster Discovery API

## 📊 Data Explanation

Le pipeline collecte les données relatives aux événements Ticketmaster. Dans cette version, seuls les champs suivants sont extraits :

### 🎯 Champs collectés
- **Event ID** et **Event Name**
- **City** et **Venue**
- **Event Segment** (catégorie de l’événement)

### ⚠️ Problèmes de qualité des données
- Doublons
- Valeurs manquantes (particulièrement sur le champ `segment`)

### 🧱 Traitement des données
- **Bronze** : Données brutes aplaties dans la table `EVENTS_RAW`
- **Silver** : Nettoyage, typage et gestion des valeurs manquantes
- **Gold** : Création de dimensions et de la table de faits pour l’analyse
## 🚀 Installation et Utilisation (Local)

### 1. Prérequis
- Docker et Docker Compose installés
- Un compte Snowflake actif
- Une clé API Ticketmaster

### 2. Configuration
Crée un fichier `.env` à la racine du projet avec tes credentials :

```env
API_KEY=your_api_key
API_SECRET=your_api_secret

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASS=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_SCHEMA=your_schema
```
### 3. Lancer les services
```bash
docker compose -f Docker/docker-compose.yml up --build
```
### 4. Transformation avec dbt 
```bash
docker compose exec dbt bash
cd project_ticketmaster
dbt run
```

---

### **Section 7 : Airflow Orchestration**

```markdown
## 🌬️ Airflow Orchestration

La pipeline est orchestrée via le DAG **`elt_dag`** situé dans `dags/elt_dag.py`.

**Ordre d’exécution :**

```text
[run_producer2026, run_producer2027] >> run_consumer >> run_dbt
```
**Accès à l’interface Airflow :**
URL : http://localhost:8080
Utilisateur : admin
Mot de passe : admin

---


### Section 8 : Déploiement sur AWS EC2
## ☁️ Déploiement sur AWS EC2

Le pipeline est déployé sur une **instance EC2** d’AWS pour une exécution en environnement cloud.

### Étapes principales :
1. Créer et lancer une instance EC2 (type recommandé : t3.medium ou t3.large)
2. Installer Docker et Docker Compose
3. Cloner ce repository
4. Copier le fichier `.env` avec tes credentials
5. Lancer les containers en arrière-plan :
   ```bash
   docker compose -f Docker/docker-compose.yml up -d
   ```
6.Configurer le Security Group pour ouvrir les ports nécessaires (ex. : 8080 pour Airflow, 15672 pour l’UI RabbitMQ).

---
### **Section 9 : CI/CD Pipeline**

Le projet dispose d’un pipeline **CI/CD** automatisé avec GitHub Actions :
- Linting du code Python
- Exécution des tests unitaires
- Validation du DAG Airflow
- Déploiement automatique sur une machine virtuelle **AWS EC2**

Le workflow se trouve dans : `.github/workflows/ci-cd.yml`

Le déploiement est configuré pour envoyer et exécuter le pipeline sur une instance **EC2 (Amazon Web Services)**, garantissant une mise en production continue et automatisée.
### **Section 10 : Data Modeling**

```markdown
## 📊 Data Modeling (Star Schema)

Le projet utilise **dbt** pour transformer les données en un schéma optimisé pour la BI :

- **Bronze** : `EVENTS_RAW` (données brutes)
- **Silver** : Nettoyage et préparation
- **Gold** :
  - `DIM_LOCATION` → Informations géographiques
  - `DIM_DATE` → Calendrier 2026–2027
  - `DIM_CATEGORIES` → Catégories d’événements
  - `FACT_EVENTS` → Table de faits centrale
```
### **Section 11 : Conclusion**
Ce projet démontre une maîtrise complète du cycle de vie des données : de l’extraction asynchrone jusqu’à la modélisation analytique en passant par le déploiement cloud et l’automatisation CI/CD.

**Technologies mises en œuvre** : Airflow, RabbitMQ, Snowflake, dbt, Docker, AWS EC2 et GitHub Actions.

