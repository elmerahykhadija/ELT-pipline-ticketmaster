import os
import time
import json
import requests
from dotenv import load_dotenv
from datetime import datetime
from kafka import KafkaProducer  # Remplacement de pika par KafkaProducer

# --- 1. Chargement de la configuration ---
load_dotenv()
api_key = os.getenv("API_KEY")
if not api_key:
    print("❌ API_KEY manquante dans .env")
    raise SystemExit(1)

# --- 2. Connexion Kafka ---
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'], # Adresse pour la communication interne Docker
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        retries=5,
        acks='all' # Garantie de persistance
    )
    topic_name = "events_topic"
except Exception as e:
    print(f"❌ Erreur de connexion Kafka : {e}")
    raise SystemExit(1)

# --- 3. Fonctions utilitaires ---
def generate_month_ranges(year):
    months = []
    for month in range(1, 13):
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, month + 1, 1)
        months.append((
            start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))
    return months

# --- 4. Paramètres API ---
url = "https://app.ticketmaster.com/discovery/v2/events.json"
sent_event_ids = set()
page_size = 50

# --- 5. Début du Producer ---
print("🚀 Producer démarré. Envoi des données par mois...")

try:
    months = generate_month_ranges(2026)

    for start_date, end_date in months:
        print(f"📅 Traitement du mois : {start_date} à {end_date}")
        total_pages = 1

        # --- Étape 1 : récupérer le nombre total de pages pour le mois ---
        params = {
            "apikey": api_key,
            "size": page_size,
            "page": 0,
            "startDateTime": start_date,
            "endDateTime": end_date
        }
        try:
            res = requests.get(url, params=params, timeout=20)
            res.raise_for_status()
            data = res.json()
            api_total_pages = data.get("page", {}).get("totalPages", 1)
            total_pages = min(api_total_pages, 1000 // page_size)
        except Exception as e:
            print(f"⚠️ Erreur Http/Inconnue initiale : {e}. Passage au mois suivant.")
            continue

        # --- Étape 2 : boucle sur les pages ---
        for page in range(total_pages):
            params["page"] = page
            try:
                time.sleep(0.2)
                res = requests.get(url, params=params, timeout=20)

                if res.status_code == 400:
                    print(f"ℹ️ Page {page} inaccessible (limite Ticketmaster).")
                    break

                res.raise_for_status()
                events = res.json().get("_embedded", {}).get("events", [])

            except Exception as e:
                print(f"⚠️ Erreur page {page}: {e}")
                continue

            # --- Étape 3 : envoi vers Kafka ---
            sent_count = 0
            for event in events:
                event_id = event.get("id")
                if not event_id or event_id in sent_event_ids:
                    continue

                sent_event_ids.add(event_id)

                venues = event.get("_embedded", {}).get("venues", [{}])
                venue_info = venues[0] if venues else {}
                classifications = event.get("classifications", [{}])
                class_info = classifications[0] if classifications else {}

                msg = {
                    "nom": event.get("name"),
                    "type": event.get("type"),
                    "id": event_id,
                    "date_locale": event.get("dates", {}).get("start", {}).get("localDate"),
                    "heure_locale": event.get("dates", {}).get("start", {}).get("localTime"),
                    "date_utc": event.get("dates", {}).get("start", {}).get("dateTime"),
                    "lieu": venue_info.get("name"),
                    "ville": venue_info.get("city", {}).get("name"),
                    "segment": class_info.get("segment", {}).get("name"),
                }

                # Envoi du message au Topic Kafka (asynchrone)
                producer.send(topic_name, value=msg)
                sent_count += 1
            
            # S'assurer que tous les messages de la page sont bien expédiés
            producer.flush()
            print(f"📄 Page {page} : {sent_count} nouveaux messages envoyés dans Kafka.")

        print(f"✅ Mois {start_date} terminé. Total unique envoyés : {len(sent_event_ids)}")
        time.sleep(5)

except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé par l'utilisateur.")

finally:
    if 'producer' in locals():
        producer.flush()
        producer.close()
        print("🔌 Connexion Kafka fermée proprement.")