import os
import time
import json
import pika
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- 1. Configuration et API Key ---
load_dotenv()
api_key = os.getenv("API_KEY")
if not api_key:
    print("❌ API_KEY manquante dans .env")
    raise SystemExit(1)

# --- 2. Connexion RabbitMQ ---
try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost",
            credentials=pika.PlainCredentials("admin", "admin123")
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue="events_queue", durable=True)
except Exception as e:
    print(f"❌ Erreur de connexion RabbitMQ : {e}")
    raise SystemExit(1)

# --- 3. URL Ticketmaster ---
url = "https://app.ticketmaster.com/discovery/v2/events.json"
sent_event_ids = set()

year = 2027
print(f"🚀 Producer démarré pour tous les événements de l'année {year}...")

# --- Générer les intervalles mensuels ---
months = [(datetime(year, m, 1), (datetime(year, m+1, 1) - timedelta(seconds=1) if m < 12 else datetime(year, 12, 31, 23, 59, 59))) for m in range(1, 13)]

try:
    while True:
        for start, end in months:
            start_date = start.isoformat() + "Z"
            end_date = end.isoformat() + "Z"

            print(f"📅 Collecte des événements de {start.strftime('%B %Y')}")

            page_size = 50
            max_pages_allowed = 1000 // page_size  # Limite Ticketmaster
            params = {
                "apikey": api_key,
                "size": page_size,
                "page": 0,
                "startDateTime": start_date,
                "endDateTime": end_date
            }

            # --- Étape 1 : requête initiale pour connaître le nombre total de pages ---
            try:
                res = requests.get(url, params=params, timeout=20)
                res.raise_for_status()
                data = res.json()
                api_total_pages = data.get("page", {}).get("totalPages", 1)
                total_pages = min(api_total_pages, max_pages_allowed)
            except requests.exceptions.HTTPError as e:
                print(f"⚠️ Erreur initiale pour {start.strftime('%B')}: {e}. Pause 30s.")
                time.sleep(30)
                continue

            sent_count = 0

            # --- Étape 2 : Parcours des pages ---
            for page in range(total_pages):
                params["page"] = page
                time.sleep(0.2)  # Respect rate limit

                try:
                    res = requests.get(url, params=params, timeout=20)
                    if res.status_code == 400:
                        print(f"ℹ️ Page {page} inaccessible pour {start.strftime('%B')} (limite atteinte).")
                        break
                    res.raise_for_status()
                    data = res.json()
                    events = data.get("_embedded", {}).get("events", [])
                except requests.exceptions.HTTPError as e:
                    print(f"⚠️ Erreur HTTP page {page} de {start.strftime('%B')}: {e}")
                    continue

                # --- Étape 3 : traitement des événements ---
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

                    channel.basic_publish(
                        exchange="",
                        routing_key="events_queue",
                        body=json.dumps(msg, ensure_ascii=False),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    sent_count += 1

            print(f"✅ {sent_count} événements envoyés pour {start.strftime('%B %Y')}.\n")
        
        # Pause avant de recommencer le scan complet
        print("😴 Pause de 60 secondes avant le prochain tour de collecte...")
        time.sleep(60)

except KeyboardInterrupt:
    print("\n🛑 Arrêt manuel demandé.")

finally:
    if 'connection' in locals() and connection.is_open:
        connection.close()
        print("🔌 Connexion RabbitMQ fermée.")