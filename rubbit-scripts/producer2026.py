import os
import time
import json
import pika
import requests
from dotenv import load_dotenv
from datetime import datetime

# --- 1. Chargement de la configuration ---
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

# --- 3. Fonctions utilitaires ---
def generate_month_ranges(year):
    """Génère automatiquement les intervalles mensuels pour une année donnée."""
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
page_size = 50  # Ticketmaster limite 1000 résultats (20 pages max)

# --- 5. Début du Producer ---
print("🚀 Producer démarré. Envoi des données par mois...")

try:
    # Générer tous les mois de l'année 2026
    months = generate_month_ranges(2026)

    for start_date, end_date in months:
        print(f"📅 Traitement du mois : {start_date} à {end_date}")
        total_pages = 1  # valeur par défaut

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
            total_pages = min(api_total_pages, 1000 // page_size)  # max 1000 events
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ Erreur HTTP initiale : {e}. Passage au mois suivant.")
            continue
        except Exception as e:
            print(f"⚠️ Erreur inconnue : {e}. Passage au mois suivant.")
            continue

        # --- Étape 2 : boucle sur les pages ---
        for page in range(total_pages):
            params["page"] = page
            try:
                time.sleep(0.2)  # Respect du rate limit (5 req/sec)
                res = requests.get(url, params=params, timeout=20)

                if res.status_code == 400:
                    print(f"ℹ️ Page {page} inaccessible (limite Ticketmaster).")
                    break

                res.raise_for_status()
                events = res.json().get("_embedded", {}).get("events", [])

            except requests.exceptions.HTTPError as e:
                print(f"⚠️ Erreur HTTP page {page}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Erreur inconnue page {page}: {e}")
                continue

            # --- Étape 3 : envoi vers RabbitMQ ---
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

                channel.basic_publish(
                    exchange="",
                    routing_key="events_queue",
                    body=json.dumps(msg, ensure_ascii=False),
                    properties=pika.BasicProperties(delivery_mode=2)  # persistant
                )
                sent_count += 1

            print(f"📄 Page {page} : {sent_count} nouveaux messages envoyés.")

        print(f"✅ Mois {start_date} terminé. Total unique envoyés : {len(sent_event_ids)}")
        print("⏱ Pause de 5 secondes avant le mois suivant...")
        time.sleep(5)

except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé par l'utilisateur.")

finally:
    if 'connection' in locals() and connection.is_open:
        connection.close()
        print("🔌 Connexion RabbitMQ fermée proprement.")