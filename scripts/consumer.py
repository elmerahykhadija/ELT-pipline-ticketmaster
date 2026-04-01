import pika
import json
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONNEXION SNOWFLAKE ---
try:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="TICKETMASTER_DB",
        schema="RAW_DATA"
    )
    cursor = conn.cursor()
    print("✅ Connecté à Snowflake. Prêt pour l'ingestion.")
except Exception as e:
    print(f"❌ Erreur Snowflake : {e}")
    exit(1)

def callback(ch, method, properties, body):
    try:
        event = json.loads(body.decode('utf-8'))
        
        sql = """
        INSERT INTO EVENTS_RAW (id, nom, type, date_locale, heure_locale, date_utc, lieu, ville, segment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            event.get('id'), event.get('nom'), event.get('type'),
            event.get('date_locale'), event.get('heure_locale'),
            event.get('date_utc'), event.get('lieu'),
            event.get('ville'), event.get('segment')
        )
        
        cursor.execute(sql, params)
        conn.commit()

        print(f" [OK] {event.get('nom')} inséré.")

        # ✅ ACK seulement après succès
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f" [ERREUR] : {e}")

        # ❗ Option : remettre le message dans la queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# --- CONNEXION RABBITMQ ---
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="rabbitmq", 
        credentials=pika.PlainCredentials("admin", "admin123")
    )
)

channel = connection.channel()

# Queue durable
channel.queue_declare(queue="events_queue", durable=True)

# 🔥 Très important pour Snowflake (évite surcharge)
channel.basic_qos(prefetch_count=1)

print(' [*] Lecture des messages existants dans la queue ...')

try:
    while True:
        # Récupère un message de la queue (non bloquant)
        method_frame, header_frame, body = channel.basic_get(queue='events_queue', auto_ack=False)
        
        if method_frame:
            # Traite le message avec la même fonction callback
            callback(channel, method_frame, header_frame, body)
        else:
            # Plus de messages dans la queue
            print(' [*] La queue est vide. Arrêt du consumer.')
            break
except KeyboardInterrupt:
    print("\n [!] Arrêt manuel du consumer.")
finally:
    cursor.close()
    conn.close()
    connection.close()