import json
import snowflake.connector
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer # Remplacement de pika

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

# --- CONNEXION KAFKA ---
try:
    consumer = KafkaConsumer(
        'events_topic',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',       # Commence à lire depuis le début si aucun historique de lecture
        enable_auto_commit=False,           # Nous commiterons manuellement
        group_id='snowflake_ingestion_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
except Exception as e:
    print(f"❌ Erreur Kafka : {e}")
    exit(1)

print(' [*] Lecture des messages existants dans le topic Kafka ...')

try:
    # On boucle tant que Poll nous retourne des données
    while True:
        # Attend max 5 secondes pour avoir de nouveaux messages
        msg_pack = consumer.poll(timeout_ms=5000)

        if not msg_pack:
            print(' [*] Plus de messages à consommer (5s sans recevoir de données). Arrêt du consumer.')
            break

        # S'il y a des données, on itère sur les topics/partitions reçus
        for tp, messages in msg_pack.items():
            for message in messages:
                event = message.value
                
                try:
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
                    print(f" [OK] {event.get('nom')} inséré.")

                except Exception as e:
                    print(f" [ERREUR] Insertion impossible : {e}")
                    # En production avec Kafka, il faudrait router cela vers un "Dead Letter Topic"
        
        conn.commit()
        # On valide le fait qu'on a bien traité et sauvegardé ces offsets seulement après le commit Snowflake
        consumer.commit()

except KeyboardInterrupt:
    print("\n [!] Arrêt manuel du consumer.")
finally:
    cursor.close()
    conn.close()
    if 'consumer' in locals():
        consumer.close()