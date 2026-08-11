import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from tzlocal import get_localzone
import mysql.connector
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

TZ_LOCAL = get_localzone()
load_dotenv()

GRAYLOG_URL = os.getenv("GRAYLOG_URL")
GRAYLOG_USER = os.getenv("GRAYLOG_USER")
GRAYLOG_PASSWORD = os.getenv("GRAYLOG_PASSWORD")
STREAM_ID = os.getenv("GRAYLOG_STREAM_ID")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


def get_last_event_time(custom_date=None):
    """
    Récupère la date de référence :
    - Si 'custom_date' est fournie (ex: '2026-07-27'), renvoie '2026-07-27 00:00:00'.
    - Sinon, va chercher le dernier log enregistré en BDD (SELECT MAX).
    """
    if custom_date:
        try:
            if isinstance(custom_date, str):
                date_obj = datetime.strptime(custom_date, "%Y-%m-%d")
                return date_obj.replace(hour=0, minute=0, second=0)
            elif hasattr(custom_date, "strftime"):
                return datetime(custom_date.year, custom_date.month, custom_date.day, 0, 0, 0)
        except ValueError as e:
            print(f"[ERREUR FORMAT DATE] Utilisez le format 'YYYY-MM-DD' : {e}")
            return None

    # Comportement par défaut : MySQL
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT MAX(event_time) FROM log_events")
        result = cur.fetchone()[0]
        return result
    except mysql.connector.Error as err:
        print(f"[ERREUR BDD] Impossible de lire le dernier événement: {err}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()


def fetch_graylog(last_time):
    """Interroge l'API Graylog avec une marge de chevauchement d'1h."""
    graylog_query = r"""
    EventID:(4624 OR 4634)
    AND NOT TargetUserName:("SYSTEM" OR "Système" OR "SERVICE LOCAL" OR "SERVICE RÉSEAU")
    AND NOT TargetUserName:DWM*
    AND NOT TargetUserName:UMFD*
    AND NOT TargetUserName:/.*\$/
    """

    if last_time:
        now_local = datetime.now()
        delta_seconds = int((now_local - last_time).total_seconds())
        # Ajout de 1h de marge (3600s), minimum 3600s
        time_range = max(delta_seconds + 3600, 3600)
    else:
        time_range = 86400  # 24h par défaut

    params = {
        "query": graylog_query,
        "range": time_range,
        "limit": 5000,
        "filter": f"streams:{STREAM_ID}",
    }

    headers = {"Accept": "application/json", "X-Requested-By": "python-script"}

    try:
        response = requests.get(
            GRAYLOG_URL,
            params=params,
            headers=headers,
            auth=HTTPBasicAuth(GRAYLOG_USER, GRAYLOG_PASSWORD),
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERREUR API] Échec de la requête Graylog: {e}")
        return {}


def parse(msg):
    """Extrait et nettoie les données d'un message brut."""
    ts = msg.get("timestamp")
    if not ts:
        return None

    try:
        event_time_utc = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        event_time_local = event_time_utc.astimezone(TZ_LOCAL).replace(tzinfo=None)
        
        event_id = int(msg.get("EventID")) if msg.get("EventID") else None
        
        return (
            event_time_local,
            msg.get("TargetUserName"),
            msg.get("source"),
            event_id
        )
    except Exception as e:
        print(f"[AVERTISSEMENT] Erreur parsing message: {e}")
        return None


def insert(events):
    """Insère en BDD avec gestion automatique des doublons."""
    if not events:
        print("Aucun nouvel événement à insérer.")
        return

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()

        sql = """
        INSERT IGNORE INTO log_events
        (event_time, username, source_pc, event_id)
        VALUES (%s, %s, %s, %s)
        """

        cur.executemany(sql, events)
        conn.commit()
        print(f"[SUCCÈS] {cur.rowcount} lignes insérées en BDD (doublons ignorés).")

    except mysql.connector.Error as err:
        print(f"[ERREUR BDD] Échec de l'insertion: {err}")
    finally:
        if conn and conn.is_connected():
            conn.close()


def main():
    print(f"--- Démarrage de l'extraction : {datetime.now()} ---")

    
    

    last_time = get_last_event_time(None)
    print(f"Date de départ retenue : {last_time}")

    data = fetch_graylog(last_time)
    messages = data.get("messages", [])
    print(f"Nombre de messages reçus de l'API : {len(messages)}")
    
    events = []
    for m in messages:
        msg_content = m.get("message")
        if not msg_content:
            continue

        parsed = parse(msg_content)
        if parsed:
            events.append(parsed)

    # print(events)
    # sys.exit()
    insert(events)
    print("--- Fin du traitement ---")


if __name__ == "__main__":
    main()