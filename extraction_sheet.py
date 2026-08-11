import mysql.connector
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from dotenv import load_dotenv
from tzlocal import get_localzone
import os
import sys
import traceback

# # 1. Définir le fuseau horaire cible (Madagascar)
# TZ_LOCAL = ZoneInfo("Indian/Antananarivo")
# 1. Détection AUTOMATIQUE et DYNAMIQUE du fuseau horaire de la machine
TZ_LOCAL = get_localzone()

# Charger les variables d'environnement du fichier .env
load_dotenv()




# ==========================================
# CONFIGURATION
# ==========================================

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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "credentials.json")

SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")


# ==========================================
# FONCTIONS PAR COMPARTIMENT
# ==========================================

from datetime import datetime
import mysql.connector
import pandas as pd


from datetime import datetime, timedelta
import mysql.connector

def recuperer_donnees_mysql(config, date_cible=None):
    """
    Récupère les données agrégées directement depuis MySQL.

    Si 'date_cible' est None, utilise la date du jour.
    """

    if date_cible is None:
        date_cible = datetime.now().strftime("%Y-%m-%d")
        print(f"Aucune date spécifiée. Utilisation de la date du jour : {date_cible}")
    else:
        print(f"Analyse pour la date demandée : {date_cible}")

    # Début de journée
    debut_jour = f"{date_cible} 00:00:00"

    # print(debut_jour);
    # sys.exit()

    # Fin de journée (= début du lendemain)
    fin_jour = (
        datetime.strptime(date_cible, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d %H:%M:%S")

    print("Connexion à la base de données MySQL...")

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                username,
                source_pc,
                DATE(event_time) AS jour,
                MIN(event_time) AS date_debut,
                MAX(event_time) AS date_fin
            FROM log_events
            WHERE event_time >= %s
              AND event_time < %s
            GROUP BY username,source_pc, DATE(event_time)
        """

        cursor.execute(query, (debut_jour, fin_jour))
        resultats = cursor.fetchall()
        # print(resultats)
        # sys.exit()
        
        return pd.DataFrame(resultats)

    except mysql.connector.Error as err:
        print(f"Erreur lors de la récupération des données : {err}")
        return []

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connexion MySQL fermée.")


def calculer_temps_presence(df_logs):
    """
    2. Calcule simplement la durée en secondes basée sur les colonnes
       générées dynamiquement par la requête SQL.
    """
    
    if df_logs.empty:
        print("-> Aucun log à traiter.")
        return pd.DataFrame()
    
    
    print("Calcul des durées de présence globales...")

    # S'assurer que les colonnes sont bien lues comme des dates/heures par Pandas
    df_logs["date_debut"] = pd.to_datetime(df_logs["date_debut"])
    df_logs["date_fin"] = pd.to_datetime(df_logs["date_fin"])

    # Calcul direct de la durée en secondes entre la fin calculée et le début
    df_logs["duration_seconds"] = (
        (df_logs["date_fin"] - df_logs["date_debut"])
        .dt.total_seconds()
        .astype(int)
    )

    # Ajout du timestamp de traitement
    df_logs["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Formatage propre des colonnes de date pour le rendu final
    df_logs["date_debut"] = df_logs["date_debut"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    df_logs["date_fin"] = df_logs["date_fin"].dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"-> {len(df_logs)} états de présence calculés.")
    return pd.DataFrame(df_logs)

def cast_matricule(value):
    value = str(value).strip()

    # si uniquement des chiffres → int
    if value.isdigit():
        return int(value)

    # sinon → string
    return value

def inserer_dans_google_sheet(df_presence, file_creds, scopes, spreadsheet_name):
    """
    Synchronise les données de présence avec Google Sheets.

    Règles :
    - Si la feuille n'existe pas, elle est créée.
    - Une présence est identifiée par (username, date_debut).
    - Si la ligne existe :
        * date_fin MySQL > date_fin Sheet  -> UPDATE
        * date_fin MySQL == date_fin Sheet -> RIEN
        * date_fin MySQL < date_fin Sheet  -> RIEN
    - Sinon -> INSERT
    """

    if df_presence.empty:
        print("-> Aucune donnée à transférer.")
        return

    print("Connexion à Google Sheets...")

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        file_creds,
        scopes
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open(spreadsheet_name)
    # print(client.list_spreadsheet_files())
    # sys.exit()

    try:
        sheet = spreadsheet.worksheet("presence")
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title="presence",
            rows="1000",
            cols="7"
        )

    headers = [
        "username",
        "source_pc",
        "date_debut",
        "date_fin",
        "duration_seconds",
        "created_at",
        "heure_travail",
    ]

    values = sheet.get_all_values()
    # print(values)
    # sys.exit()
    if not values:
        sheet.append_row(headers)
        values = [headers]

    # -------------------------------------------------------
    # Index : (username, date) -> infos de la ligne
    # -------------------------------------------------------
    existing_rows = {}

    for row_number, row in enumerate(values[1:], start=2):

        if len(row) < 4:
            continue

        username = row[0].strip()

        try:
            date_debut = str(pd.to_datetime(row[2]).date())
        except:
            continue

        key = (username, date_debut)

        existing_rows[key] = {
            "row": row_number,
            "date_fin": row[3]
        }

    # -------------------------------------------------------
    # Synchronisation
    # -------------------------------------------------------

    # ligne = df_presence.loc[31]
    # print(ligne)
    # sys.exit()
    
    updates = []
    rows_to_insert = []
    for _, presence in df_presence.iterrows():

        # presence = ligne
        username = cast_matricule(presence["username"])

        date_debut = str(
            pd.to_datetime(
                presence["date_debut"]
            ).date()
        )

        heure_travail = round(
            presence["duration_seconds"] / 3600,
            2
        )

        key = (str(username), date_debut)
        # key = ('650', '2026-07-03')

        row_data = [
            username,
            presence["source_pc"],
            presence["date_debut"],
            presence["date_fin"],
            presence["duration_seconds"],
            presence["created_at"],
            heure_travail,
        ]

        # print(presence.loc[31])
        # print(key)
        # sys.exit()

        # -----------------------------
        # Déjà présent
        # -----------------------------
       
        if key in existing_rows:

            info = existing_rows[key]
            row_number = info["row"]

            try:
                sheet_datetime_fin = pd.to_datetime(info["date_fin"])
                mysql_datetime_fin = pd.to_datetime(presence["date_fin"])
            except Exception:
                sheet_datetime_fin = None
                mysql_datetime_fin = None

            if sheet_datetime_fin is None or mysql_datetime_fin is None:
                print(f"= IGNORÉ : {username} ({date_debut}) date invalide")

            elif mysql_datetime_fin > sheet_datetime_fin:

                updates.extend([
                    {
                        "range": f"D{row_number}:E{row_number}",
                        "values": [[
                            presence["date_fin"],
                            presence["duration_seconds"],
                        ]]
                    },
                    {
                        "range": f"G{row_number}",
                        "values": [[
                            heure_travail
                        ]]
                    }
                ])

                print(f"✓ UPDATE : {username} ({date_debut})")

            elif mysql_datetime_fin == sheet_datetime_fin:

                print(f"= IGNORÉ : {username} ({date_debut}) date identique")

            else:

                print(f"= IGNORÉ : {username} ({date_debut}) Google plus récent")


# Nouvelle présence
# -----------------------------
        else:

            rows_to_insert.append(row_data)

            print(f"+ INSERT : {username} ({date_debut})")
       
# Envoi des mises à jour
# ----------------------------------

    # print(rows_to_insert)
    # sys.exit()
    if updates:
        sheet.batch_update(updates)
        print(f"{len(updates)//2} mise(s) à jour envoyée(s).")

    # ----------------------------------
    # Insertion des nouvelles lignes
    # ----------------------------------
    if rows_to_insert:
        sheet.append_rows(
            rows_to_insert,
            value_input_option="USER_ENTERED"
        )
    print(f"{len(rows_to_insert)} ligne(s) insérée(s).")
    print("-> Synchronisation terminée.")


# ==========================================
# ORCHESTRATION (MAIN)
# ==========================================

def main():
    print("=== DÉBUT DU SCRIPT D'EXTRACTION ===")
    
    try:
        # Étape 1 : Récupération
        df_brut = recuperer_donnees_mysql(DB_CONFIG,None)
        
        # Étape 2 : Calculs
        df_calcule = calculer_temps_presence(df_brut)

        # print(df_calcule)
        # sys.exit()

        
        
        # Étape 3 : Insertion
        inserer_dans_google_sheet(df_calcule, CREDS_FILE, SCOPES, SPREADSHEET_NAME)
        
    except Exception as error:
        print("\nUne erreur générale est survenue :")
        traceback.print_exc()      # Affiche la pile d'appels complète
        print(f"\nType : {type(error)}")
        print(f"Message : {repr(error)}")
        
    print("=== FIN DU SCRIPT ===")

if __name__ == "__main__":
    main()