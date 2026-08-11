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
from extraction_sheet import cast_matricule

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

def get_pc_mysql(config):
    """
    Récupère les données agrégées directement depuis MySQL.

   
    """

    
    print("Connexion à la base de données MySQL...")

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT DISTINCT username,source_pc FROM log_events WHERE username REGEXP '^[0-9]+$' GROUP by username,source_pc ORDER BY CAST(username AS UNSIGNED) ASC;
        """

        cursor.execute(query)
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

def insertion_pc_sheetV1(df_presence, file_creds, scopes, spreadsheet_name):
    """
    Synchronise les données de présence avec Google Sheets.
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
        sheet = spreadsheet.worksheet("colab_dans_graylog")
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title="colab_dans_graylog",
            rows="1000",
            cols="7"
        )

    headers = [
        "username",
        "source_pc",
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


def insertion_pc_sheet(df_pc, file_creds, scopes, spreadsheet_name):
    """
    Insère dans Google Sheets les couples :
        username | source_pc

    Le DataFrame doit contenir les colonnes :
        - username
        - source_pc

    Les anciennes données de la feuille sont remplacées.
    """

    if df_pc.empty:
        print("-> Aucune donnée à transférer.")
        return

    print("Connexion à Google Sheets...")

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        file_creds,
        scopes
    )

    client = gspread.authorize(creds)
    spreadsheet = client.open(spreadsheet_name)

    # -------------------------------------------------------
    # Récupération ou création de la feuille
    # -------------------------------------------------------

    try:
        sheet = spreadsheet.worksheet("colab_dans_graylog")

    except gspread.exceptions.WorksheetNotFound:

        sheet = spreadsheet.add_worksheet(
            title="colab_dans_graylog",
            rows="1000",
            cols="2"
        )

    # -------------------------------------------------------
    # Préparation des données
    # -------------------------------------------------------

    headers = [
        "username",
        "source_pc",
    ]

    rows = []

    for _, row in df_pc.iterrows():

        username = str(row["username"]).strip()
        source_pc = str(row["source_pc"]).strip()

        rows.append([
            username,
            source_pc,
        ])

    # -------------------------------------------------------
    # Nettoyage de la feuille
    # -------------------------------------------------------

    sheet.clear()

    # -------------------------------------------------------
    # Insertion des données
    # -------------------------------------------------------

    sheet.update(
        range_name="A1",
        values=[headers] + rows,
        value_input_option="USER_ENTERED"
    )

    print(f"✓ {len(rows)} ligne(s) insérée(s).")
    print("-> Synchronisation terminée.")


def main():
    print("=== DÉBUT DU SCRIPT D'EXTRACTION ===")
    
    try:
        # Étape 1 : Récupération
        df_brut = get_pc_mysql(DB_CONFIG)
        # print(df_brut)
        # sys.exit() 
       
        insertion_pc_sheet(df_brut, CREDS_FILE, SCOPES, SPREADSHEET_NAME)
        
    except Exception as error:
        print("\nUne erreur générale est survenue :")
        traceback.print_exc()      # Affiche la pile d'appels complète
        print(f"\nType : {type(error)}")
        print(f"Message : {repr(error)}")
        
    print("=== FIN DU SCRIPT ===")

if __name__ == "__main__":
    main()