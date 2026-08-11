# 1. Image Python légère
FROM python:3.11-slim

# 2. Installer cron
RUN apt-get update \
    && apt-get install -y cron procps \
    && rm -rf /var/lib/apt/lists/*

# 3. Dossier de travail
WORKDIR /app

# 4. Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copier le code
COPY main.py extraction_sheet.py pc_graylog.py ./

# 6. Copier les tâches cron (Correction du nom du fichier source)
COPY cronjob /etc/cron.d/graylog-cron

# 7. Permissions obligatoires et enregistrement de cron
RUN chmod 0644 /etc/cron.d/graylog-cron && crontab /etc/cron.d/graylog-cron

# 8. Créer les fichiers de log
RUN touch /var/log/cron-main.log /var/log/cron-sheet.log /var/log/pc_graylog.log

# 9. Charger les variables d'environnement, lancer cron (arrière-plan) et suivre les deux logs
CMD ["sh", "-c", "env > /etc/environment && cron && tail -f /var/log/cron-main.log /var/log/cron-sheet.log /var/log/pc_graylog.log"]