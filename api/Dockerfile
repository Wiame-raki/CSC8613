# Dockerfile

# TODO: choisir une image de base Python
FROM python:3.10-slim

# TODO: définir le répertoire de travail dans le conteneur
WORKDIR /app

# TODO: copier le fichier app.py
COPY app.py /app

# Installer FastAPI et Uvicorn
RUN pip install fastapi uvicorn

# TODO: lancer le serveur au démarrage du conteneur
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
