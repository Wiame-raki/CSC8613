from fastapi import FastAPI
from fastapi.responses import Response
from pydantic import BaseModel
from feast import FeatureStore
import mlflow.pyfunc
import pandas as pd
import os
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# --- C'est cette ligne qui manquait et causait le NameError ---
app = FastAPI(title="StreamFlow Churn Prediction API")

# --------------------
# Config & Init
# --------------------
REPO_PATH = "/repo"
MODEL_NAME = "streamflow_churn"
MODEL_URI = f"models:/{MODEL_NAME}/Production"

try:
    store = FeatureStore(repo_path=REPO_PATH)
    model = mlflow.pyfunc.load_model(MODEL_URI)

except Exception as e:

    store = None
    model = None

class UserPayload(BaseModel):
    user_id: str

@app.get("/health")
def health():
    return {"status": "ok"}

# --------------------
# Prometheus Metrics
# --------------------

# TODO: Créez les métriques
REQUEST_COUNT = Counter("api_requests_total", "Total number of API requests")
REQUEST_LATENCY = Histogram("api_request_latency_seconds", "Latency of API requests in seconds")

@app.post("/predict")
def predict(payload: UserPayload):
    # TODO: prendre le temps au départ avec time
    start_time = time.time()

    # TODO: incrementiez le request counter
    REQUEST_COUNT.inc()

    # Logique devant normalement exister dans votre code
    if store is None or model is None:
        return {"error": "Model or feature store not initialized"}

    features_request = [
        "subs_profile_fv:months_active",
        "subs_profile_fv:monthly_fee",
        "subs_profile_fv:paperless_billing",
        "subs_profile_fv:plan_stream_tv",
        "subs_profile_fv:plan_stream_movies",
        "subs_profile_fv:net_service",
        "usage_agg_30d_fv:watch_hours_30d",
        "usage_agg_30d_fv:avg_session_mins_7d",
        "usage_agg_30d_fv:unique_devices_30d",
        "usage_agg_30d_fv:skips_7d",
        "usage_agg_30d_fv:rebuffer_events_7d",
        "payments_agg_90d_fv:failed_payments_90d",
        "support_agg_90d_fv:support_tickets_90d",
        "support_agg_90d_fv:ticket_avg_resolution_hrs_90d",
    ]
    
    # --- RECUPERATION DES FEATURES (Nécessaire pour définir X) ---
    feature_dict = store.get_online_features(
        features=features_request,
        entity_rows=[{"user_id": payload.user_id}],
    ).to_dict()

    X = pd.DataFrame({k: [v[0]] for k, v in feature_dict.items()})
    # -------------------------------------------------------------

    X = X.drop(columns=["user_id"], errors="ignore")
    y_pred = model.predict(X)

    # TODO: observe latency in seconds (end - start)
    REQUEST_LATENCY.observe(time.time() - start_time)

    return {
        "user_id": payload.user_id,
        "prediction": int(y_pred[0]), 
        "features_used": X.to_dict(orient="records")[0],
    }

@app.get("/metrics")
def metrics():
    # TODO: returnez une Response avec generate_latest() et CONTENT_TYPE_LATEST
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)