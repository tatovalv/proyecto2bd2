"""
Dashboard web para visualizar resultados de análisis GDELT desde MongoDB.
"""

import json
import os
from datetime import datetime

from bson import ObjectId
from flask import Flask, jsonify, render_template
from pymongo import MongoClient

app = Flask(__name__)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:gdelt2026@mongodb:27017/gdelt_analytics?authSource=admin",
)

client = MongoClient(MONGO_URI)
db = client.get_default_database()

COLLECTIONS = [
    ("conflict_heatmap", "Mapa de Calor de Conflictos (Goldstein)"),
    ("top_countries_events", "Top 10 Países con Más Eventos"),
    ("tone_sources_correlation", "Correlación AvgTone vs Fuentes"),
    ("cameo_by_region", "Distribución CAMEO por Región"),
    ("actor_interaction_matrix", "Matriz de Interacción de Actores"),
    ("media_coverage", "Cobertura Mediática por País"),
    ("sentiment_trend", "Tendencia de Sentimiento"),
    ("conflict_country_pairs", "Pares de Países en Conflicto"),
    ("escalation_events", "Escalada de Eventos (24h)"),
    ("religion_conflict_clusters", "Conflictos por Religión y Región"),
    ("gkg_themes_by_continent", "Temas GKG por Continente"),
    ("top_organizations", "Organizaciones Más Mencionadas"),
    ("tone_lag_analysis", "Análisis de Rezago Tono→Conflicto"),
    ("diplomatic_conflict_graph", "Grafo Diplomático vs Conflicto"),
    ("source_diversity_index", "Diversidad de Fuentes por País"),
    ("ethnic_conflict_frequency", "Conflictos por Etnia"),
    ("breaking_news", "Noticias de Última Hora"),
    ("quadclass_timeline", "Evolución QuadClass por Región"),
    ("hourly_event_density", "Densidad de Eventos por Hora"),
]


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def serialize_doc(doc):
    doc.pop("_id", None)
    for key, val in doc.items():
        if isinstance(val, datetime):
            doc[key] = val.isoformat()
        elif hasattr(val, "isoformat"):
            doc[key] = val.isoformat()
    return doc


@app.route("/")
def index():
    meta = db.pipeline_metadata.find_one(sort=[("last_run", -1)])
    return render_template(
        "index.html",
        collections=COLLECTIONS,
        last_run=meta.get("last_run") if meta else "N/A",
    )


@app.route("/api/<collection>")
def api_collection(collection):
    allowed = {c[0] for c in COLLECTIONS}
    if collection not in allowed:
        return jsonify({"error": "Colección no válida"}), 404

    limit = 500
    docs = list(db[collection].find().limit(limit))
    docs = [serialize_doc(d) for d in docs]
    return jsonify({"collection": collection, "count": len(docs), "data": docs})


@app.route("/api/summary")
def api_summary():
    summary = {}
    for col_name, _ in COLLECTIONS:
        summary[col_name] = db[col_name].estimated_document_count()
    meta = db.pipeline_metadata.find_one(sort=[("last_run", -1)])
    return jsonify({
        "collections": summary,
        "last_run": meta.get("last_run") if meta else None,
        "total_records": sum(summary.values()),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
