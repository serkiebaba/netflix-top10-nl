# app.py
import os, json
from flask import Flask, jsonify
from flask_cors import CORS

APP_NAME = "NETFLIX TOP 10 (NL)"
CATALOG_SERIES_ID = "netflix-top10-nl"
CATALOG_MOVIES_ID = "netflix-top10-nl-movies"

app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(__file__)
CACHE_SERIES = os.path.join(BASE_DIR, "cache", "netflix_nl_series.json")
CACHE_MOVIES = os.path.join(BASE_DIR, "cache", "netflix_nl_movies.json")

def _load(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "metas" in data:
            return data["metas"]
    except Exception:
        pass
    return []

def load_series():
    return _load(CACHE_SERIES)

def load_movies():
    return _load(CACHE_MOVIES)

@app.route("/manifest.json")
def manifest():
    # We bieden 2 catalogs aan (Series + Movies) en ook de "meta" resource
    return jsonify({
        "id": "nl-top10-addon",
        "version": "1.3.0",
        "name": APP_NAME,
        "description": "Netflix NL Top 10 (Series & Movies) uit dagelijkse cache (FlixPatrol → GitHub Actions).",
        "resources": ["catalog", "meta"],
        "types": ["series", "movie"],
        "catalogs": [
            {"type": "series", "id": CATALOG_SERIES_ID, "name": f"{APP_NAME} – Series"},
            {"type": "movie",  "id": CATALOG_MOVIES_ID, "name": f"{APP_NAME} – Movies"}
        ]
    })

@app.route(f"/catalog/series/{CATALOG_SERIES_ID}.json")
def catalog_series():
    metas = load_series()
    return jsonify({"metas": metas if metas else [{
        "id": "nl-series-cache-missing",
        "type": "series",
        "name": "Geen data (cache ontbreekt)",
        "poster": None,
        "description": "Run de GitHub Action of wacht op de geplande update."
    }]})

@app.route(f"/catalog/movie/{CATALOG_MOVIES_ID}.json")
def catalog_movies():
    metas = load_movies()
    return jsonify({"metas": metas if metas else [{
        "id": "nl-movies-cache-missing",
        "type": "movie",
        "name": "Geen data (cache ontbreekt)",
        "poster": None,
        "description": "Run de GitHub Action of wacht op de geplande update."
    }]})

@app.route("/meta/<content_type>/<item_id>.json")
def meta(content_type, item_id):
    """
    Geef metadata terug uit onze cache zodat Stremio links niet 'leeg' zijn.
    We zoeken eerst exact op id. Als dat niet lukt en het is een tmdb-id,
    proberen we te matchen op het numerieke deel.
    """
    metas = load_series() if content_type == "series" else load_movies()
    # exact id
    for m in metas:
        if m.get("id") == item_id:
            return jsonify({"meta": m})
    # losse tmdb-id match (tmdb:tv:12345 / tmdb:movie:6789)
    if item_id.startswith("tmdb:"):
        needle = item_id.split(":")[-1]
        for m in metas:
            mid = m.get("id", "")
            if mid.startswith("tmdb:") and mid.split(":")[-1] == needle:
                return jsonify({"meta": m})
    # niks gevonden
    return jsonify({"meta": None})

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
