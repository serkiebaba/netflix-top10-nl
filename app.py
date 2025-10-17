# app.py
import os, json
from flask import Flask, jsonify
from flask_cors import CORS

APP_NAME = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID = "netflix-top10-nl"

app = Flask(__name__)
CORS(app)

CACHE_PATH = os.path.join(os.path.dirname(__file__), "cache", "netflix_nl_series.json")

def load_cached_metas():
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "metas" in data:
            return data["metas"]
    except Exception:
        pass
    return [{
        "id": "netflix-nl-cache-missing",
        "type": CATALOG_TYPE,
        "name": "Geen data (cache ontbreekt)",
        "poster": None,
        "description": "De cache is nog niet opgebouwd. Run de GitHub Action of wacht op de geplande update."
    }]

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": CATALOG_ID,
        "version": "1.2.0",
        "name": APP_NAME,
        "description": "Netflix NL Top 10 (Series) uit dagelijkse cache (GitHub Actions).",
        "resources": ["catalog"],
        "types": [CATALOG_TYPE],
        "catalogs": [
            {"type": CATALOG_TYPE, "id": CATALOG_ID, "name": APP_NAME}
        ]
    })

@app.route(f"/catalog/{CATALOG_TYPE}/{CATALOG_ID}.json")
def catalog():
    return jsonify({"metas": load_cached_metas()})

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
