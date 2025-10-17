import os
import re
import io
import csv
import json
import requests
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# ====== App / Catalog settings ======
APP_NAME     = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID   = "netflix-top10-nl"

# Netflix CSV endpoints (Netflix wisselt soms; we proberen beide)
DATA_URLS = [
    "https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://www.netflix.com/tudum/top10/data/AllWeeklyTop10ByCountry.csv",
]

# Requests headers om blokkades te voorkomen
REQ_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept": "text/csv, text/plain, */*",
    "Referer": "https://top10.netflix.com/",
}

# TMDB key uit Render env (Settings → Environment → TMDB_API_KEY)
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

app = Flask(__name__)
CORS(app)


# ---------- Netflix CSV ophalen (met fallback en safeguards) ----------
def _get_csv_text():
    """Haal CSV-tekst op, probeer beide URL's, laat redirects toe, filter 'null'."""
    last_err = None
    for url in DATA_URLS:
        try:
            r = requests.get(url, headers=REQ_HEADERS, timeout=30, allow_redirects=True)
            txt = r.content.decode("utf-8", errors="ignore").strip()
            # sommige CDN's geven 200 + 'null' of lege body → ongeldig
            if r.status_code == 200 and txt and txt.lower() != "null" and "," in txt:
                return txt
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Kon CSV niet ophalen. Laatste fout: {last_err}")


def fetch_latest_nl_tv_top10():
    """
    Parse de CSV en geef de Top 10 voor de nieuwste week (country: Netherlands, category bevat 'TV').
    Resultaat: list[ {rank, show_title, season_title, week} ]
    """
    csv_text = _get_csv_text()
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)

    rows = []
    for row in reader:
        if (row.get("country_name") or "") != "Netherlands":
            continue
        if "tv" not in (row.get("category") or "").lower():
            continue
        try:
            row["rank"] = int(row.get("rank", 9999))
        except Exception:
            row["rank"] = 9999
        rows.append(row)

    if not rows:
        return []

    def parse_week(w):
        try:
            return datetime.strptime(w, "%Y-%m-%d")
        except Exception:
            return datetime.min

    latest_week = max(rows, key=lambda x: parse_week(x.get("week", ""))).get("week")
    week_rows = [r for r in rows if r.get("week") == latest_week]
    week_rows.sort(key=lambda x: x["rank"])
    week_rows = week_rows[:10]

    result = []
    for r in week_rows:
        result.append({
            "rank": r["rank"],
            "show_title": (r.get("show_title") or "").strip(),
            "season_title": (r.get("season_title") or "").strip(),
            "week": latest_week
        })
    return result


# ---------- TMDB lookup ----------
def tmdb_lookup_tv(title: str):
    """
    Zoek TMDB tv-id en poster_path (en-US). Return (id, poster_path) of (None, None).
    Zonder TMDB_API_KEY: (None, None).
    """
    if not TMDB_API_KEY:
        return None, None

    url = "https://api.themoviedb.org/3/search/tv"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "include_adult": "false",
        "language": "en-US",
        "page": 1,
    }
    try:
        resp = requests.get(url, params=params, timeout=12)
        resp.raise_for_status()
        j = resp.json()
        if j.get("results"):
            m = j["results"][0]
            return m.get("id"), m.get("poster_path")
    except Exception:
        pass
    return None, None


# ---------- Routes ----------
@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": CATALOG_ID,
        "version": "1.2.0",
        "name": APP_NAME,
        "description": "Live Netflix Top 10 NL (Series) vanaf de officiële CSV, met TMDB-IDs.",
        "resources": ["catalog"],
        "types": [CATALOG_TYPE],
        "catalogs": [
            {"type": CATALOG_TYPE, "id": CATALOG_ID, "name": APP_NAME}
        ]
    })


@app.route(f"/catalog/{CATALOG_TYPE}/{CATALOG_ID}.json")
def catalog():
    try:
        top10 = fetch_latest_nl_tv_top10()
    except Exception as e:
        # veilige fallback zodat Stremio niet crasht
        return jsonify({"metas": [{
            "id": "netflix-nl-update-required",
            "type": CATALOG_TYPE,
            "name": "Update required",
            "poster": None,
            "description": f"Kon Top 10 niet ophalen: {e}"
        }]}), 200

    metas = []
    for item in top10:
        name = item["show_title"] or item["season_title"] or "Onbekend"
        # TMDB-lookup om echte tmdb:tv:<id> te krijgen
        tmdb_id, poster_path = tmdb_lookup_tv(name)

        if tmdb_id:
            metas.append({
                "id": f"tmdb:tv:{tmdb_id}",
                "type": CATALOG_TYPE,
                "name": name,
                "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
                "description": f"Netflix NL Top 10 – positie #{item['rank']} (week {item['week']})"
            })
        else:
            # fallback (geen TMDB-match): nog steeds tonen, maar zonder streams
            safe_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
            metas.append({
                "id": f"netflix-fallback-{item['rank']}-{safe_slug}",
                "type": CATALOG_TYPE,
                "name": name,
                "poster": None,
                "description": (f"Netflix NL Top 10 – positie #{item['rank']} "
                                f"(week {item['week']}) – TMDB match niet gevonden")
            })

    return jsonify({"metas": metas})


@app.route("/")
def root():
    return jsonify({
        "ok": True,
        "manifest": "/manifest.json",
        "catalog": f"/catalog/{CATALOG_TYPE}/{CATALOG_ID}.json"
    })


# Debug endpoint – laat de eerste CSV-regels zien (handig om te checken dat Render 'm echt binnenkrijgt)
@app.route("/__csvdebug")
def csvdebug():
    try:
        txt = _get_csv_text()
        head = "\n".join(txt.splitlines()[:6])
        return {"ok": True, "first_lines": head}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
