import os, re, json, requests
from flask import Flask, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup

# ---------- Config ----------
APP_NAME = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID = "netflix-top10-nl"
TUDUM_URL = "https://www.netflix.com/tudum/top10/netherlands/tv"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

app = Flask(__name__)
CORS(app)

# Beetje “echte browser” headers – Tudum is hier minder kieskeurig dan CSV
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.netflix.com/",
}

# woorden die we expliciet willen uitsluiten als “titel”
BAD_SUBSTR = (
    "sign in","log in","n/a","tudum","netflix","top 10",
    "week","netherlands","nl","tv","series","clip","watch",
    "popular","help","privacy","cookie","methodology"
)

def looks_like_title(s: str) -> bool:
    s = re.sub(r"\s+", " ", s or "").strip()
    if not (2 <= len(s) <= 140):
        return False
    low = s.lower()
    if any(b in low for b in BAD_SUBSTR):
        return False
    # te korte ‘woorden’ of rommel vermijden
    if re.fullmatch(r"[A-Za-z]{1,2}", s):
        return False
    return True

def clean_title(s: str) -> str:
    """Verwijder staarten zoals ': Season 1', '(Limited Series)', etc."""
    s = re.sub(r"\s*\(.*?Limited Series.*?\)", "", s, flags=re.I)
    s = re.sub(r"\s*:\s*Season\s*\d+\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*Season\s*\d+\s*$", "", s, flags=re.I)
    return s.strip(" –-")

def fetch_tudum_titles():
    """Haal de HTML op en parseer maximaal Top 10 titels."""
    r = requests.get(TUDUM_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    titles = []

    # 1) Probeer __NEXT_DATA__ json: pluk alles wat op een titel lijkt
    nd = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if nd and nd.string:
        try:
            data = json.loads(nd.string)
            raw = re.findall(r'"title"\s*:\s*"([^"]{2,140})"', json.dumps(data))
            for t in raw:
                t2 = clean_title(t)
                if looks_like_title(t2) and t2 not in titles:
                    titles.append(t2)
        except Exception:
            pass

    # 2) Fallback: aria-label / headings met strictere filter
    if len(titles) < 10:
        for el in soup.find_all(["a","div"], attrs={"aria-label": True}):
            t = clean_title(el.get("aria-label",""))
            if looks_like_title(t) and t not in titles:
                titles.append(t)

        for h in soup.find_all(["h1","h2","h3"]):
            t = clean_title(h.get_text(strip=True))
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    # 3) Laatste fallback: regex in de html
    if len(titles) < 10:
        raw = re.findall(r'"title"\s*:\s*"([^"]{2,140})"', html)
        for t in raw:
            t2 = clean_title(t)
            if looks_like_title(t2) and t2 not in titles:
                titles.append(t2)

    # Alleen top 10
    return titles[:10]

# ---------- TMDB helpers ----------
def tmdb_search_tv(title: str):
    """Zoek TMDB tv-id + poster path via /search/tv."""
    if not TMDB_API_KEY:
        return None, None
    url = "https://api.themoviedb.org/3/search/tv"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "include_adult": "false",
        "language": "en-US",
        "page": 1
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

def tmdb_get_imdb_id(tv_id: int):
    """Vraag TMDB om imdb_id voor die tv-serie (via /tv/{id}/external_ids)."""
    if not TMDB_API_KEY or not tv_id:
        return None
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/external_ids"
    params = {"api_key": TMDB_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=12)
        resp.raise_for_status()
        j = resp.json()
        imdb = j.get("imdb_id")
        if imdb and imdb.startswith("tt"):
            return imdb
    except Exception:
        pass
    return None

# ---------- Stremio API ----------
@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": CATALOG_ID,
        "version": "1.2.0",
        "name": APP_NAME,
        "description": "Live Netflix Top 10 NL (Series) – rechtstreeks uit Tudum; TMDB→IMDb koppeling.",
        "resources": ["catalog"],
        "types": [CATALOG_TYPE],
        "catalogs": [
            {"type": CATALOG_TYPE, "id": CATALOG_ID, "name": APP_NAME}
        ]
    })

@app.route(f"/catalog/{CATALOG_TYPE}/{CATALOG_ID}.json")
def catalog():
    try:
        titles = fetch_tudum_titles()
    except Exception as e:
        return jsonify({"metas":[{
            "id": "netflix-nl-update-required",
            "type": CATALOG_TYPE,
            "name": "Update required",
            "poster": None,
            "description": f"Kon Tudum niet parsen: {e}"
        }]}), 200

    metas = []
    if titles:
        for i, name in enumerate(titles, start=1):
            tmdb_id, poster_path = tmdb_search_tv(name)
            imdb_id = tmdb_get_imdb_id(tmdb_id) if tmdb_id else None

            # Gebruik IMDb-id als primaire ID (beste compatibiliteit in Stremio).
            if imdb_id:
                meta_id = imdb_id
            elif tmdb_id:
                meta_id = f"tmdb:tv:{tmdb_id}"
            else:
                # fallback – klikbaar maar zonder rijke meta/streams
                slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                meta_id = f"netflix-fallback:{slug}"

            metas.append({
                "id": meta_id,
                "type": CATALOG_TYPE,
                "name": name,
                "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
                "description": f"Netflix NL Top 10 – positie #{i}"
            })
    else:
        metas = [{
            "id": "netflix-nl-no-data",
            "type": CATALOG_TYPE,
            "name": "Geen titels gevonden",
            "poster": None,
            "description": "Tudum gaf geen bruikbare lijst terug."
        }]

    return jsonify({"metas": metas})

# ---------- Debug ----------
@app.route("/__debug")
def __debug():
    out = {"ok": False, "titles": []}
    try:
        titles = fetch_tudum_titles()
        out["ok"] = True
        out["titles"] = titles
    except Exception as e:
        out["error"] = str(e)
    return jsonify(out)

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json", "debug": "/__debug"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
