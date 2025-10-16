import os, re, json, time, requests
from flask import Flask, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup

APP_NAME     = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID   = "netflix-top10-nl"
TUDUM        = "https://www.netflix.com/tudum/top10/netherlands/tv"

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

app = Flask(__name__)
CORS(app)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.netflix.com/",
}

BAD_SUBSTR = (
    "sign in", "log in", "n/a", "tudum", "netflix", "top 10",
    "week", "netherlands", "nl", "tv", "series", "clip", "watch",
    "popular", "help", "privacy", "cookie"
)

def looks_like_title(s: str) -> bool:
    s = s.strip()
    if len(s) < 2 or len(s) > 120:
        return False
    low = s.lower()
    if any(b in low for b in BAD_SUBSTR):
        return False
    if re.search(r'^\w{1,2}$', s):
        return False
    return True

def normalize_title(t: str) -> str:
    """Strip 'Season X', 'Limited Series', trailing punctuation, etc."""
    t = re.sub(r':\s*Season\s*\d+\s*$', '', t, flags=re.I)
    t = re.sub(r'\s*\(Season\s*\d+\)\s*$', '', t, flags=re.I)
    t = re.sub(r'\s*Limited Series\s*$', '', t, flags=re.I)
    return t.strip(" -–:;")

def fetch_titles_from_tudum():
    r = requests.get(TUDUM, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    titles = []

    # 1) Parse __NEXT_DATA__
    tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if tag and tag.string:
        try:
            data = json.loads(tag.string)
            raw = re.findall(r'"title"\s*:\s*"([^"]{2,120})"', json.dumps(data))
            for t in raw:
                t = normalize_title(t.strip())
                if looks_like_title(t) and t not in titles:
                    titles.append(t)
        except Exception:
            pass

    # 2) Fallbacks
    if len(titles) < 10:
        for el in soup.find_all(["a", "div"], attrs={"aria-label": True}):
            t = normalize_title(el.get("aria-label", "").strip())
            if looks_like_title(t) and t not in titles:
                titles.append(t)

        for h in soup.find_all(["h1", "h2", "h3"]):
            t = normalize_title(h.get_text(strip=True))
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    if len(titles) < 10:
        raw = re.findall(r'"title"\s*:\s*"([^"]{2,120})"', html)
        for t in raw:
            t = normalize_title(t.strip())
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    return titles[:10]

def tmdb_lookup_tv(title: str):
    """Return (tmdb_id, poster_path) or (None, None)"""
    if not TMDB_API_KEY:
        return None, None
    url = "https://api.themoviedb.org/3/search/tv"
    params = {"api_key": TMDB_API_KEY, "query": title, "include_adult": "false", "language": "en-US", "page": 1}
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

# --- tiny cache so we don't hammer TMDB every time ---
_cache = {"at": 0, "metas": []}
TTL = 15 * 60  # 15 minutes

def build_metas():
    titles = []
    try:
        titles = fetch_titles_from_tudum()
    except Exception:
        titles = []

    metas = []
    if titles:
        for i, raw in enumerate(titles, start=1):
            name = normalize_title(raw)
            tmdb_id, poster_path = tmdb_lookup_tv(name)
            if tmdb_id:
                metas.append({
                    "id": f"tmdb:show:{tmdb_id}",   # <-- IMPORTANT FOR STREMIO
                    "type": CATALOG_TYPE,
                    "name": name,
                    "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
                    "description": f"Netflix NL Top 10 – positie #{i}"
                })
            else:
                metas.append({
                    "id": f"netflix-fallback-{i}-{re.sub(r'[^a-z0-9]+','-', name.lower())}",
                    "type": CATALOG_TYPE,
                    "name": name,
                    "poster": None,
                    "description": f"Netflix NL Top 10 – positie #{i} (TMDB match niet gevonden)"
                })
    else:
        metas = [{
            "id": "netflix-nl-1-update-required",
            "type": CATALOG_TYPE,
            "name": "Update required",
            "poster": None,
            "description": "Kon geen titels parsen – Netflix structuur/headers gewijzigd?"
        }]

    return metas

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": CATALOG_ID,
        "version": "1.1.1",
        "name": APP_NAME,
        "description": "Live Netflix Top 10 NL (Series) rechtstreeks van Tudum.",
        "resources": ["catalog"],
        "types": [CATALOG_TYPE],
        "catalogs": [
            {"type": CATALOG_TYPE, "id": CATALOG_ID, "name": APP_NAME}
        ]
    })

@app.route(f"/catalog/{CATALOG_TYPE}/{CATALOG_ID}.json")
def catalog():
    now = time.time()
    if now - _cache["at"] > TTL or not _cache["metas"]:
        _cache["metas"] = build_metas()
        _cache["at"] = now
    return jsonify({"metas": _cache["metas"]})

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
