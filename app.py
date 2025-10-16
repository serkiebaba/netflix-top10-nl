import os, re, json, requests
from flask import Flask, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup

APP_NAME = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID = "netflix-top10-nl"
TUDUM = "https://www.netflix.com/tudum/top10/netherlands/tv"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

app = Flask(__name__)
CORS(app)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.netflix.com/",
}

# woorden die we expliciet willen uitsluiten als "titel"
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
    # vermijd te generieke, niet-serietitels
    if re.search(r'^\w{1,2}$', s):
        return False
    return True

def fetch_titles_from_tudum():
    r = requests.get(TUDUM, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    titles = []

    # 1) parse __NEXT_DATA__ JSON (Next.js data)
    tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if tag and tag.string:
        try:
            data = json.loads(tag.string)
            raw = re.findall(r'"title"\s*:\s*"([^"]{2,120})"', json.dumps(data))
            for t in raw:
                t = t.strip()
                if looks_like_title(t) and t not in titles:
                    titles.append(t)
        except Exception:
            pass

    # 2) Fallback: aria-label + headings – met strictere filters
    if len(titles) < 10:
        for el in soup.find_all(["a", "div"], attrs={"aria-label": True}):
            t = el.get("aria-label", "").strip()
            if looks_like_title(t) and t not in titles:
                titles.append(t)

        for h in soup.find_all(["h1", "h2", "h3"]):
            t = h.get_text(strip=True)
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    # 3) laatste fallback: regex direct op HTML
    if len(titles) < 10:
        raw = re.findall(r'"title"\s*:\s*"([^"]{2,120})"', html)
        for t in raw:
            t = t.strip()
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    # Alleen top 10
    return titles[:10]

def tmdb_lookup_tv(title: str):
    """Zoek TMDB tv-id + poster pad voor een titel. Geeft (id, poster_path) of (None, None)."""
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

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": CATALOG_ID,
        "version": "1.1.0",
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
    try:
        titles = fetch_titles_from_tudum()
    except Exception:
        titles = []

    metas = []
    if titles:
        for i, name in enumerate(titles, start=1):
            tmdb_id, poster_path = tmdb_lookup_tv(name)
            if tmdb_id:
                metas.append({
                    "id": f"tmdb:tv:{tmdb_id}",
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

    return jsonify({"metas": metas})

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
