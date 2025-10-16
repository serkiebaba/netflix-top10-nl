import re, json, requests
from flask import Flask, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup

APP_NAME = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID = "netflix-top10-nl"
TUDUM = "https://www.netflix.com/tudum/top10/netherlands/tv"

app = Flask(__name__)
CORS(app)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.netflix.com/",
}

BAD_SUBSTR = (
    "tudum", "netflix", "top 10", "week", "netherlands", "nl", "tv", "series",
    "popular", "most watched", "copyright", "privacy", "help"
)

def looks_like_title(s: str) -> bool:
    s = s.strip()
    if len(s) < 2 or len(s) > 120:
        return False
    low = s.lower()
    if any(b in low for b in BAD_SUBSTR):
        return False
    # vermijd zinnen met teveel spaties / geen hoofdletter-achtig patroon
    return True

def fetch_titles():
    r = requests.get(TUDUM, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    titles = []

    # 1) Probeer __NEXT_DATA__ JSON
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

    # 2) Fallback: zoek headings en aria-labels
    if len(titles) < 10:
        for el in soup.find_all(["a", "div"], attrs={"aria-label": True}):
            t = el.get("aria-label", "").strip()
            if looks_like_title(t) and t not in titles:
                titles.append(t)

        for h in soup.find_all(["h1", "h2", "h3"]):
            t = h.get_text(strip=True)
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    # 3) Fallback: brute regex op HTML
    if len(titles) < 10:
        raw = re.findall(r'"title"\s*:\s*"([^"]{2,120})"', html)
        for t in raw:
            t = t.strip()
            if looks_like_title(t) and t not in titles:
                titles.append(t)

    # Top-10 teruggeven
    return titles[:10]

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": "netflix-top10-nl",
        "version": "1.0.1",
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
        titles = fetch_titles()
    except Exception:
        titles = []

    metas = []
    if titles:
        for i, name in enumerate(titles, start=1):
            metas.append({
                "id": f"netflix-nl-{i}-{re.sub(r'[^a-z0-9]+','-', name.lower())}",
                "type": CATVLOG_TYPE if False else CATALOG_TYPE,  # keep type correct
                "name": name,
                "poster": None,  # TMDB metadata addon vult posters/plot aan
                "description": f"Netflix NL Top 10 – positie #{i}"
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
    app.run(host="0.0.0.0", port=8000)
