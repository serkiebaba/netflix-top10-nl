import re, json, time, requests
from flask import Flask, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup

APP_NAME = "NETFLIX TOP 10 (NL) – Series"
CATALOG_TYPE = "series"
CATALOG_ID = "netflix-top10-nl"
TUDUM = "https://www.netflix.com/tudum/top10/netherlands/tv"

app = Flask(__name__)
CORS(app)

def _ua():
    return {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def fetch_titles():
    r = requests.get(TUDUM, headers=_ua(), timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    titles = []
    tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if tag and tag.string:
        try:
            data = json.loads(tag.string)
            raw = re.findall(r'"title"\s*:\s*"([^"]{2,100})"', json.dumps(data).lower())
            for t in raw:
                t = t.strip().title()
                if len(t)>2 and t not in titles:
                    titles.append(t)
        except:
            pass

    if not titles:
        for h in soup.find_all(["h2","h3"]):
            t = h.get_text(strip=True)
            if len(t)>2 and t not in titles:
                titles.append(t)

    return titles[:10]

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "id": "netflix-top10-nl",
        "version": "1.0.0",
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
    titles = fetch_titles()
    metas = []
    for i, name in enumerate(titles, start=1):
        metas.append({
            "id": f"netflix-nl-{i}-{re.sub(r'[^a-z0-9]+','-', name.lower())}",
            "type": CATALOG_TYPE,
            "name": name,
            "poster": None,
            "description": f"Netflix NL Top 10 – positie #{i}"
        })
    return jsonify({"metas": metas})

@app.route("/")
def root():
    return jsonify({"ok": True, "manifest": "/manifest.json"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
