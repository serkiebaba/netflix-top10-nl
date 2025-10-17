import os, io, json
import requests
import pandas as pd

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
OUT_PATH = os.path.join("cache", "netflix_nl_series.json")

# Officiële datasets (moeten op 'top10.netflix.com' blijven)
CSV_URLS = [
    "https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://top10.netflix.com/data/AllWeeklyTop10.csv",
]

# Minimalistische headers; GEEN 'Referer' meer (die triggert soms een redirect naar tudum)
HEADERS_PRIMARY = {
    "User-Agent": "Mozilla/5.0"
}
HEADERS_SECONDARY = {}  # fallback: helemaal kaal

def fetch_csv_once(url: str, headers: dict) -> str:
    """
    Fetch zonder (foute) cross-domain redirect.
    - Volg alleen redirects die op top10.netflix.com blijven.
    - Als redirect naar 'www.netflix.com/tudum/...' => behandel als fout.
    """
    with requests.Session() as s:
        r = s.get(url, headers=headers, timeout=30, allow_redirects=False)
        # Handmatig redirect afhandelen (alleen als het op hetzelfde domein blijft)
        if r.is_redirect or r.is_permanent_redirect:
            loc = r.headers.get("Location", "")
            # Normaliseer
            loc_low = loc.lower()
            if loc and loc_low.startswith("https://top10.netflix.com/"):
                r = s.get(loc, headers=headers, timeout=30, allow_redirects=False)
            else:
                # Redirect naar ander domein (bijv. www.netflix.com/tudum/...) => forceer fout
                raise RuntimeError(f"Unexpected redirect to different host: {loc}")

        r.raise_for_status()
        text = (r.text or "").strip()
        if not text or text.lower() == "null":
            raise RuntimeError("CSV body is empty or 'null'")
        return text

def fetch_csv() -> str:
    last_error = None
    for url in CSV_URLS:
        # 1) poging met minimal headers (zonder referer)
        try:
            print(f"[build_cache] Fetching CSV (primary): {url}")
            return fetch_csv_once(url, HEADERS_PRIMARY)
        except Exception as e1:
            print(f"[build_cache] Primary failed: {e1}")

        # 2) fallback-headers (helemaal kaal)
        try:
            print(f"[build_cache] Fetching CSV (secondary): {url}")
            return fetch_csv_once(url, HEADERS_SECONDARY)
        except Exception as e2:
            print(f"[build_cache] Secondary failed: {e2}")
            last_error = e2
    raise RuntimeError(f"Kon CSV niet ophalen. Laatste fout: {last_error}")

def tmdb_tv_lookup(title):
    if not TMDB_API_KEY:
        return None, None
    try:
        resp = requests.get(
            "https://api.themoviedb.org/3/search/tv",
            params={
                "api_key": TMDB_API_KEY,
                "query": title,
                "include_adult": "false",
                "language": "nl-NL",
                "page": 1
            },
            timeout=15
        )
        resp.raise_for_status()
        j = resp.json()
        if j.get("results"):
            m = j["results"][0]
            return m.get("id"), m.get("poster_path")
    except Exception:
        pass
    return None, None

def main():
    csv_text = fetch_csv()
    df = pd.read_csv(io.StringIO(csv_text))

    # kolomhelpers (case-insensitive + fallbacks)
    cn = [c.lower() for c in df.columns]
    def col(name, fallbacks=()):
        for i, c in enumerate(cn):
            if c == name.lower():
                return df.columns[i]
        for fb in fallbacks:
            for i, c in enumerate(cn):
                if c == fb.lower():
                    return df.columns[i]
        raise KeyError(f"Kolom {name} niet gevonden (beschikbaar: {df.columns.tolist()})")

    country_col   = col("country_name", ("country",))
    rank_col      = col("weekly_rank", ("rank",))
    category_col  = col("category",)
    title_col     = col("show_title", ("title","show_title_name","series_title"))

    # Alleen Netherlands + TV
    dfnl = df[(df[country_col] == "Netherlands") & (df[category_col].str.upper().str.contains("TV"))]

    # Pak laatste week wanneer aanwezig
    week_col = None
    for wk in ["week", "week_end", "week_start", "week_ended_on"]:
        try:
            week_col = col(wk)
            break
        except KeyError:
            continue
    if week_col:
        latest_week = dfnl[week_col].max()
        dfnl = dfnl[dfnl[week_col] == latest_week]

    # Top 10
    dfnl = dfnl.sort_values(by=rank_col).head(10)

    metas = []
    for _, row in dfnl.iterrows():
        name = str(row[title_col]).strip()
        rank = int(row[rank_col])
        tmdb_id, poster_path = tmdb_tv_lookup(name)
        if tmdb_id:
            metas.append({
                "id": f"tmdb:tv:{tmdb_id}",
                "type": "series",
                "name": name,
                "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
                "description": f"Netflix NL Top 10 – positie #{rank}"
            })
        else:
            metas.append({
                "id": f"netflix-fallback-{rank}",
                "type": "series",
                "name": name,
                "poster": None,
                "description": f"Netflix NL Top 10 – positie #{rank} (TMDB match niet gevonden)"
            })

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"metas": metas}, f, ensure_ascii=False, indent=2)
    print(f"Geschreven: {OUT_PATH} ({len(metas)} items)")

if __name__ == "__main__":
    main()
