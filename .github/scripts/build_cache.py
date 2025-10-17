import os, csv, io, json, time
import requests
import pandas as pd

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
OUT_PATH = os.path.join("cache", "netflix_nl_series.json")

CSV_URLS = [
    "https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://top10.netflix.com/data/AllWeeklyTop10.csv"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://top10.netflix.com/"
}

def fetch_csv():
    last_error = None
    for url in CSV_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            if r.text and r.text.strip().lower() != "null":
                return r.text
        except Exception as e:
            last_error = e
    raise RuntimeError(f"Kon CSV niet ophalen. Laatste fout: {last_error}")

def tmdb_tv_lookup(title):
    if not TMDB_API_KEY:
        return None, None
    try:
        resp = requests.get(
            "https://api.themoviedb.org/3/search/tv",
            params={"api_key": TMDB_API_KEY, "query": title, "include_adult": "false", "language": "en-US", "page": 1},
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

    dfnl = df[(df[country_col] == "Netherlands") & (df[category_col].str.upper().str.contains("TV"))]

    if "week" in [x.lower() for x in df.columns]:
        wcol = df.columns[[x.lower()=="week" for x in df.columns].index(True)]
        latest_week = dfnl[wcol].max()
        dfnl = dfnl[dfnl[wcol] == latest_week]

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
