import os, io, json
import requests
import pandas as pd

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
OUT_PATH = os.path.join("cache", "netflix_nl_series.json")

# 1) Officiële CSV (kan op Actions-IPs soms redirecten/blokkeren)
PRIMARY_CSV = [
    "https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://top10.netflix.com/data/AllWeeklyTop10.csv",
]

# 2) Read-proxy mirrors (read-only fetchers) – vaak wél stabiel vanaf CI
#   NB: dit zijn publieke, gratis read-proxies. Voor persoonlijk gebruik prima.
PROXY_CSV = [
    "https://r.jina.ai/http://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://r.jina.ai/http://top10.netflix.com/data/AllWeeklyTop10.csv",
    "https://r.jina.ai/https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://r.jina.ai/https://top10.netflix.com/data/AllWeeklyTop10.csv",
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def fetch_text(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"}, timeout=40, allow_redirects=True)
    r.raise_for_status()
    t = (r.text or "").strip()
    if not t or t.lower() == "null":
        raise RuntimeError("empty body")
    # simpele sanity check: CSV moet minstens deze kop bevatten
    if "," not in t.splitlines()[0]:
        raise RuntimeError("not a CSV (first line has no commas)")
    return t

def fetch_csv_text() -> str:
    last = None
    # 1) probeer direct bij Netflix
    for u in PRIMARY_CSV:
        try:
            print(f"[build_cache] CSV try (primary): {u}")
            return fetch_text(u)
        except Exception as e:
            print(f"[build_cache] primary failed: {e}")
            last = e
    # 2) probeer via read-proxy (meestal succes)
    for u in PROXY_CSV:
        try:
            print(f"[build_cache] CSV try (proxy): {u}")
            return fetch_text(u)
        except Exception as e:
            print(f"[build_cache] proxy failed: {e}")
            last = e
    raise RuntimeError(f"Kon CSV niet ophalen: {last}")

def read_csv_robust(csv_text: str) -> pd.DataFrame:
    # meerdere parser-strategieën i.v.m. wisselende quoting/velden
    for kwargs in (
        {},
        {"engine": "python"},
        {"engine": "python", "on_bad_lines": "skip"},
    ):
        try:
            return pd.read_csv(io.StringIO(csv_text), **kwargs)
        except Exception as e:
            print(f"[build_cache] read_csv failed with {kwargs or 'default'}: {e}")
    raise RuntimeError("pandas kon CSV niet parsen")

def tmdb_tv_lookup(title: str):
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
            timeout=20
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
    csv_text = fetch_csv_text()
    df = read_csv_robust(csv_text)

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

    # Filter Netherlands + TV
    dfnl = df[(df[country_col] == "Netherlands") & (df[category_col].astype(str).str.upper().str.contains("TV"))]

    # Laatste week wanneer beschikbaar
    week_col = None
    for wk in ["week", "week_end", "week_start", "week_ended_on"]:
        try:
            week_col = col(wk)
            break
        except KeyError:
            continue
    if week_col is not None and week_col in dfnl.columns:
        latest_week = dfnl[week_col].max()
        dfnl = dfnl[dfnl[week_col] == latest_week]

    dfnl = dfnl.sort_values(by=rank_col, ascending=True).head(10)

    metas = []
    for _, row in dfnl.iterrows():
        name = str(row[title_col]).strip()
        try:
            rank = int(row[rank_col])
        except Exception:
            # fallback als rank geen int is
            try:
                rank = int(float(row[rank_col]))
            except Exception:
                rank = len(metas) + 1
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
