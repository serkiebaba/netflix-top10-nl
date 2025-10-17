import os, io, json
import requests
import pandas as pd
from bs4 import BeautifulSoup

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
OUT_PATH = os.path.join("cache", "netflix_nl_series.json")

CSV_URLS = [
    "https://top10.netflix.com/data/AllWeeklyTop10ByCountry.csv",
    "https://top10.netflix.com/data/AllWeeklyTop10.csv",
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def fetch_csv_text_or_raise():
    last_error = None
    for url in CSV_URLS:
        try:
            print(f"[build_cache] Try CSV: {url}")
            # GEEN referer; laat redirects toe (sommige IP's vereisen het), we accepteren 200 of 3xx->200
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30, allow_redirects=True)
            r.raise_for_status()
            t = (r.text or "").strip()
            if t and t.lower() != "null":
                print("[build_cache] CSV ok")
                return t
            raise RuntimeError("CSV leeg of 'null'")
        except Exception as e:
            print(f"[build_cache] CSV failed: {e}")
            last_error = e
    raise RuntimeError(f"CSV niet beschikbaar: {last_error}")

def fetch_html_table_titles(country="netherlands", section="tv"):
    """
    Fallback: haal de NL Top10 HTML op en parse de tabel.
    URL-voorbeeld: https://www.netflix.com/tudum/top10/netherlands/tv
    We extraheren de titels en ranks (top 10).
    """
    url = f"https://www.netflix.com/tudum/top10/{country}/{section}"
    print(f"[build_cache] Fallback HTML: {url}")
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    html = r.text

    # 1) Probeer met pandas.read_html (werkt vaak direct):
    try:
        tables = pd.read_html(html, flavor="bs4")  # vereist lxml + bs4
        # Zoek tabel met 'Rank' kolom
        target = None
        for tb in tables:
            cols = [str(c).strip().lower() for c in tb.columns]
            if any("rank" in c for c in cols):
                target = tb
                break
        if target is None:
            raise RuntimeError("Geen tabel met 'Rank' kolom gevonden")
        # Normaliseer kolomnamen
        target.columns = [str(c).strip().lower() for c in target.columns]
        # Vind kolommen
        rank_col = next((c for c in target.columns if "rank" in c), None)
        title_col = next((c for c in target.columns if "title" in c or "show" in c or "series" in c or "program" in c), None)
        if title_col is None:
            # fallback: soms staat de titel als index/kolom-0
            title_col = target.columns[0]
        # Sorteer en pak top 10
        df10 = target.sort_values(by=rank_col).head(10) if rank_col else target.head(10)
        titles = [str(x).strip() for x in df10[title_col].tolist()]
        ranks  = list(range(1, len(titles)+1)) if rank_col is None else [int(v) for v in df10[rank_col].tolist()][:len(titles)]
        if not titles:
            raise RuntimeError("Geen titels uit tabel kunnen halen")
        return list(zip(ranks, titles))
    except Exception as e:
        print(f"[build_cache] read_html faalde: {e}")

    # 2) Als read_html niet lukt, brute-force met BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    # Zoek alle <table> en probeer rijen te lezen
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            texts = [td.get_text(strip=True) for td in tds]
            if not texts:
                continue
            # Heuristiek: regel bevat rank + titel
            # rank is cijfer 1..10
            try:
                possible_rank = int(texts[0])
                # titel is ergens in de rest
                title = None
                for tx in texts[1:]:
                    if len(tx) > 1 and not tx.isdigit():
                        title = tx
                        break
                if possible_rank in range(1, 11) and title:
                    rows.append((possible_rank, title))
            except:
                continue
    rows = sorted(rows, key=lambda x: x[0])[:10]
    if not rows:
        raise RuntimeError("HTML fallback kon geen rijen vinden")
    return rows

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
    titles_by_rank = None

    # 1) Probeer CSV
    try:
        csv_text = fetch_csv_text_or_raise()
        df = pd.read_csv(io.StringIO(csv_text))
        # Case-insensitive kolomhelpers
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

        # Laatste week wanneer beschikbaar
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

        dfnl = dfnl.sort_values(by=rank_col).head(10)
        titles_by_rank = [(int(r[rank_col]), str(r[title_col]).strip()) for _, r in dfnl.iterrows()]

    except Exception as e:
        print(f"[build_cache] CSV path failed, fallback to HTML: {e}")
        # 2) Fallback: HTML tabel NL/TV
        titles_by_rank = fetch_html_table_titles("netherlands", "tv")

    # Bouw metas
    metas = []
    for rank, name in titles_by_rank:
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
