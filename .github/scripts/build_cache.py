import os, io, json, re
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
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30, allow_redirects=True)
            r.raise_for_status()
            t = (r.text or "").strip()
            if t and t.lower() != "null":
                print("[build_cache] CSV OK")
                return t
            raise RuntimeError("CSV leeg of 'null'")
        except Exception as e:
            print(f"[build_cache] CSV failed: {e}")
            last_error = e
    raise RuntimeError(f"CSV niet beschikbaar: {last_error}")

def read_csv_robust(csv_text: str) -> pd.DataFrame:
    """
    Netflix CSV wisselt soms kolomaantallen/quoting. We proberen meerdere parsers.
    """
    # 1) snelste parser
    try:
        return pd.read_csv(io.StringIO(csv_text))
    except Exception as e1:
        print(f"[build_cache] pandas default failed: {e1}")
    # 2) python-engine, toleranter
    try:
        return pd.read_csv(io.StringIO(csv_text), engine="python")
    except Exception as e2:
        print(f"[build_cache] engine=python failed: {e2}")
    # 3) laatste redmiddel: skip slechte regels
    try:
        return pd.read_csv(io.StringIO(csv_text), engine="python", on_bad_lines="skip")
    except Exception as e3:
        print(f"[build_cache] on_bad_lines=skip failed: {e3}")
        raise

def html_fallback_titles(country="netherlands", section="tv"):
    """
    Simpele HTML fallback: pak #1..#10 titels van de Tudum-pagina.
    """
    url = f"https://www.netflix.com/tudum/top10/{country}/{section}"
    print(f"[build_cache] Fallback HTML: {url}")
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 1) Probeer tabelrijen met rangen 1..10 te vinden
    titles = []
    # vind alle tekst, zoek regels met rank en een plausibele titel
    candidates = [el.get_text(" ", strip=True) for el in soup.find_all(["tr","li","p","div","span"])]
    rank_pat = re.compile(r"^\s*(?:#)?([1-9]|10)\b")
    for txt in candidates:
        m = rank_pat.match(txt)
        if not m:
            continue
        # heuristiek: titel is rest van de regel na het rangnummer/symbool
        rest = txt[m.end():].strip(" .:-–—")
        # filter ruis
        if rest and len(rest) > 1 and not rest.isdigit():
            rank = int(m.group(1))
            titles.append((rank, rest))
    # dedup & top10
    seen = set()
    uniq = []
    for rnk, name in sorted(titles, key=lambda x: x[0]):
        key = (rnk, name.lower())
        if rnk <= 10 and key not in seen:
            uniq.append((rnk, name))
            seen.add(key)
        if len(uniq) == 10:
            break
    if not uniq:
        raise RuntimeError("HTML fallback kon geen rijen vinden")
    return uniq

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
    try:
        csv_text = fetch_csv_text_or_raise()
        df = read_csv_robust(csv_text)

        # kolomhelpers
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
        print(f"[build_cache] CSV pad faalde -> HTML fallback: {e}")
        titles_by_rank = html_fallback_titles("netherlands", "tv")

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
