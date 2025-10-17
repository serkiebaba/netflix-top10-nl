import os, io, json, datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# output-bestanden
OUT_SERIES = os.path.join("cache", "netflix_nl_series.json")   # blijft je huidige Stremio dataset
OUT_MOVIES = os.path.join("cache", "netflix_nl_movies.json")   # extra (optioneel gebruiken in Stremio)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
BASE = "https://flixpatrol.com/top10/streaming/netherlands/{date}/"

def date_candidates(days_back=7):
    today = datetime.date.today()
    for d in range(days_back + 1):
        yield (today - datetime.timedelta(days=d)).strftime("%Y-%m-%d")

def fetch_first_available_html():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,*/*"})
    last_err = None
    for ds in date_candidates(7):
        url = BASE.format(date=ds)
        try:
            print(f"[flix] try {url}")
            r = s.get(url, timeout=30)
            if r.status_code == 404:
                last_err = "404"
                continue
            r.raise_for_status()
            html = r.text
            # sanity: pagina moet "Netflix TOP 10 in the Netherlands" of "TOP 10" bevatten
            if "Netflix TOP 10 in the Netherlands" not in html and "TOP 10 Movies" not in html:
                last_err = "unexpected page content"
                continue
            return ds, html
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(f"Kon geen bruikbare FlixPatrol NL pagina vinden: {last_err}")

def find_netflix_section(html):
    soup = BeautifulSoup(html, "html.parser")

    # 1) Zoek heading met "Netflix TOP 10 in the Netherlands"
    target_h = None
    for h in soup.find_all(["h1", "h2", "h3"]):
        t = (h.get_text(" ", strip=True) or "").lower()
        if t.startswith("netflix top 10 in the netherlands"):
            target_h = h
            break
    if not target_h:
        # fallback: eerste blok op de pagina is meestal Netflix; neem eerste heading met "top 10" + "netherlands"
        for h in soup.find_all(["h1", "h2", "h3"]):
            t = (h.get_text(" ", strip=True) or "").lower()
            if "top 10" in t and "netherlands" in t and "netflix" in t:
                target_h = h
                break
    if not target_h:
        raise RuntimeError("Netflix heading niet gevonden")

    # 2) Verzamel siblings tot aan de volgende provider-heading (HBO/Disney/Prime/etc.)
    block_nodes = []
    for sib in target_h.next_siblings:
        if getattr(sib, "name", None) in ("h1", "h2", "h3"):
            txt = (sib.get_text(" ", strip=True) or "").lower()
            # bij de volgende provider stoppen we
            if " top 10 in the netherlands" in txt and "netflix" not in txt:
                break
        block_nodes.append(sib)

    # 3) In deze blok nodes zoeken we twee tabellen: "TOP 10 Movies" en "TOP 10 TV Shows"
    block_html = "".join(str(x) for x in block_nodes)
    if not block_html.strip():
        raise RuntimeError("Netflix-blok is leeg")

    return block_html

def table_to_top10(df):
    """Normaliseer df naar [(rank, title)]."""
    if df is None or df.empty:
        return []
    d = df.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    # titelkolom
    name_col = None
    for k in ["title", "show", "program", "film", "name"]:
        if k in d.columns:
            name_col = k
            break
    if not name_col:
        name_col = d.columns[0]
    # rankkolom
    rank_col = None
    if "#" in d.columns: rank_col = "#"
    elif "rank" in d.columns: rank_col = "rank"

    if rank_col:
        with pd.option_context("mode.chained_assignment", None):
            d[rank_col] = pd.to_numeric(d[rank_col], errors="coerce")
            d = d.sort_values(by=rank_col)
    d = d.head(10)

    out, r = [], 1
    for _, row in d.iterrows():
        name = str(row[name_col]).strip()
        if not name: 
            continue
        rr = int(row[rank_col]) if rank_col and pd.notna(row.get(rank_col)) else r
        out.append((rr, name))
        r += 1

    # dedup op titel, behoud volgorde
    seen, unique = set(), []
    for rr, nm in out:
        key = nm.lower()
        if key in seen: 
            continue
        unique.append((rr, nm))
        seen.add(key)
        if len(unique) == 10:
            break
    return unique

def parse_netflix_top10(block_html):
    """Zoekt in het Netflix-blok de twee tabellen: Movies en TV Shows."""
    # Probeer eerst pandas.read_html op alleen het Netflix-blok
    movies, shows = None, None
    try:
        dfs = pd.read_html(block_html, flavor="bs4")
        # we labelen op basis van de heading vóór de tabel
        soup = BeautifulSoup(block_html, "html.parser")

        def label_table(tb):
            # pak de tabel als node en kijk naar voorafgaande heading/label
            node = soup.find(lambda tag: tag.name == "table" and tag.get_text(" ", strip=True)[:30] in tb.to_string()[:30])
            # fallback: gewoon de eerstvolgende heading boven de tabel pakken
            if not node:
                node = soup.find("table")
            hdr = None
            if node:
                # loop omhoog en zoek 'TOP 10 Movies' / 'TOP 10 TV Shows' in siblings/buren
                prev = node
                for _ in range(8):
                    prev = prev.find_previous(["h4", "h3", "h2", "div"])
                    if not prev: break
                    txt = (prev.get_text(" ", strip=True) or "").lower()
                    if "top 10 movies" in txt:
                        return "movies"
                    if "top 10 tv shows" in txt or "top 10 shows" in txt:
                        return "shows"
            # als we niets kunnen labelen, beslissen we later op heuristiek
            return None

        labeled = []
        for df in dfs:
            label = label_table(df)
            labeled.append((label, df))

        # Distribution
        if labeled:
            # probeer met labels
            for lab, df in labeled:
                if lab == "movies" and movies is None:
                    movies = df
                elif (lab == "shows" or lab == "tv") and shows is None:
                    shows = df

            # nog niet gelabeld? neem de eerste 2 tabellen: [movies, shows]
            if not movies or not shows:
                if len(dfs) >= 2:
                    if not movies: movies = dfs[0]
                    if not shows:  shows  = dfs[1]
                elif len(dfs) == 1:
                    shows = dfs[0]  # kies als series bij gebrek aan beter
    except Exception as e:
        print(f"[flix] pandas.read_html failed on netflix block: {e}")
        # Fallback met soup + handmatige parsing
        soup = BeautifulSoup(block_html, "html.parser")
        all_tables = soup.find_all("table")
        if len(all_tables) >= 2:
            movies_html = str(all_tables[0])
            shows_html  = str(all_tables[1])
            try:  movies = pd.read_html(movies_html)[0]
            except: movies = None
            try:  shows  = pd.read_html(shows_html)[0]
            except: shows  = None

    return movies, shows

def tmdb_lookup(title, is_series=True):
    if not TMDB_API_KEY:
        return None, None
    try:
        url = "https://api.themoviedb.org/3/search/tv" if is_series else "https://api.themoviedb.org/3/search/movie"
        resp = requests.get(url, params={
            "api_key": TMDB_API_KEY,
            "query": title,
            "include_adult": "false",
            "language": "nl-NL",
            "page": 1
        }, timeout=20)
        resp.raise_for_status()
        j = resp.json()
        if j.get("results"):
            m = j["results"][0]
            return m.get("id"), m.get("poster_path")
    except Exception:
        pass
    return None, None

def build_metas(list_titles, is_series=True):
    metas = []
    for rank, name in list_titles:
        tmdb_id, poster_path = tmdb_lookup(name, is_series=is_series)
        if is_series:
            idv = f"tmdb:tv:{tmdb_id}" if tmdb_id else f"flix-fallback-s{rank}"
            typ = "series"
        else:
            idv = f"tmdb:movie:{tmdb_id}" if tmdb_id else f"flix-fallback-m{rank}"
            typ = "movie"
        metas.append({
            "id": idv,
            "type": typ,
            "name": name,
            "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
            "description": f"Netflix NL Top 10 – positie #{rank}"
        })
    return metas

def main():
    date_str, html = fetch_first_available_html()
    block_html = find_netflix_section(html)
    movies_df, shows_df = parse_netflix_top10(block_html)

    shows = table_to_top10(shows_df)
    movies = table_to_top10(movies_df)

    if not shows and not movies:
        raise RuntimeError("Kon Netflix TOP 10 Movies/TV niet vinden op de pagina")

    os.makedirs(os.path.dirname(OUT_SERIES), exist_ok=True)

    if shows:
        series_metas = build_metas(shows, is_series=True)
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": series_metas}, f, ensure_ascii=False, indent=2)
        print(f"[flix] Netflix SERIES geschreven ({len(series_metas)}) – datum {date_str}")
    else:
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] Geen Netflix series gevonden; lege lijst geschreven")

    if movies:
        movies_metas = build_metas(movies, is_series=False)
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": movies_metas}, f, ensure_ascii=False, indent=2)
        print(f"[flix] Netflix MOVIES geschreven ({len(movies_metas)}) – datum {date_str}")
    else:
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] Geen Netflix films gevonden; lege lijst geschreven")

if __name__ == "__main__":
    main()
