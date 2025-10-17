import os, json, datetime
import requests
from bs4 import BeautifulSoup

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

OUT_SERIES = os.path.join("cache", "netflix_nl_series.json")
OUT_MOVIES = os.path.join("cache", "netflix_nl_movies.json")

UA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
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
            if "Top 10" not in html and "TOP 10" not in html:
                last_err = "unexpected page content"
                continue
            return ds, html
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(f"Kon geen bruikbare FlixPatrol NL pagina vinden: {last_err}")

def find_netflix_block_html(html):
    soup = BeautifulSoup(html, "html.parser")

    # Zoek heading met 'Netflix TOP 10 in the Netherlands'
    target = None
    for h in soup.find_all(["h1","h2","h3"]):
        t = (h.get_text(" ", strip=True) or "").lower()
        if t.startswith("netflix top 10 in the netherlands"):
            target = h; break
    if not target:
        for h in soup.find_all(["h1","h2","h3"]):
            t = (h.get_text(" ", strip=True) or "").lower()
            if "netflix" in t and "top 10" in t and "netherlands" in t:
                target = h; break
    if not target:
        raise RuntimeError("Netflix heading niet gevonden")

    # Verzamel alles tot de volgende provider-heading
    nodes = []
    for sib in target.next_siblings:
        if getattr(sib, "name", None) in ("h1","h2","h3"):
            txt = (sib.get_text(" ", strip=True) or "").lower()
            if " top 10 in the netherlands" in txt and "netflix" not in txt:
                break
        nodes.append(sib)
    block_html = "".join(str(x) for x in nodes).strip()
    if not block_html:
        raise RuntimeError("Netflix-blok is leeg")
    return block_html

def digits(s):
    return "".join(ch for ch in s if ch.isdigit())

def parse_titles_from_table(tbl):
    """Pak (rank, title) uit een FlixPatrol top-10 <table>."""
    out = []
    for tr in tbl.find_all("tr"):
        cells = tr.find_all(["td","th"])
        if not cells: 
            continue

        # Rank = digits uit 1e cel
        rank_txt = cells[0].get_text(" ", strip=True)
        d = digits(rank_txt)
        if not d:
            continue
        rank = int(d)

        # Titel: prefer <td class*=title>, anders 1e <a>, anders langste tekst (excl. 1e kolom)
        title = ""
        title_cell = None
        for td in cells[1:]:
            cls = " ".join(td.get("class", [])).lower()
            if "title" in cls:
                title_cell = td; break
        if title_cell:
            a = title_cell.find("a")
            title = a.get_text(" ", strip=True) if a else title_cell.get_text(" ", strip=True)
        else:
            a = tr.find("a")
            if a and a.get_text(strip=True):
                title = a.get_text(" ", strip=True)
            else:
                rest = [c.get_text(" ", strip=True) for c in cells[1:]]
                rest.sort(key=lambda s: len(s or ""), reverse=True)
                title = (rest[0] if rest else "").strip()

        if not title:
            continue
        out.append((rank, title))

    # Sorteer op rank, dedup, pak top-10
    out.sort(key=lambda x: x[0])
    seen, uniq = set(), []
    for r, t in out:
        k = t.lower()
        if k in seen: 
            continue
        uniq.append((r, t))
        seen.add(k)
        if len(uniq) == 10:
            break
    return uniq

def parse_netflix_top10(block_html):
    """
    Koppel headings precies aan hun tabel: Movies en TV Shows.
    Fallback: als een heading ontbreekt, neem dan eerste tabel = Movies, tweede = TV Shows.
    """
    soup = BeautifulSoup(block_html, "html.parser")

    def next_table_after_heading(matchers):
        for h in soup.find_all(["h2","h3","h4","div"]):
            txt = (h.get_text(" ", strip=True) or "").lower()
            if any(m in txt for m in matchers):
                tbl = h.find_next("table")
                if tbl:
                    return tbl
        return None

    movies_tbl = next_table_after_heading(["top 10 movies"])
    shows_tbl  = next_table_after_heading(["top 10 tv shows", "top 10 shows"])

    # Fallback op volgorde (1e=movies, 2e=shows)
    if movies_tbl is None or shows_tbl is None:
        tables = soup.find_all("table")
        if len(tables) >= 1 and movies_tbl is None:
            movies_tbl = tables[0]
        if len(tables) >= 2 and shows_tbl is None:
            shows_tbl = tables[1]

    movies = parse_titles_from_table(movies_tbl) if movies_tbl else []
    shows  = parse_titles_from_table(shows_tbl)  if shows_tbl  else []

    return movies, shows

def tmdb_lookup_both_ways(title, prefer_series=True):
    """
    Probeer eerst de 'juiste' zoekroute (tv voor series, movie voor films).
    Als dat geen resultaat heeft, probeer de andere route.
    """
    if not TMDB_API_KEY:
        return None, None, None  # id, poster, kind

    def q(url):
        resp = requests.get(url, params={
            "api_key": TMDB_API_KEY,
            "query": title,
            "include_adult": "false",
            "language": "en-US",
            "page": 1
        }, timeout=15)
        resp.raise_for_status()
        j = resp.json()
        if j.get("results"):
            m = j["results"][0]
            return m.get("id"), m.get("poster_path")
        return None, None

    try:
        if prefer_series:
            # tv eerst
            tid, p = q("https://api.themoviedb.org/3/search/tv")
            if tid: return tid, p, "tv"
            # dan movie
            mid, p2 = q("https://api.themoviedb.org/3/search/movie")
            if mid: return mid, p2, "movie"
        else:
            # movie eerst
            mid, p = q("https://api.themoviedb.org/3/search/movie")
            if mid: return mid, p, "movie"
            tid, p2 = q("https://api.themoviedb.org/3/search/tv")
            if tid: return tid, p2, "tv"
    except Exception:
        pass
    return None, None, None

def build_metas(items, want_series=True):
    metas = []
    for rank, name in items:
        tid, poster_path, kind = tmdb_lookup_both_ways(name, prefer_series=want_series)
        if want_series:
            # we willen series; als TMDB alleen 'movie' vindt, maken we toch een series-meta (zonder streams)
            meta_id = f"tmdb:tv:{tid}" if tid and kind == "tv" else (f"tmdb:movie:{tid}" if tid else f"flix-fallback-s{rank}")
            mtype = "series"
        else:
            meta_id = f"tmdb:movie:{tid}" if tid and kind == "movie" else (f"tmdb:tv:{tid}" if tid else f"flix-fallback-m{rank}")
            mtype = "movie"
        metas.append({
            "id": meta_id,
            "type": mtype,
            "name": name,
            "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
            "description": f"Netflix NL Top 10 – positie #{rank}"
        })
    return metas

def main():
    date_str, html = fetch_first_available_html()
    block_html = find_netflix_block_html(html)
    movies_list, shows_list = parse_netflix_top10(block_html)

    if not shows_list and not movies_list:
        raise RuntimeError("Kon Netflix top-10 tabellen niet vinden")

    os.makedirs(os.path.dirname(OUT_SERIES), exist_ok=True)

    # SERIES (TV Shows)
    if shows_list:
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": build_metas(shows_list, want_series=True)}, f, ensure_ascii=False, indent=2)
        print(f"[flix] SERIES ok ({len(shows_list)}) – {date_str}")
    else:
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] SERIES leeg")

    # MOVIES (Films)
    if movies_list:
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": build_metas(movies_list, want_series=False)}, f, ensure_ascii=False, indent=2)
        print(f"[flix] MOVIES ok ({len(movies_list)}) – {date_str}")
    else:
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] MOVIES leeg")

if __name__ == "__main__":
    main()
