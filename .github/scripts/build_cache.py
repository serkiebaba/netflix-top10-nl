import os, io, json, datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

OUT_SERIES = os.path.join("cache", "netflix_nl_series.json")
OUT_MOVIES = os.path.join("cache", "netflix_nl_movies.json")

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
    for h in soup.find_all(["h1", "h2", "h3"]):
        t = (h.get_text(" ", strip=True) or "").lower()
        if t.startswith("netflix top 10 in the netherlands"):
            target = h
            break
    # Fallback
    if not target:
        for h in soup.find_all(["h1", "h2", "h3"]):
            t = (h.get_text(" ", strip=True) or "").lower()
            if "netflix" in t and "top 10" in t and "netherlands" in t:
                target = h; break
    if not target:
        raise RuntimeError("Netflix heading niet gevonden")

    # Verzamel nodes tot aan volgende provider-heading
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

def parse_titles_from_table(tbl):
    """
    Haal (rank, title) uit een <table>. We pakken:
    - rank: getal uit eerste <td> of iets dat op # lijkt
    - title: tekst uit <td> dat een <a> bevat of class met 'title'
    """
    out = []
    rows = tbl.find_all("tr")
    for tr in rows[1:]:  # skip header
        tds = tr.find_all("td")
        if not tds or len(tds) < 2:
            continue
        # rank: uit eerste cel
        rank_txt = tds[0].get_text(" ", strip=True)
        try:
            rank = int("".join(ch for ch in rank_txt if ch.isdigit()))
        except Exception:
            continue

        # title: prefer <td class*=title> else 1e <a> in de rij, else tweede kolom
        title_cell = None
        for td in tds:
            cls = " ".join(td.get("class", [])).lower()
            if "title" in cls:
                title_cell = td; break
        if not title_cell:
            a = tr.find("a")
            if a and a.get_text(strip=True):
                title = a.get_text(" ", strip=True)
            else:
                title = tds[1].get_text(" ", strip=True)
        else:
            a = title_cell.find("a")
            title = a.get_text(" ", strip=True) if a else title_cell.get_text(" ", strip=True)

        title = title.strip()
        if not title:
            continue
        out.append((rank, title))

    # Sorteer en pak top-10, dedup op titel
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
    Zoek in het Netflix-blok expliciet de twee secties
    met headings 'TOP 10 Movies' en 'TOP 10 TV Shows',
    en parse dan de eerstvolgende <table>.
    """
    soup = BeautifulSoup(block_html, "html.parser")

    def table_after_heading(words):
        for h in soup.find_all(["h2","h3","h4","div"]):
            txt = (h.get_text(" ", strip=True) or "").lower()
            if any(w in txt for w in words):
                tbl = h.find_next("table")
                if tbl:
                    return tbl
        return None

    movies_tbl = table_after_heading(["top 10 movies"])
    shows_tbl  = table_after_heading(["top 10 tv shows", "top 10 shows"])

    movies, shows = [], []
    if movies_tbl:
        movies = parse_titles_from_table(movies_tbl)
    if shows_tbl:
        shows = parse_titles_from_table(shows_tbl)

    return movies, shows

def tmdb_lookup(title, is_series=True):
    if not TMDB_API_KEY:
        return None, None
    try:
        url = "https://api.themoviedb.org/3/search/tv" if is_series else "https://api.themoviedb.org/3/search/movie"
        # kleine schoonmaak (haakjes/jaartal verwijderen helpt matching)
        clean = title
        for ch in ["(", ")", "[", "]"]:
            clean = clean.replace(ch, " ")
        resp = requests.get(url, params={
            "api_key": TMDB_API_KEY,
            "query": clean,
            "include_adult": "false",
            "language": "en-US",
            "page": 1
        }, timeout=15)
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
    block_html = find_netflix_block_html(html)
    movies_list, shows_list = parse_netflix_top10(block_html)

    if not shows_list and not movies_list:
        raise RuntimeError("Kon Netflix top-10 tabellen niet vinden")

    os.makedirs(os.path.dirname(OUT_SERIES), exist_ok=True)

    # SERIES
    if shows_list:
        series_metas = build_metas(shows_list, is_series=True)
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": series_metas}, f, ensure_ascii=False, indent=2)
        print(f"[flix] Netflix SERIES geschreven ({len(series_metas)}) – datum {date_str}")
    else:
        with open(OUT_SERIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] Geen Netflix series gevonden; lege lijst geschreven")

    # MOVIES
    if movies_list:
        movies_metas = build_metas(movies_list, is_series=False)
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": movies_metas}, f, ensure_ascii=False, indent=2)
        print(f"[flix] Netflix MOVIES geschreven ({len(movies_metas)}) – datum {date_str}")
    else:
        with open(OUT_MOVIES, "w", encoding="utf-8") as f:
            json.dump({"metas": []}, f, ensure_ascii=False, indent=2)
        print("[flix] Geen Netflix films gevonden; lege lijst geschreven")

if __name__ == "__main__":
    main()
