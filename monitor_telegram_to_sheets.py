import os, time, re, pytz, requests
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from bs4 import BeautifulSoup

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest

import gspread
from google.oauth2.service_account import Credentials

# ================== НАСТРОЙКИ ==================
TZ       = os.getenv("TZ", "Europe/Amsterdam")
SHEET    = os.getenv("GOOGLE_SHEET_NAME", os.getenv("GSHEET_NAME", "Admiral Mentions"))
API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION  = os.getenv("TELETHON_SESSION")

CHANNELS = [
    "@sehockey", "@vbros_po_bortu", "@khl_official_telegram", "@arena_breda",
    "@besprokata", "@derzhiperedachu", "@hockeywithroman", "@erykalov_s_shaiboi"
]

NEWS_SITES = [
    "https://hcadmiral.pro/news/",
    "https://www.khl.ru/news/",
    "https://allhockey.ru/",
    "https://sport.business-gazeta.ru/",
    "https://www.sports.ru/hockey/khl/",
    "https://lenta.ru/rubrics/sport/hockey/",
    "https://matchtv.ru/hockey",
    "https://sport25.ru/",
    "https://sport25.pro/",
    "https://metaratings.ru/hockey/",
    "https://vl.ru/",
]

MAX_PAGES_PER_SITE = 15
REQUEST_TIMEOUT    = 12
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AdmiralMonitor/1.3)"}

# ================== КЛЮЧИ / РЕГУЛЯРКИ ==================
# Антишум для "Адмирал"
RE_ADMIRAL  = re.compile(r"\bадмирал(?!т[её]й|т[её]йск|т[её]йство)(ы|а|у|ом|е)?\b", re.IGNORECASE)
# Хоккейный контекст
RE_HOCKEY   = re.compile(r"\b(кхл|хокке[йя]|шайб[аеуы]?|матч|игр[аеуы]|турнир|плей-офф|гол|вратар|защитник|нападающ|тренер|трансфер|состав|звено|бросок|штраф)\b", re.IGNORECASE)
# Владивосток / Приморье
RE_VLAD     = re.compile(r"\bвладивосток(а|у|ом|е)?\b", re.IGNORECASE)
RE_PRIM     = re.compile(r"\bпримор(?:ье|ья|ью|ьи|ье|ск|ского|ском|скому|ске)\b|\bприморец(а|у|ем|е)?\b|\bприморц(ы|ев|ам|ами|ах)\b", re.IGNORECASE)

# Фамилии (в падежах)
NAME_PATTERNS = [
    r"\bШугаев(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bХуска\b", r"\bЦыба\b",
    r"\bКоледов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШепелев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bДерябин(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bРучкин(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bСолянников(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШулак(а|у|ом|е|и|ам|ами|ах)?\b",
    r"\bГрман(а|у|ом|е|ы|ам|ами|ах)?\b", r"\bМарин(а|у|ом|е|ы|ам|ами|ах)?\b",
    r"\bГераськин(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bОлсон(а|у|ом|е|ы|ам|ами|ах)?\b",
    r"\bПопов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКошелев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bДемченко\b", r"\bШаров(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bПетухов(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bШэн\b", r"\bЗавгородн(?:ий|его|ему|им|ем|ие|их|ими)?\b",
    r"\bСтарков(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШестаков(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bМуранов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bГутик(а|у|ом|е|и|ам|ами|ах)?\b",
    r"\bТамбиев(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bБанада(ы|е|у|ой|ам|ами|ах)?\b",
    r"\bМазитов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКатаев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bГромов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКарас(?:ёв|ев)(а|у|ым|е|ы|ам|ами|ах)?\b",
]
RE_NAMES = [re.compile(p, re.IGNORECASE) for p in NAME_PATTERNS]

# ================== ПРАВИЛА ПО ДОМЕНАМ ==================
SITE_RULES = {
    "hcadmiral.pro":             {"allow_path_contains": ["/news"]},
    "khl.ru":                    {"allow_path_contains": ["/news"]},
    "allhockey.ru":              {"allow_path_contains": ["/news", "/blog", "/article"]},
    "sport.business-gazeta.ru":  {"allow_path_contains": ["/article", "/blog", "/news"]},
    "sports.ru":                 {"allow_path_contains": ["/hockey/khl", "/admiral"]},
    "lenta.ru":                  {"allow_path_contains": ["/rubrics/sport/hockey"]},
    "matchtv.ru":                {"allow_path_contains": ["/hockey"]},
    "sport25.ru":                {"allow_path_contains": ["/news", "/article", "/hockey"]},
    "sport25.pro":               {"allow_path_contains": ["/news", "/article", "/hockey"]},
    "metaratings.ru":            {"allow_path_contains": ["/hockey", "/news", "/articles"]},
    "vl.ru":                     {"allow_path_contains": ["/news"]},
}

# ================== GOOGLE SHEETS ==================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
spread = gc.open(SHEET)

def ensure_worksheet(date_str: str):
    try:
        ws = spread.worksheet(date_str)
    except gspread.exceptions.WorksheetNotFound:
        ws = spread.add_worksheet(title=date_str, rows=2000, cols=10)
        ws.append_row(["Время", "Источник", "Канал/Сайт", "Заголовок/Текст", "Ссылка"], value_input_option="RAW")
    return ws

def ensure_seen_sheet():
    try:
        ws = spread.worksheet("SEEN")
    except gspread.exceptions.WorksheetNotFound:
        ws = spread.add_worksheet(title="SEEN", rows=20000, cols=3)
        ws.append_row(["url", "first_seen_at", "source"], value_input_option="RAW")
    return ws

def load_seen_urls() -> set:
    ws = ensure_seen_sheet()
    try:
        vals = ws.col_values(1)[1:]
        return set(vals)
    except Exception:
        return set()

def add_seen_urls(urls_with_source: list[tuple[str,str]]):
    if not urls_with_source:
        return
    ws = ensure_seen_sheet()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [[u, now, src] for (u, src) in urls_with_source]
    ws.append_rows(rows, value_input_option="RAW")

def append_grouped_rows(grouped_rows: dict):
    for date_str, rows in grouped_rows.items():
        rows_sorted = sorted(rows, key=lambda r: r[0])  # по времени
        ws = ensure_worksheet(date_str)
        if rows_sorted:
            ws.append_rows(rows_sorted, value_input_option="RAW")

# ================== УТИЛИТЫ ТЕКСТА/URL ==================
def canonical_url(u: str) -> str:
    try:
        p = urlparse(u)
        scheme = p.scheme or "https"
        netloc = p.netloc.lower()
        path = p.path or "/"
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "yclid", "gclid", "fbclid"))]
        query = urlencode(q, doseq=True)
        return urlunparse((scheme, netloc, path.rstrip("/"), "", query, ""))  # без fragment
    except Exception:
        return u

def split_sentences(text: str) -> list[str]:
    if not text:
        return []
    t = text.replace("...", "…")
    parts = re.split(r"[\.!\?\n\r]+", t)
    return [p.strip() for p in parts if p.strip()]

def domain_allowed_path(domain: str, path: str) -> bool:
    rules = SITE_RULES.get(domain, {})
    allow = rules.get("allow_path_contains")
    if not allow:
        return True
    return any(sub in path for sub in allow)

def sentence_level_match(text: str) -> bool:
    """
    Совпадение по теме ХК «Адмирал»:
    - есть «Адмирал» (без адмиралтей….) ИЛИ любая фамилия игрока/тренера; ИЛИ
    - есть предложение, где вместе встречаются (хоккей-контекст) и (Владивосток/Приморье).
    """
    if not text:
        return False
    t = text
    if RE_ADMIRAL.search(t) or any(r.search(t) for r in RE_NAMES):
        return True
    for sent in split_sentences(t):
        if RE_HOCKEY.search(sent) and (RE_VLAD.search(sent) or RE_PRIM.search(sent)):
            return True
    return False

# ================== HTTP / ПАРСИНГ САЙТОВ ==================
def safe_get(url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception as e:
        print(f"[HTTP] {url} ошибка: {e}")
    return None

def is_same_domain(base: str, href: str) -> bool:
    try:
        return urlparse(base).netloc == urlparse(href).netloc
    except Exception:
        return False

def collect_links_from_site(root_url: str) -> list[str]:
    html = safe_get(root_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    base = root_url
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#"):
            continue
        full = urljoin(base, href)
        if not is_same_domain(base, full):
            continue
        if any(x in full for x in ("/news", "/novosti", "/article", "/posts", "/post", "/story")) or full.endswith(".html"):
            links.append(full)
        else:
            links.append(full)
    uniq, seen = [], set()
    for u in links:
        cu = canonical_url(u)
        if cu not in seen:
            seen.add(cu)
            uniq.append(cu)
        if len(uniq) >= MAX_PAGES_PER_SITE:
            break
    return uniq

def parse_iso_guess(s: str, tz):
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
    except Exception:
        return None

def extract_datetime_generic(soup: BeautifulSoup, tz):
    m = soup.select_one("meta[property='article:published_time']")
    if m and m.get("content"):
        dt = parse_iso_guess(m["content"], tz)
        if dt: return dt
    t = soup.find("time")
    if t and t.get("datetime"):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%zZ", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                s = t["datetime"].replace("Z","+0000")
                dt = datetime.strptime(s, fmt)
                if not dt.tzinfo: dt = tz.localize(dt)
                return dt.astimezone(tz)
            except Exception:
                pass
    for name in ["pubdate","date","DC.date.issued","article:modified_time","og:updated_time","mediator_published_time","datePublished","datecreated","dcterms.date"]:
        m = soup.select_one(f"meta[name='{name}'], meta[property='{name}']")
        if m and m.get("content"):
            dt = parse_iso_guess(m["content"], tz)
            if dt: return dt
    return None

def extract_datetime_by_domain(domain: str, soup: BeautifulSoup, tz):
    d = domain.lower()
    def pick(*selectors):
        for sel in selectors:
            m = soup.select_one(sel)
            if m and m.get("content"):
                return parse_iso_guess(m["content"], tz)
        return None

    if "sports.ru" in d:
        return pick("meta[property='article:published_time']")
    if "lenta.ru" in d:
        return pick("meta[property='article:published_time']", "meta[itemprop='datePublished']")
    if "matchtv.ru" in d:
        dt = pick("meta[property='article:published_time']")
        if dt: return dt
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            return parse_iso_guess(t["datetime"], tz)
    if "metaratings.ru" in d:
        return pick("meta[property='article:published_time']", "meta[name='pubdate']")
    if "allhockey.ru" in d:
        return pick("meta[name='pubdate']", "meta[property='article:published_time']")
    if "sport.business-gazeta.ru" in d:
        return pick("meta[property='article:published_time']", "meta[name='DC.date.issued']")
    if d == "vl.ru" or d.endswith(".vl.ru"):
        return pick("meta[name='mediator_published_time']", "meta[property='article:published_time']")
    if "sport25.ru" in d or "sport25.pro" in d:
        dt = pick("meta[property='article:published_time']")
        if dt: return dt
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            return parse_iso_guess(t["datetime"], tz)
    if "khl.ru" in d or "hcadmiral.pro" in d:
        dt = pick("meta[property='article:published_time']")
        if dt: return dt
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            return parse_iso_guess(t["datetime"], tz)
    return None

def extract_datetime_from_page(domain: str, soup: BeautifulSoup, tz):
    return (extract_datetime_generic(soup, tz)
            or extract_datetime_by_domain(domain, soup, tz)
            or datetime.now(pytz.timezone(TZ)))

def extract_title(soup: BeautifulSoup) -> str:
    og = soup.select_one("meta[property='og:title']")
    if og and og.get("content"):
        return og["content"].strip()
    if soup.title and soup.title.text:
        return soup.title.text.strip()
    h1 = soup.find("h1")
    return h1.text.strip() if h1 and h1.text else ""

def extract_main_text(soup: BeautifulSoup) -> str:
    ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    return " ".join(ps)[:4000]

def collect_from_sites(tz, seen: set) -> list[tuple]:
    results = []
    for site in NEWS_SITES:
        try:
            links = collect_links_from_site(site)
            site_domain = urlparse(site).netloc.lower()
            for url in links:
                url_c = canonical_url(url)
                if url_c in seen:
                    continue
                parsed = urlparse(url_c)
                if not domain_allowed_path(site_domain, parsed.path):
                    continue
                html = safe_get(url_c)
                if not html:
                    continue
                soup = BeautifulSoup(html, "html.parser")
                title = extract_title(soup)
                body  = extract_main_text(soup)
                content = f"{title}\n{body}"
                if not sentence_level_match(content):
                    continue
                dt = extract_datetime_from_page(site_domain, soup, tz)
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")
                snippet  = title if title else (body[:140] + "…")
                results.append((date_str, time_str, "СМИ", site_domain, snippet, url_c))
        except Exception as e:
            print(f"[SITE] {site} ошибка: {e}")
            continue
    return results

# ================== TELEGRAM ==================
client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

async def collect_from_telegram(tz, seen: set) -> list[tuple]:
    collected = []
    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch)
            hist = await client(GetHistoryRequest(
                peer=entity, limit=50, offset_date=None, offset_id=0,
                max_id=0, min_id=0, add_offset=0, hash=0
            ))
            for msg in hist.messages:
                text = getattr(msg, "message", "") or ""
                if not text:
                    continue
                url = f"https://t.me/{ch.lstrip('@')}/{msg.id}"
                url_c = canonical_url(url)
                if url_c in seen:
                    continue
                # для телеги оставляем ту же логику: фамилии/адмирал ИЛИ хоккей+локальный контекст в одном предложении
                if sentence_level_match(text):
                    dt_utc = msg.date.replace(tzinfo=timezone.utc)
                    dt_local = dt_utc.astimezone(tz)
                    date_str = dt_local.strftime("%Y-%m-%d")
                    time_str = dt_local.strftime("%H:%M:%S")
                    snippet = (text[:140] + "…") if len(text) > 140 else text
                    collected.append((date_str, time_str, "Telegram", ch, snippet, url_c))
        except FloodWaitError as e:
            print(f"[TG] FloodWait: ждём {e.seconds} сек")
            time.sleep(int(e.seconds) + 1)
        except Exception as e:
            print(f"[TG] Ошибка на канале {ch}: {e}")
    return collected

# ================== ЗАПУСК ==================
async def main():
    tz = pytz.timezone(TZ)
    seen = load_seen_urls()

    tg_rows  = await collect_from_telegram(tz, seen)
    web_rows = collect_from_sites(tz, seen)

    grouped: dict[str, list[list[str]]] = {}
    to_seen: list[tuple[str,str]] = []
    for date_str, time_str, source, place, snippet, url in (tg_rows + web_rows):
        grouped.setdefault(date_str, []).append([time_str, source, place, snippet, url])
        to_seen.append((url, source))

    if grouped:
        append_grouped_rows(grouped)
        if to_seen:
            already = load_seen_urls()
            add_seen_urls([(u, s) for (u, s) in to_seen if u not in already])
        total = sum(len(v) for v in grouped.values())
        print(f"✅ Добавлено {total} строк (по датам, с дедупликацией).")
    else:
        print("Совпадений по строгим правилам не найдено.")

if __name__ == "__main__":
    with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        globals()["client"] = client
        client.loop.run_until_complete(main())
