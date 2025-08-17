import os, time, re, pytz, requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse
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

# Телеграм-каналы
CHANNELS = [
    "@sehockey", "@vbros_po_bortu", "@khl_official_telegram", "@arena_breda",
    "@besprokata", "@derzhiperedachu", "@hockeywithroman", "@erykalov_s_shaiboi"
]

# Сайты для сканирования
NEWS_SITES = [
    "https://hcadmiral.pro/news/",            # новости Адмирала
    "https://www.khl.ru/news/",               # КХЛ
    "https://allhockey.ru/",                  # AllHockey
    "https://sport.business-gazeta.ru/",      # БО спорт
    "https://www.sports.ru/hockey/khl/",      # Sports.ru KHL
    "https://lenta.ru/rubrics/sport/hockey/", # Lenta.ru хоккей
    "https://matchtv.ru/hockey",              # Матч ТВ хоккей
    "https://sport25.ru/",                    # Спорт25
    "https://sport25.pro/",                   # Спорт25 (альт)
    "https://metaratings.ru/hockey/",         # Metaratings хоккей
    "https://vl.ru/",                         # VL.ru
]

MAX_PAGES_PER_SITE = 15
REQUEST_TIMEOUT    = 12
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AdmiralMonitor/1.1)"}

# ================== КЛЮЧЕВЫЕ ШАБЛОНЫ ==================
TEAM_PATTERNS = [
    r"\bадмирал(ы|а|у|ом|е)?\b",
    r"\bморяк(и|ов|ам|ами|ах|а|у|ом|е)?\b",
    r"\bвладивосток(а|у|ом|е)?\b",
    r"(?:\bприморц(ы|ев|ам|ами|ах)\b|\bприморец(а|у|ем|е)?\b)",
]

NAME_PATTERNS = [
    # вратари
    r"\bШугаев(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bХуска\b", r"\bЦыба\b",
    # защитники
    r"\bКоледов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШепелев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bДерябин(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bРучкин(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bСолянников(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШулак(а|у|ом|е|и|ам|ами|ах)?\b",
    r"\bГрман(а|у|ом|е|ы|ам|ами|ах)?\b", r"\bМарин(а|у|ом|е|ы|ам|ами|ах)?\b",
    # нападающие
    r"\bГераськин(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bОлсон(а|у|ом|е|ы|ам|ами|ах)?\b",
    r"\bПопов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКошелев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bДемченко\b", r"\bШаров(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bПетухов(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bШэн\b", r"\bЗавгородн(?:ий|его|ему|им|ем|ие|их|ими)?\b",
    r"\bСтарков(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bШестаков(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bМуранов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bГутик(а|у|ом|е|и|ам|ами|ах)?\b",
    # тренеры
    r"\bТамбиев(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bБанада(ы|е|у|ой|ам|ами|ах)?\b",
    r"\bМазитов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКатаев(а|у|ым|е|ы|ам|ами|ах)?\b",
    r"\bГромов(а|у|ым|е|ы|ам|ами|ах)?\b", r"\bКарас(?:ёв|ев)(а|у|ым|е|ы|ам|ами|ах)?\b",
]
KEYWORD_PATTERNS = [re.compile(p, re.IGNORECASE) for p in (TEAM_PATTERNS + NAME_PATTERNS)]

def text_matches(text: str) -> bool:
    t = (text or "").strip()
    return any(p.search(t) for p in KEYWORD_PATTERNS)

# ================== GOOGLE SHEETS ==================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
spread = gc.open(SHEET)

def ensure_worksheet(date_str: str):
    """Лист с названием YYYY-MM-DD (создаём при отсутствии)."""
    try:
        ws = spread.worksheet(date_str)
    except gspread.exceptions.WorksheetNotFound:
        ws = spread.add_worksheet(title=date_str, rows=2000, cols=10)
        ws.append_row(["Время", "Источник", "Канал/Сайт", "Заголовок/Текст", "Ссылка"], value_input_option="RAW")
    return ws

def append_grouped_rows(grouped_rows: dict):
    """grouped_rows: { 'YYYY-MM-DD': [ [time, source, place, snippet, url], ... ] }"""
    for date_str, rows in grouped_rows.items():
        rows_sorted = sorted(rows, key=lambda r: r[0])  # сортировка по времени
        ws = ensure_worksheet(date_str)
        if rows_sorted:
            ws.append_rows(rows_sorted, value_input_option="RAW")

# ================== HTTP ВСПОМОГАТЕЛЬНЫЕ ==================
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
    """
    Берём стартовую страницу и собираем до MAX_PAGES_PER_SITE ссылок, которые похожи на новости/посты.
    Сильно зависит от верстки, поэтому применяем простые эвристики + ограничение по количеству.
    """
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
        # эвристика: новости/посты
        if any(x in full for x in ("/news", "/novosti", "/article", "/posts", "/post", "/story")) or full.endswith(".html"):
            links.append(full)
        else:
            # иногда новость прямо на главной
            links.append(full)

    uniq, seen = [], set()
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
        if len(uniq) >= MAX_PAGES_PER_SITE:
            break
    return uniq

# ================== ПАРСИНГ ДАТ ==================
def parse_iso_guess(s: str, tz) -> datetime | None:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
    except Exception:
        return None

def extract_datetime_generic(soup: BeautifulSoup, tz) -> datetime | None:
    # OpenGraph article:published_time
    tag = soup.select_one("meta[property='article:published_time']")
    if tag and tag.get("content"):
        dt = parse_iso_guess(tag["content"], tz)
        if dt: return dt

    # time[datetime]
    ttag = soup.find("time")
    if ttag and ttag.get("datetime"):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%zZ", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                s = ttag["datetime"].replace("Z", "+0000")
                dt = datetime.strptime(s, fmt)
                if not dt.tzinfo: dt = tz.localize(dt)
                return dt.astimezone(tz)
            except Exception:
                continue

    # meta name/prop
    for name in ["pubdate","date","DC.date.issued","article:modified_time","og:updated_time","mediator_published_time","datePublished","datecreated","dcterms.date"]:
        m = soup.select_one(f"meta[name='{name}'], meta[property='{name}']")
        if m and m.get("content"):
            dt = parse_iso_guess(m["content"], tz)
            if dt: return dt

    return None

def extract_datetime_by_domain(domain: str, soup: BeautifulSoup, tz) -> datetime | None:
    """
    Набор точечных правил (если generic не сработал).
    Мы не держим «идеальные» парсеры под каждый сайт, но покрываем популярные варианты.
    """
    d = domain.lower()

    # Sports.ru
    if "sports.ru" in d:
        m = soup.select_one("meta[property='article:published_time']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # Lenta.ru
    if "lenta.ru" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("meta[itemprop='datePublished']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # MatchTV
    if "matchtv.ru" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("time[datetime]")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            return parse_iso_guess(t["datetime"], tz)

    # Metaratings
    if "metaratings.ru" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("meta[name='pubdate']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # Allhockey
    if "allhockey.ru" in d:
        m = soup.select_one("meta[name='pubdate']") or soup.select_one("meta[property='article:published_time']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # sport.business-gazeta.ru
    if "sport.business-gazeta.ru" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("meta[name='DC.date.issued']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # VL.ru
    if d == "vl.ru" or d.endswith(".vl.ru"):
        m = soup.select_one("meta[name='mediator_published_time']") or soup.select_one("meta[property='article:published_time']")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    # sport25 (оба домена)
    if "sport25.ru" in d or "sport25.pro" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("time[datetime]")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            return parse_iso_guess(t["datetime"], tz)

    # KHL / Admiral
    if "khl.ru" in d or "hcadmiral.pro" in d:
        m = soup.select_one("meta[property='article:published_time']") or soup.select_one("time[datetime]")
        if m and m.get("content"):
            return parse_iso_guess(m["content"], tz)

    return None

def extract_datetime_from_page(domain: str, soup: BeautifulSoup, tz) -> datetime:
    dt = extract_datetime_generic(soup, tz)
    if not dt:
        dt = extract_datetime_by_domain(domain, soup, tz)
    if not dt:
        dt = datetime.now(tz)  # фолбэк: не нашли дату — ставим текущую, чтобы не терять материал
    return dt

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
    txt = " ".join(ps)
    return txt[:4000]

# ================== СБОР САЙТОВ ==================
def collect_from_sites(tz) -> list[tuple]:
    results = []
    for site in NEWS_SITES:
        try:
            links = collect_links_from_site(site)
            for url in links:
                html = safe_get(url)
                if not html:
                    continue
                soup = BeautifulSoup(html, "html.parser")
                domain = urlparse(site).netloc
                title  = extract_title(soup)
                body   = extract_main_text(soup)
                content_for_match = f"{title}\n{body}"

                if not text_matches(content_for_match):
                    continue

                dt = extract_datetime_from_page(domain, soup, tz)
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M:%S")
                snippet  = title if title else (body[:140] + "…")
                results.append((date_str, time_str, "СМИ", domain, snippet, url))
        except Exception as e:
            print(f"[SITE] {site} ошибка: {e}")
            continue
    return results

# ================== ТЕЛЕГРАМ ==================
client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

async def collect_from_telegram(tz) -> list[tuple]:
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
                if text_matches(text):
                    dt_utc = msg.date.replace(tzinfo=timezone.utc)
                    dt_local = dt_utc.astimezone(tz)
                    date_str = dt_local.strftime("%Y-%m-%d")
                    time_str = dt_local.strftime("%H:%M:%S")
                    url = f"https://t.me/{ch.lstrip('@')}/{msg.id}"
                    snippet = (text[:140] + "…") if len(text) > 140 else text
                    collected.append((date_str, time_str, "Telegram", ch, snippet, url))
        except FloodWaitError as e:
            print(f"[TG] FloodWait: ждём {e.seconds} сек")
            time.sleep(int(e.seconds) + 1)
        except Exception as e:
            print(f"[TG] Ошибка на канале {ch}: {e}")
    return collected

# ================== ЗАПУСК ==================
async def main():
    tz = pytz.timezone(TZ)

    tg_rows  = await collect_from_telegram(tz)
    web_rows = collect_from_sites(tz)

    grouped: dict[str, list[list[str]]] = {}
    for date_str, time_str, source, place, snippet, url in (tg_rows + web_rows):
        grouped.setdefault(date_str, []).append([time_str, source, place, snippet, url])

    if grouped:
        append_grouped_rows(grouped)
        total = sum(len(v) for v in grouped.values())
        print(f"✅ Добавлено {total} строк по датам (каждая дата — отдельный лист).")
    else:
        print("Нет новых совпадений по ключевым словам.")

if __name__ == "__main__":
    with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        globals()["client"] = client
        client.loop.run_until_complete(main())
