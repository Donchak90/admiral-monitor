import os, re, time, pytz, requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest

import gspread
from google.oauth2.service_account import Credentials

# -------- ENV --------
API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION  = os.getenv("TELETHON_SESSION")   # StringSession
TZ       = os.getenv("TZ", "Europe/Amsterdam")
SHEET    = os.getenv("GOOGLE_SHEET_NAME", "Admiral Mentions")

# -------- Google Sheets --------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
doc = gc.open(SHEET)
ws = doc.sheet1

HEADER = ["Название материала","Краткое изложение","Дата публикации","Источник (сайт/ТГ)","Ссылка","Просмотры"]
if not ws.get_all_values():
    ws.append_row(HEADER)

def get_state_ws():
    try:
        return doc.worksheet("state")
    except gspread.exceptions.WorksheetNotFound:
        st = doc.add_worksheet(title="state", rows=200, cols=2)
        st.update("A1:B1", [["channel","last_id"]])
        return st

state_ws = get_state_ws()

def load_state():
    m = {}
    for row in state_ws.get_all_values()[1:]:
        if len(row) >= 2 and row[0]:
            try:
                m[row[0]] = int(row[1])
            except:
                m[row[0]] = 0
    return m

def save_state(state):
    rows = [["channel","last_id"]] + [[k, str(v)] for k, v in state.items()]
    state_ws.clear()
    state_ws.update("A1", rows)

# -------- Каналы под мониторинг --------
CHANNELS = [
    "sehockey","vbros_po_bortu","khl_official_telegram","arena_breda",
    "besprokata","derzhiperedachu","hockeywithroman","erykalov_s_shaiboi",
]

# -------- Ключевые слова --------
BASE_KEYWORDS = ["Адмирал","хоккейный клуб Адмирал","хоккейная команда Адмирал","Владивосток"]
ROSTER_URL  = "https://hcadmiral.pro/team/players/"
COACHES_URL = "https://hcadmiral.pro/team/coaches/"

def fetch_names(url):
    try:
        html = requests.get(url, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        texts = {el.get_text(" ", strip=True) for el in soup.select("a,div,span")}
        names = []
        for t in texts:
            if not t:
                continue
            # русские ФИО 2-3 слова, отсеиваем служебные слова
            if re.search(r"[А-Яа-яЁё]", t) and len(t) <= 40 and (" " in t or "-" in t):
                if not re.search(r"тренер|вратар|защитник|нападающ|рост|вес|возраст|сезон|матч|главный|ассистент|команда|клуб", t, re.I):
                    parts = t.split()
                    if 2 <= len(parts) <= 3:
                        names.append(t)
        return sorted(set(names))
    except Exception:
        return []

PLAYERS = fetch_names(ROSTER_URL)
COACHES = fetch_names(COACHES_URL)
KEYWORDS = BASE_KEYWORDS + PLAYERS + COACHES
kw = re.compile("|".join(map(re.escape, KEYWORDS)), re.I)

# -------- Telegram --------
client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

def contains(text: str) -> bool:
    return bool(text and kw.search(text))

def to_row(msg, username):
    text = (msg.message or "").strip()
    title = (text.split("\n")[0] if text else "(без текста)")[:120]
    summary = text[:300]
    dt = msg.date.astimezone(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M")
    link = f"https://t.me/{username}/{msg.id}"
    views = getattr(msg, "views", "")
    return [title, summary, dt, f"Telegram @{username}", link, views]

def existing_links():
    links = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 5 and row[4]:
            links.add(row[4])
    return links

async def scan(username, since_id=0, days=7, limit=800):
    cutoff = datetime.utcnow() - timedelta(days=days)
    out, max_id = [], since_id
    hist = await client(GetHistoryRequest(
        peer=username, limit=limit, offset_date=None,
        offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0
    ))
    for m in hist.messages:
        mid = getattr(m, "id", 0)
        if mid <= since_id:
            continue
        if not m.message:
            continue
        if m.date.replace(tzinfo=None) < cutoff:
            continue
        if contains(m.message):
            out.append(to_row(m, username))
        max_id = max(max_id, mid)
    return out, max_id

async def main():
    await client.start()
    state = load_state()
    seen = existing_links()
    to_add = []

    for ch in CHANNELS:
        try:
            rows, max_id = await scan(ch, since_id=state.get(ch, 0))
            for r in rows:
                if r[4] not in seen:
                    seen.add(r[4])
                    to_add.append(r)
            state[ch] = max_id
        except FloodWaitError as e:
            time.sleep(int(e.seconds) + 1)
        except Exception:
            # не падаем на одном канале
            continue

    # пачками, чтобы не упереться в лимиты
    for i in range(0, len(to_add), 300):


ls -R
ls -R

cat > monitor_telegram_to_sheets.py <<'EOF'
import os, re, time, pytz, requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest

import gspread
from google.oauth2.service_account import Credentials

# -------- ENV --------
API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION  = os.getenv("TELETHON_SESSION")   # StringSession
TZ       = os.getenv("TZ", "Europe/Amsterdam")
SHEET    = os.getenv("GOOGLE_SHEET_NAME", "Admiral Mentions")

# -------- Google Sheets --------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
doc = gc.open(SHEET)
ws = doc.sheet1

HEADER = ["Название материала","Краткое изложение","Дата публикации","Источник (сайт/ТГ)","Ссылка","Просмотры"]
if not ws.get_all_values():
    ws.append_row(HEADER)

def get_state_ws():
    try:
        return doc.worksheet("state")
    except gspread.exceptions.WorksheetNotFound:
        st = doc.add_worksheet(title="state", rows=200, cols=2)
        st.update("A1:B1", [["channel","last_id"]])
        return st

state_ws = get_state_ws()

def load_state():
    m = {}
    for row in state_ws.get_all_values()[1:]:
        if len(row) >= 2 and row[0]:
            try:
                m[row[0]] = int(row[1])
            except:
                m[row[0]] = 0
    return m

def save_state(state):
    rows = [["channel","last_id"]] + [[k, str(v)] for k, v in state.items()]
    state_ws.clear()
    state_ws.update("A1", rows)

# -------- Каналы под мониторинг --------
CHANNELS = [
    "sehockey","vbros_po_bortu","khl_official_telegram","arena_breda",
    "besprokata","derzhiperedachu","hockeywithroman","erykalov_s_shaiboi",
]

# -------- Ключевые слова --------
BASE_KEYWORDS = ["Адмирал","хоккейный клуб Адмирал","хоккейная команда Адмирал","Владивосток"]
ROSTER_URL  = "https://hcadmiral.pro/team/players/"
COACHES_URL = "https://hcadmiral.pro/team/coaches/"

def fetch_names(url):
    try:
        html = requests.get(url, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        texts = {el.get_text(" ", strip=True) for el in soup.select("a,div,span")}
        names = []
        for t in texts:
            if not t:
                continue
            # русские ФИО 2-3 слова, отсеиваем служебные слова
            if re.search(r"[А-Яа-яЁё]", t) and len(t) <= 40 and (" " in t or "-" in t):
                if not re.search(r"тренер|вратар|защитник|нападающ|рост|вес|возраст|сезон|матч|главный|ассистент|команда|клуб", t, re.I):
                    parts = t.split()
                    if 2 <= len(parts) <= 3:
                        names.append(t)
        return sorted(set(names))
    except Exception:
        return []

PLAYERS = fetch_names(ROSTER_URL)
COACHES = fetch_names(COACHES_URL)
KEYWORDS = BASE_KEYWORDS + PLAYERS + COACHES
kw = re.compile("|".join(map(re.escape, KEYWORDS)), re.I)

# -------- Telegram --------
client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

def contains(text: str) -> bool:
    return bool(text and kw.search(text))

def to_row(msg, username):
    text = (msg.message or "").strip()
    title = (text.split("\n")[0] if text else "(без текста)")[:120]
    summary = text[:300]
    dt = msg.date.astimezone(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M")
    link = f"https://t.me/{username}/{msg.id}"
    views = getattr(msg, "views", "")
    return [title, summary, dt, f"Telegram @{username}", link, views]

def existing_links():
    links = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 5 and row[4]:
            links.add(row[4])
    return links

async def scan(username, since_id=0, days=7, limit=800):
    cutoff = datetime.utcnow() - timedelta(days=days)
    out, max_id = [], since_id
    hist = await client(GetHistoryRequest(
        peer=username, limit=limit, offset_date=None,
        offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0
    ))
    for m in hist.messages:
        mid = getattr(m, "id", 0)
        if mid <= since_id:
            continue
        if not m.message:
            continue
        if m.date.replace(tzinfo=None) < cutoff:
            continue
        if contains(m.message):
            out.append(to_row(m, username))
        max_id = max(max_id, mid)
    return out, max_id

async def main():
    await client.start()
    state = load_state()
    seen = existing_links()
    to_add = []

    for ch in CHANNELS:
        try:
            rows, max_id = await scan(ch, since_id=state.get(ch, 0))
            for r in rows:
                if r[4] not in seen:
                    seen.add(r[4])
                    to_add.append(r)
            state[ch] = max_id
        except FloodWaitError as e:
            time.sleep(int(e.seconds) + 1)
        except Exception:
            # не падаем на одном канале
            continue

    # пачками, чтобы не упереться в лимиты
    for i in range(0, len(to_add), 300):
        ws.append_rows(to_add[i:i+300], value_input_option="USER_ENTERED")

    save_state(state)
    print(f"Добавлено строк: {len(to_add)}")

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
