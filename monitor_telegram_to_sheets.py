import os, time, pytz, requests
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
SHEET    = os.getenv("GOOGLE_SHEET_NAME", os.getenv("GSHEET_NAME", "Admiral Mentions"))

# Авторизация в Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",  # нужен для gc.open(SHEET) по названию
]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open(SHEET).sheet1

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

channels = [
    "@sehockey", "@vbros_po_bortu", "@khl_official_telegram", "@arena_breda",
    "@besprokata", "@derzhiperedachu", "@hockeywithroman", "@erykalov_s_shaiboi"
]

keywords = [
    "Адмирал", "хоккейная команда Адмирал", "хоккейный клуб Адмирал"
]

def contains_keywords(text):
    return any(kw.lower() in text.lower() for kw in keywords)

async def main():
    tz = pytz.timezone(TZ)
    now = datetime.now(tz)
    since = now - timedelta(days=1)
    to_add = []

    for ch in channels:
        try:
            entity = await client.get_entity(ch)
            hist = await client(GetHistoryRequest(
                peer=entity,
                limit=50,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            for msg in hist.messages:
                if not msg.message:
                    continue
                # фильтруем по ключевым словам
                if contains_keywords(msg.message):
                    date_str = msg.date.astimezone(tz).strftime("%Y-%m-%d %H:%M")
                    to_add.append([
                        date_str,
                        ch,
                        msg.message[:100] + ("..." if len(msg.message) > 100 else ""),
                        f"https://t.me/{ch.lstrip('@')}/{msg.id}"
                    ])
        except FloodWaitError as e:
            print(f"FloodWait: ждём {e.seconds} секунд")
            time.sleep(int(e.seconds) + 1)
        except Exception as e:
            print(f"Ошибка на канале {ch}: {e}")
            continue

    # пачками, чтобы не упереться в лимиты Google Sheets
    if to_add:
        for i in range(0, len(to_add), 300):
            batch = to_add[i:i+300]
            try:
                sheet.append_rows(batch, value_input_option="RAW")
                print(f"✅ Добавлено {len(batch)} строк в Google Sheets")
            except Exception as e:
                print(f"❌ Ошибка при добавлении пачки {i//300 + 1}: {e}")
    else:
        print("Нет новых совпадений по ключевым словам за последние ~50 сообщений в каналах.")

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
