from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("Enter api_id: "))
api_hash = input("Enter api_hash: ")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\n=== YOUR TELETHON STRING SESSION ===")
    print(client.session.save())
    print("====================================")
