import os
import sys
import logging
from dotenv import load_dotenv

# Завантажуємо .env (він у корені, на рівень вище src)
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

def get_env(key: str, default: str = None) -> str:
    val = os.getenv(key, default)
    if not val and default is None:
        logging.error(f"❌ CRITICAL: Environment variable '{key}' is missing.")
    return val

# --- ШЛЯХИ ---
INTEGRATOR_MOUNT_PATH = get_env("INTEGRATOR_MOUNT_PATH", default="/mnt/drive")
FOLDER_INBOX = get_env("FOLDER_INBOX", default="Inbox")
FOLDER_PROCESSED = get_env("FOLDER_PROCESSED", default="Processed")
FOLDER_ERROR = get_env("FOLDER_ERROR", default="Error")

# --- СЕРВІСИ ---
KOHA_API_URL = get_env("KOHA_API_URL", "").rstrip('/')
KOHA_USER = get_env("KOHA_API_USER")
KOHA_PASS = get_env("KOHA_API_PASS")

DSPACE_API_URL = get_env("DSPACE_API_URL", "").rstrip('/')
DSPACE_USER = get_env("DSPACE_API_USER")
DSPACE_PASS = get_env("DSPACE_API_PASS")

# --- НАЛАШТУВАННЯ ---
TIMEOUT = 30
UPLOAD_TIMEOUT = 300