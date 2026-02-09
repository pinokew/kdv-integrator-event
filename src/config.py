import os
import logging
import sys

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))
except ImportError:
    pass

def get_env(key: str, required: bool = True, default: str = None) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise ValueError(f"CRITICAL ERROR: Environment variable '{key}' is missing.")
    return val

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True 
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- КОНФІГУРАЦІЯ ---

KDV_API_TOKEN = get_env("KDV_API_TOKEN")

KOHA_API_URL = get_env("KOHA_API_URL").rstrip('/')
KOHA_OPAC_URL = get_env("KOHA_OPAC_URL").rstrip('/')
KOHA_USER = get_env("KOHA_API_USER")
KOHA_PASS = get_env("KOHA_API_PASS")

DSPACE_API_URL = get_env("DSPACE_API_URL").rstrip('/')
# Додано URL для фронтенду (UI) DSpace, щоб формувати красиві посилання
DSPACE_UI_URL = get_env("DSPACE_UI_URL").rstrip('/')

DSPACE_USER = get_env("DSPACE_API_USER")
DSPACE_PASS = get_env("DSPACE_API_PASS")

DSPACE_SUBMISSION_SECTION = get_env("DSPACE_SUBMISSION_SECTION", required=False, default="traditionalpageone")

INTEGRATOR_MOUNT_PATH = get_env("INTEGRATOR_MOUNT_PATH", default="/mnt/drive")
FOLDER_PROCESSED = get_env("FOLDER_PROCESSED", default="Processed")
FOLDER_ERROR = get_env("FOLDER_ERROR", default="Error")

TIMEOUT = 30
UPLOAD_TIMEOUT = 300