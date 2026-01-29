import logging
import os
import sys
import shutil
import time
from datetime import datetime
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import Annotated
from dotenv import load_dotenv

# Імпортуємо наші модулі
from src.config import INTEGRATOR_MOUNT_PATH, FOLDER_INBOX, FOLDER_PROCESSED
from src.koha import KohaClient
from src.dspace import DSpaceClient

load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KDV-API")

API_TOKEN = os.getenv("KDV_API_TOKEN")
if not API_TOKEN:
    logger.critical("KDV_API_TOKEN is missing!")

app = FastAPI(title="KDV Integrator API", version="3.0.0")

class IntegrationResponse(BaseModel):
    status: str
    message: str
    job_id: str

# --- AUTH ---
async def verify_token(x_kdv_token: Annotated[str, Header(alias="X-KDV-TOKEN")]):
    if not API_TOKEN:
        raise HTTPException(status_code=500, detail="Server config error")
    if x_kdv_token != API_TOKEN:
        logger.warning(f"⛔ Unauthorized access attempt. Token: {x_kdv_token}")
        raise HTTPException(status_code=401, detail="Invalid API Token")
    return x_kdv_token

# --- WORKER LOGIC ---
def process_biblio_task(bib_id: int):
    """
    Фонова задача: виконує реальну інтеграцію для однієї книги.
    """
    logger.info(f"🚀 [START] Job for Biblio {bib_id}")
    koha = KohaClient()
    dspace = DSpaceClient()

    # 1. Читаємо дані з Koha
    # Ми не скануємо список, а беремо конкретну книгу
    info = koha.get_integration_data(bib_id)
    
    if not info:
        logger.error(f"Bib {bib_id}: Metadata parse failed or no file info (956 missing?).")
        return

    # Anti-Double-Click: Якщо вже оброблено, виходимо
    if info['status'] == 'imported':
        logger.info(f"Bib {bib_id} already imported. Skipping.")
        return
    
    if info['status'] == 'processing':
        # Це спірний момент. Якщо процес впав, статус може зависнути.
        # Для MVP дозволимо перезапуск, якщо минуло багато часу (але поки просто логуємо)
        logger.warning(f"Bib {bib_id} has 'processing' status. Retrying anyway.")

    # Ставимо статус 'processing'
    if not koha.update_status(bib_id, "processing", f"Job started at {datetime.now()}"):
        logger.error(f"Failed to update status for {bib_id}. Aborting.")
        return

    try:
        # 2. Перевірка файлу
        # Шлях в Koha: Inbox/file.pdf -> Реальний: /mnt/drive/Inbox/file.pdf
        rel_path = info['file_path']
        if not rel_path:
            koha.update_status(bib_id, "error", "No file path in 956$u")
            return

        clean_path = rel_path.lstrip('/')
        # Якщо шлях вже містить Inbox, не дублюємо
        if clean_path.startswith(f"{FOLDER_INBOX}/"):
            full_path = os.path.join(INTEGRATOR_MOUNT_PATH, clean_path)
        else:
            full_path = os.path.join(INTEGRATOR_MOUNT_PATH, FOLDER_INBOX, clean_path)

        if not os.path.exists(full_path):
            msg = f"File not found: {full_path}"
            logger.error(msg)
            koha.update_status(bib_id, "error", msg)
            return

        # 3. DSpace Creation
        coll_uuid = info['collection_uuid']
        if not coll_uuid:
            koha.update_status(bib_id, "error", "Missing Collection UUID")
            return

        # Створення
        item = dspace.create_item_direct(coll_uuid, info['title'], info['author'])
        if not item:
            koha.update_status(bib_id, "error", "DSpace Create Failed")
            return

        item_uuid = item['uuid']
        handle = item.get('handle')

        # 4. Upload File
        if not dspace.upload_to_item(item_uuid, full_path):
            koha.update_status(bib_id, "error", "DSpace Upload Failed")
            return

        # 5. Success
        final_link = f"https://repo.fby.com.ua/handle/{handle}" if handle else f"https://repo.fby.com.ua/items/{item_uuid}"
        
        logger.info(f"✅ Success! Link: {final_link}")
        
        # Оновлюємо Koha
        koha.update_status(bib_id, "imported", f"OK {datetime.now().strftime('%Y-%m-%d %H:%M')}", handle_url=final_link)

        # Переміщуємо файл
        try:
            filename = os.path.basename(full_path)
            dest_dir = os.path.join(INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED)
            if not os.path.exists(dest_dir): os.makedirs(dest_dir)
            
            dest_path = os.path.join(dest_dir, filename)
            if os.path.exists(dest_path):
                dest_path = os.path.join(dest_dir, f"{int(time.time())}_{filename}")
            
            shutil.move(full_path, dest_path)
            logger.info(f"📂 File moved to {dest_path}")
        except Exception as e:
            logger.error(f"File move failed: {e}")

    except Exception as e:
        logger.error(f"🔥 Critical Error processing {bib_id}: {e}")
        koha.update_status(bib_id, "error", f"System Error: {str(e)}")

# --- API ---

@app.get("/")
def read_root():
    return {"status": "online", "mode": "event-driven v3"}

@app.post("/v1/integrate/{biblionumber}", status_code=202, response_model=IntegrationResponse)
async def integrate_biblio(
    biblionumber: int, 
    background_tasks: BackgroundTasks,
    token: str = Depends(verify_token)
):
    logger.info(f"📨 Trigger received for Biblio: {biblionumber}")
    
    # Запускаємо задачу у фоні
    background_tasks.add_task(process_biblio_task, biblionumber)
    
    return {
        "status": "accepted", 
        "message": "Integration task queued",
        "job_id": f"job-{biblionumber}-{int(time.time())}"
    }