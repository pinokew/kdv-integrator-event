import logging
import os
import sys
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import Annotated
from dotenv import load_dotenv

# Завантажуємо змінні з .env
load_dotenv()

# Отримуємо токен. Якщо його немає - це критична помилка конфігурації.
API_TOKEN = os.getenv("KDV_API_TOKEN")
if not API_TOKEN:
    print("❌ CRITICAL ERROR: KDV_API_TOKEN is not set in .env!")
    # Можна зробити sys.exit(1), але краще залишити, щоб бачити логи

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KDV-API")

app = FastAPI(title="KDV Integrator API", version="3.0.0")

# --- МОДЕЛІ ДАНИХ ---
class IntegrationResponse(BaseModel):
    status: str
    message: str
    job_id: str

# --- ФУНКЦІЯ ПЕРЕВІРКИ БЕЗПЕКИ ---
async def verify_token(x_kdv_token: Annotated[str, Header(alias="X-KDV-TOKEN")]):
    """
    Перевіряє токен.
    Annotated[str, Header(...)] робить заголовок ОБОВ'ЯЗКОВИМ.
    Якщо його немає - FastAPI автоматично поверне 422.
    """
    if not API_TOKEN:
        logger.error("API Token is not configured on server!")
        raise HTTPException(status_code=500, detail="Server misconfiguration")
        
    if x_kdv_token != API_TOKEN:
        logger.warning(f"⛔ Unauthorized access attempt. Token: {x_kdv_token}")
        raise HTTPException(status_code=401, detail="Invalid API Token")
    
    return x_kdv_token

# --- ФОНОВІ ЗАДАЧІ ---
def fake_integration_task(bib_id: int):
    logger.info(f"🟢 [START] Processing Biblio {bib_id}...")
    import time
    time.sleep(5) 
    logger.info(f"🔴 [DONE] Biblio {bib_id} processed.")

# --- ENDPOINTS ---

@app.get("/")
def read_root():
    """Публічний Healthcheck."""
    return {"system": "KDV Integrator", "status": "online", "version": "3.0", "security": "enabled"}

@app.post("/v1/integrate/{biblionumber}", status_code=202, response_model=IntegrationResponse)
async def integrate_biblio(
    biblionumber: int, 
    background_tasks: BackgroundTasks,
    # Тут ми явно викликаємо залежність і зберігаємо результат (хоча він нам не треба)
    token: str = Depends(verify_token)
):
    """
    Захищений ендпоінт.
    """
    logger.info(f"📨 Authorized request for Biblio: {biblionumber}")
    
    background_tasks.add_task(fake_integration_task, biblionumber)
    
    return {
        "status": "accepted", 
        "message": f"Integration started for biblio {biblionumber}",
        "job_id": f"job-{biblionumber}"
    }