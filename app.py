import logging
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KDV-API")

app = FastAPI(title="KDV Integrator API", version="3.0.0")

# --- МОДЕЛІ ДАНИХ ---
class IntegrationResponse(BaseModel):
    status: str
    message: str
    job_id: str

# --- ФОНОВІ ЗАДАЧІ (ЗАГЛУШКА) ---
def fake_integration_task(bib_id: int):
    """
    Тут пізніше буде код Daywalker.
    Зараз ми просто імітуємо бурхливу діяльність.
    """
    logger.info(f"🟢 [START] Processing Biblio {bib_id}...")
    import time
    time.sleep(5) # Імітуємо роботу (завантаження файлів)
    logger.info(f"🔴 [DONE] Biblio {bib_id} processed.")

# --- ENDPOINTS ---

@app.get("/")
def read_root():
    """Перевірка, чи живий сервіс."""
    return {"system": "KDV Integrator", "status": "online", "version": "3.0"}

@app.post("/v1/integrate/{biblionumber}", status_code=202)
async def integrate_biblio(
    biblionumber: int, 
    background_tasks: BackgroundTasks,
    x_kdv_token: Optional[str] = Header(None) # Поки що просто приймаємо, не валідуємо строго
):
    """
    Ендпоінт, який викликатиме кнопка в Koha.
    """
    logger.info(f"📨 Received request for Biblio: {biblionumber}")
    
    # 1. (Пізніше) Тут буде перевірка токена
    
    # 2. Запускаємо задачу у фоні (щоб не змушувати браузер чекати)
    background_tasks.add_task(fake_integration_task, biblionumber)
    
    # 3. Миттєво відповідаємо
    return {
        "status": "accepted", 
        "message": f"Integration started for biblio {biblionumber}",
        "job_id": f"job-{biblionumber}"
    }