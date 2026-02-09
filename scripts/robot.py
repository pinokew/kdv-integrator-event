# запуск робота для масової архівації:
# docker compose exec kdv-api python3 -m src.robot

import requests
import time
import logging
import sys
import os
from .config import KDV_API_TOKEN


# Налаштування логування
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ROBOT] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "robot_batch.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Robot")

API_BASE = "http://localhost:5000/kdv/api"
HEADERS = {"X-KDV-TOKEN": KDV_API_TOKEN}
POLL_INTERVAL = 3  # секунди перерви між опитуванням статусу
BATCH_DELAY = 5    # секунди перерви між книгами (щоб не "покласти" DSpace)

def parse_candidates(filename):
    """
    Парсить файл candidates.txt, підтримуючи діапазони та списки.
    Приклади рядків у файлі:
      14
      20, 21, 25
      100-110
      300-305, 400
    """
    if not os.path.exists(filename):
        logger.error(f"File {filename} not found!")
        return []

    unique_ids = set()

    with open(filename, 'r') as f:
        for line in f:
            # Видаляємо коментарі та зайві пробіли
            line = line.split('#')[0].strip()
            if not line: continue

            # Розбиваємо по комі (якщо є перелік в одному рядку)
            parts = line.split(',')
            
            for part in parts:
                part = part.strip()
                if not part: continue

                # Перевірка на діапазон (наприклад "14-30")
                if '-' in part:
                    try:
                        start_s, end_s = part.split('-')
                        start = int(start_s)
                        end = int(end_s)
                        
                        # Захист від "30-14" (міняємо місцями)
                        if start > end: start, end = end, start
                        
                        # Додаємо весь діапазон (включно з останнім)
                        for i in range(start, end + 1):
                            unique_ids.add(i)
                    except ValueError:
                        logger.error(f"⚠️ Invalid range format ignored: '{part}'")
                
                # Звичайне число
                elif part.isdigit():
                    unique_ids.add(int(part))
                else:
                    logger.warning(f"⚠️ Invalid ID format ignored: '{part}'")

    # Повертаємо відсортований список рядків
    sorted_ids = sorted(list(unique_ids))
    return [str(i) for i in sorted_ids]

def process_single_biblio(biblionumber):
    """
    Виконує повний цикл архівації для однієї книги:
    POST (Start) -> Polling (Wait) -> Result
    """
    logger.info(f"▶️ Processing Biblio #{biblionumber}...")

    # 1. Ініціація (POST)
    try:
        resp = requests.post(f"{API_BASE}/integrate/{biblionumber}", headers=HEADERS)
        
        # Обробка статусів HTTP
        if resp.status_code == 409:
            # 409 Conflict: вже обробляється або заблоковано
            logger.warning(f"⚠️ #{biblionumber} SKIPPED: Already processed/locked.")
            return "SKIPPED"
        
        if resp.status_code == 400 or resp.status_code == 404:
             logger.error(f"❌ #{biblionumber} CLIENT ERROR: {resp.json().get('message')}")
             return "ERROR_CLIENT"

        if resp.status_code != 202:
            logger.error(f"❌ #{biblionumber} POST Failed ({resp.status_code}): {resp.text}")
            return "ERROR_POST"
            
        data = resp.json()
        task_id = data.get('task_id')
        if not task_id:
            logger.error(f"❌ #{biblionumber} No task_id returned!")
            return "ERROR_NO_TASK"
            
        logger.info(f"   Task started: {task_id}. Waiting...")

    except Exception as e:
        logger.error(f"❌ #{biblionumber} Connection Error: {e}")
        return "ERROR_CONN"

    # 2. Очікування (Polling)
    waited = 0
    max_wait = 900 # 15 хвилин максимум (для дуже великих файлів)
    
    while waited < max_wait:
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL
        
        try:
            status_resp = requests.get(f"{API_BASE}/status/{task_id}", headers=HEADERS)
            
            if status_resp.status_code == 404:
                 # Інколи буває race condition, спробуємо ще раз
                 continue
            
            if status_resp.status_code != 200:
                logger.warning(f"   Status check failed ({status_resp.status_code}). Retrying...")
                continue
                
            s_data = status_resp.json()
            status = s_data.get('status')
            
            if status == 'success':
                res = s_data.get('result', {})
                handle = res.get('handle')
                uuid = res.get('uuid')
                special_status = res.get('status') # linked_existing?
                
                if special_status == 'linked_existing':
                    logger.info(f"🔄 #{biblionumber} LINKED (Duplicate): {handle}")
                    return "LINKED"
                else:
                    logger.info(f"✅ #{biblionumber} SUCCESS! Handle: {handle}")
                    return "SUCCESS"
            
            elif status == 'error':
                err_msg = s_data.get('error')
                logger.error(f"❌ #{biblionumber} FAILED: {err_msg}")
                return "FAILED"
            
            # Якщо processing/queued - чекаємо далі
            
        except Exception as e:
            logger.warning(f"   Polling exception: {e}")

    logger.error(f"❌ #{biblionumber} TIMEOUT (waited {max_wait}s)")
    return "TIMEOUT"

def run_batch(filename="candidates.txt"):
    ids = parse_candidates(filename)
    
    if not ids:
        logger.warning("No candidates found via parse logic. Exiting.")
        return

    logger.info("="*40)
    logger.info(f"📋 BATCH STARTED. Candidates: {len(ids)}")
    logger.info(f"   List: {', '.join(ids[:10])} ...") # Показати перші 10
    logger.info("="*40)
    
    stats = {
        "SUCCESS": 0, 
        "FAILED": 0, 
        "SKIPPED": 0, 
        "LINKED": 0, 
        "TIMEOUT": 0,
        "ERROR_CLIENT": 0,
        "ERROR_CONN": 0
    }
    
    for i, bib_id in enumerate(ids):
        logger.info(f"--- Item {i+1}/{len(ids)} ---")
        result = process_single_biblio(bib_id)
        
        # Спрощення статистики для звіту
        key = result if result in stats else "FAILED"
        stats[key] = stats.get(key, 0) + 1
        
        # Пауза між книгами, щоб DSpace встиг "видихнути" (індексація Solr)
        if i < len(ids) - 1:
            time.sleep(BATCH_DELAY)

    logger.info("="*40)
    logger.info(f"🏁 BATCH COMPLETED.")
    logger.info(f"📊 Stats: {stats}")
    logger.info(f"📝 See full details in robot_batch.log")

if __name__ == "__main__":
    # Для запуску: docker compose exec kdv-api python3 -m src.robot
    run_batch("candidates.txt")