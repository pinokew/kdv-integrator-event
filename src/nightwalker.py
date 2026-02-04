# Повне сканування (Авто-режим): Просто запусти скрипт без цифр. Він піде з 1 і зупиниться, коли база закінчиться (зустріне 200 "дірок" підряд).
# Bash

# docker compose exec kdv-api python3 -m src.nightwalker

# Ручний режим (якщо треба швидко перевірити конкретний шматок):
# Bash

# docker compose exec kdv-api python3 -m src.nightwalker 5000 5100

import logging
import sys
import time
import os
from datetime import datetime, timezone
from dateutil import parser 
from io import BytesIO
from pymarc import parse_xml_to_array

from .koha import KohaClient
from .dspace import DSpaceClient
from .app import parse_marc_details

# --- НАЛАШТУВАННЯ ЛОГУВАННЯ ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [WALKER] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "nightwalker.log")),
        logging.StreamHandler(sys.stdout)
    ],
    force=True 
)
logger = logging.getLogger("NightWalker")

# Кількість пустих ID підряд, після яких робот вважає, що база закінчилась
MAX_CONSECUTIVE_ERRORS = 201

def parse_date(date_str):
    """Парсинг ISO рядка (для DSpace)"""
    if not date_str: return None
    try:
        dt = parser.parse(date_str)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except: return None

def extract_koha_date_from_xml(xml_data):
    """Витягує дату з поля 005 MARC"""
    try:
        reader = parse_xml_to_array(BytesIO(xml_data.encode('utf-8')))
        record = reader[0]
        if '005' in record:
            f005 = record['005'].data
            dt_str = f005.split('.')[0] 
            return datetime.strptime(dt_str, "%Y%m%d%H%M%S")
    except: pass
    return None

def audit_record(biblionumber):
    """
    Перевіряє один запис.
    Повертає True, якщо запис існує в Koha (навіть якщо не синхронізований).
    Повертає False, якщо запису в Koha немає (404/Empty).
    """
    koha = KohaClient()
    dspace = DSpaceClient()

    try:
        # 1. Читаємо XML
        xml_data = koha._get_biblio_xml(biblionumber)
        if not xml_data: 
            return False # Запис не існує

        marc_data = parse_marc_details(xml_data)
        marc_data['koha.biblionumber'] = str(biblionumber)
        
        meta = koha.get_biblio_metadata(biblionumber)
        if not meta: 
            return False # Технічно запис є, але без метаданих (рідкісний випадок)

        koha_date = extract_koha_date_from_xml(xml_data)

    except Exception as e:
        logger.error(f"Error reading Koha #{biblionumber}: {e}")
        return False

    # === АУДИТ 1: DEAD LINK DETECTOR ===
    has_file = bool(meta.get('file_path'))
    has_handle = bool(marc_data.get('handle'))
    status = meta.get('status')

    if has_file and not has_handle and status not in ['processing', 'imported']:
        logger.warning(f"🧟 [ZOMBIE] #{biblionumber}: File exists but NO Handle!")
    
    # === АУДИТ 2: SYNC CHECK ===
    item_uuid = meta.get('dspace_uuid')
    
    if not item_uuid:
        found = dspace.find_item_by_biblionumber(biblionumber)
        if found: item_uuid = found['uuid']

    if item_uuid:
        dspace_date_str = dspace.get_item_last_modified(item_uuid)
        dspace_date = parse_date(dspace_date_str)

        if koha_date and dspace_date:
            diff = (koha_date - dspace_date).total_seconds()
            
            # Поріг 5 секунд
            if diff > 5:
                logger.info(f"🔄 [SYNC NEEDED] #{biblionumber}. Koha newer by {round(diff)}s. Updating...")
                success = dspace.update_metadata(item_uuid, marc_data)
                if success:
                    logger.info(f"✅ [SYNC SUCCESS] #{biblionumber} updated.")
                else:
                    logger.error(f"❌ [SYNC FAILED] #{biblionumber} update failed.")
    
    return True # Запис існує і був оброблений

def run_auto_mode():
    logger.info("="*40)
    logger.info(f"🌙 NIGHT WALKER STARTED (Auto-Discovery Mode)")
    logger.info(f"ℹ️  Will stop after {MAX_CONSECUTIVE_ERRORS} consecutive empty records.")
    logger.info("="*40)

    bib_id = 1
    gap_count = 0
    processed_count = 0

    while True:
        exists = audit_record(bib_id)
        
        if exists:
            gap_count = 0 # Скидаємо лічильник пропусків, бо знайшли живу книгу
            processed_count += 1
            # Логуємо кожні 100 записів для розуміння прогресу
            if processed_count % 100 == 0:
                logger.info(f"   ...scanned {bib_id} records...")
        else:
            gap_count += 1
        
        if gap_count >= MAX_CONSECUTIVE_ERRORS:
            logger.info(f"🛑 STOPPING: Hit {MAX_CONSECUTIVE_ERRORS} empty records in a row.")
            logger.info(f"   Last checked ID: {bib_id}")
            break

        bib_id += 1
        time.sleep(0.05) # Дуже коротка пауза для швидкості

    logger.info("="*40)
    logger.info("🏁 WALKER FINISHED.")

def run_range_mode(start_id, end_id):
    logger.info("="*40)
    logger.info(f"🌙 NIGHT WALKER STARTED (Range: {start_id}-{end_id})")
    logger.info("="*40)

    for bib_id in range(start_id, end_id + 1):
        audit_record(bib_id)
        time.sleep(0.1)

    logger.info("="*40)
    logger.info("🏁 WALKER FINISHED.")

if __name__ == "__main__":
    # Якщо передано аргументи - працюємо по діапазону
    if len(sys.argv) == 3:
        try:
            start = int(sys.argv[1])
            end = int(sys.argv[2])
            run_range_mode(start, end)
        except ValueError:
            print("Error: IDs must be integers.")
    # Якщо аргументів немає - працюємо в авто-режимі (все підряд)
    else:
        run_auto_mode()