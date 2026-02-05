import os
import logging
import shutil
import re
from flask import Flask, jsonify, request, abort, make_response
from flask_cors import CORS
from io import BytesIO
from pymarc import parse_xml_to_array

from .tasks import task_manager
from .config import setup_logging, KDV_API_TOKEN, INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED, FOLDER_ERROR, DSPACE_UI_URL
from .mapping import METADATA_RULES, TYPE_CONVERSION
from .koha import KohaClient
from .dspace import DSpaceClient
from .covers import CoverService  # 🟢 Етап 6: Імпорт сервісу обкладинок

setup_logging()
logger = logging.getLogger("KDV-Core")

app = Flask(__name__)
# Додаємо CORS для браузера
CORS(app)

LIMIT_WARNING = 150 * 1024 * 1024
LIMIT_ERROR = 250 * 1024 * 1024

def get_versioned_path(base_dir, biblionumber):
    """
    Генерує унікальний шлях для файлу з версійністю.
    Патерн: {base_dir}/Processed/biblio_{id}_v{XX}.pdf
    """
    target_dir = os.path.join(base_dir, FOLDER_PROCESSED)
    os.makedirs(target_dir, exist_ok=True)
    
    version = 1
    while True:
        filename = f"biblio_{biblionumber}_v{version:02d}.pdf"
        full_path = os.path.join(target_dir, filename)
        
        if not os.path.exists(full_path):
            return full_path
        
        version += 1
        if version > 999:
            return os.path.join(target_dir, f"biblio_{biblionumber}_v999_overflow_{os.urandom(4).hex()}.pdf")

def parse_marc_details(xml_data):
    """Універсальний парсер MARC на основі правил з mapping.py."""
    try:
        reader = parse_xml_to_array(BytesIO(xml_data.encode('utf-8')))
        record = reader[0]
        extracted_data = {}

        for dspace_field, rule in METADATA_RULES.items():
            values = []
            sources = rule.get('sources', [{"tag": rule.get("tag"), "subfield": rule.get("subfield")}])
            
            for src in sources:
                tag = src.get('tag')
                sub = src.get('subfield')
                if not tag or tag not in record: continue

                if rule.get('multivalue'):
                    for field in record.get_fields(tag):
                        val = field[sub] if sub in field else None
                        if val: values.append(val)
                else:
                    val = record[tag][sub] if sub in record[tag] else None
                    if val:
                        values.append(val)
                        break 

            final_values = []
            for v in values:
                if 'regex' in rule:
                    match = re.search(rule['regex'], v)
                    if match: v = match.group(1)
                    else: continue
                if 'conversion' in rule and rule['conversion'] == 'type':
                    v = TYPE_CONVERSION.get(v, TYPE_CONVERSION.get("DEFAULT"))
                final_values.append(v)

            if final_values:
                if rule.get('multivalue'):
                    extracted_data[dspace_field] = final_values
                else:
                    extracted_data[dspace_field] = final_values[0]

        handle = None
        if '856' in record and 'u' in record['856']:
            full_url = record['856']['u']
            match = re.search(r'handle/(\d+/\d+)', full_url)
            if match: handle = match.group(1)
        extracted_data['handle'] = handle

        return extracted_data
    except Exception as e:
        logger.warning(f"Could not parse MARC details: {e}")
        return {}

def process_integration_logic(task_id, biblionumber):
    logger.info(f"⚙️ [Thread] Processing Biblio #{biblionumber}")
    koha = KohaClient()
    dspace = DSpaceClient()
    
    # Ініціалізація сервісу обкладинок з поточним клієнтом Koha
    cover_service = CoverService(koha_client=koha)
    
    # current_active_path буде зберігати актуальне місцезнаходження файлу
    current_active_path = None

    try:
        # --- 1. CHECKS & METADATA ---
        meta = koha.get_biblio_metadata(biblionumber)
        if not meta: raise Exception("No 956 field found")

        file_rel_path = meta['file_path']
        original_full_path = os.path.join(INTEGRATOR_MOUNT_PATH, file_rel_path)
        
        if not os.path.exists(original_full_path):
            koha.set_status(biblionumber, 'error', f"File missing: {file_rel_path}")
            raise Exception("File not found on disk")

        # Size Policy
        file_size = os.path.getsize(original_full_path)
        if file_size > LIMIT_ERROR:
            msg = f"FILE TOO LARGE ({round(file_size/1024/1024)} MB)"
            koha.set_status(biblionumber, 'error', msg)
            raise Exception(msg)
        if file_size > LIMIT_WARNING:
            koha.set_status(biblionumber, None, f"Warning: {round(file_size/1024/1024)} MB")

        # --- 🟢 2. RENAME FIRST (Move to Processed) ---
        source_dir = os.path.dirname(original_full_path)
        versioned_path = get_versioned_path(source_dir, biblionumber)
        
        logger.info(f"📂 Renaming and moving to Processed: {versioned_path}")
        shutil.move(original_full_path, versioned_path)
        
        # Тепер ми працюємо з файлом у папці Processed
        current_active_path = versioned_path

        # --- 🟢 2.1 COVER AUTOMATOR (Phase 6) ---
        # Виконуємо генерацію ПІСЛЯ переміщення файлу, щоб читати стабільний шлях.
        try:
            logger.info(f"🎨 [Cover] Starting generation for #{biblionumber}")
            # Визначаємо папку для збереження (Processed або поруч з файлом)
            pdf_dir = os.path.dirname(current_active_path)
            
            cover_result = cover_service.process_book(
                biblionumber=str(biblionumber),
                pdf_path=current_active_path,
                output_base_dir=pdf_dir
            )
            logger.info(f"🖼️ [Cover] Service Result: {cover_result}")
        except Exception as cover_e:
            # Stability Guard: Не зупиняємо основний процес
            logger.warning(f"⚠️ [Cover] Generation failed (continuing integration): {cover_e}")
        # ----------------------------------------

        # --- 3. PREPARE METADATA ---
        raw_xml = koha._get_biblio_xml(biblionumber)
        md = parse_marc_details(raw_xml)
        md['koha.biblionumber'] = str(biblionumber)
        
        logger.info(f"Parsed Metadata: {md}")

        collection_uuid = meta['collection_uuid']
        if not collection_uuid: raise Exception("Collection UUID missing")

        # --- 4. DUPLICATE CHECK ---
        existing_item = dspace.find_item_by_biblionumber(biblionumber)
        if existing_item:
            logger.warning(f"🔄 Item already exists (UUID: {existing_item['uuid']}). Linking only.")
            item_uuid = existing_item['uuid']
            handle = existing_item.get('handle')
            final_link = f"{DSPACE_UI_URL}/handle/{handle}" if handle else f"{DSPACE_UI_URL}/items/{item_uuid}"
            
            koha.set_success(biblionumber, final_link, item_uuid=item_uuid)
            # Файл вже у Processed, все добре.
            return {"handle": final_link, "uuid": item_uuid, "status": "linked_existing"}

        # --- 5. CREATE ITEM ---
        item_data = dspace.create_item_direct(collection_uuid, md)
        if not item_data: raise Exception("Failed to create item in DSpace")

        item_uuid = item_data['uuid']
        handle = item_data.get('handle')
        final_link = f"{DSPACE_UI_URL}/handle/{handle}" if handle else f"{DSPACE_UI_URL}/items/{item_uuid}"

        # --- 🟢 6. UPLOAD (Using Renamed File) ---
        # Тепер DSpace отримає файл з ім'ям "biblio_123_v01.pdf"
        if not dspace.upload_to_item(item_uuid, current_active_path):
            raise Exception("Failed to upload file")

        # --- 7. FINALIZE ---
        koha.set_success(biblionumber, final_link, item_uuid=item_uuid)

        return {"handle": final_link, "uuid": item_uuid}

    except Exception as e:
        logger.error(f"❌ Logic Error processing #{biblionumber}: {e}")
        try: koha.set_status(biblionumber, 'error', str(e))
        except: pass
        
        # 🔴 8. ERROR HANDLING (Move from Processed to Error)
        # Якщо файл вже був переміщений у Processed (current_active_path), 
        # але сталася помилка — переміщаємо його в Error.
        if current_active_path and os.path.exists(current_active_path):
            try:
                source_dir = os.path.dirname(current_active_path) # Це папка Processed
                # Error папка має бути на рівень вище (поруч з Processed, а не всередині)
                parent_dir = os.path.dirname(source_dir) 
                error_dir = os.path.join(parent_dir, FOLDER_ERROR)
                os.makedirs(error_dir, exist_ok=True)
                
                # Зберігаємо версійне ім'я, щоб знати, яка спроба провалилась
                filename = os.path.basename(current_active_path)
                error_dest = os.path.join(error_dir, filename)
                
                logger.info(f"⚠️ Moving failed file to Error folder: {error_dest}")
                shutil.move(current_active_path, error_dest)
            except Exception as move_err:
                logger.error(f"Failed to move file to Error folder: {move_err}")

        raise e

# --- ENDPOINTS ---
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-KDV-TOKEN, Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    return response

@app.before_request
def check_security():
    if request.path.endswith('/health') or request.method == 'OPTIONS': return
    if request.headers.get('X-KDV-TOKEN') != KDV_API_TOKEN:
        abort(401, description="Invalid Token")

@app.route('/kdv/api/health', methods=['GET'])
def healthcheck(): return jsonify({"status": "ok", "mode": "v6.5-rename-first"})

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['POST'])
def archive_record_async(biblionumber):
    koha = KohaClient()
    try:
        koha.set_status(biblionumber, 'processing', 'Queued...')
        task_id = task_manager.start_task(process_integration_logic, biblionumber)
        return jsonify({"status": "accepted", "task_id": task_id}), 202
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/kdv/api/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    info = task_manager.get_status(task_id)
    return jsonify(info) if info else (jsonify({"status": "not_found"}), 404)

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['PUT'])
def update_record(biblionumber):
    koha = KohaClient()
    dspace = DSpaceClient()
    try:
        raw_xml = koha._get_biblio_xml(biblionumber)
        md = parse_marc_details(raw_xml)
        md['koha.biblionumber'] = str(biblionumber)
        
        meta = koha.get_biblio_metadata(biblionumber)
        item_uuid = meta.get('dspace_uuid')

        if not item_uuid and md.get('handle'):
            item_uuid = dspace.find_item_uuid_by_handle(md['handle'])

        if not item_uuid:
            existing = dspace.find_item_by_biblionumber(biblionumber)
            if existing: item_uuid = existing['uuid']

        if not item_uuid:
            return jsonify({"status": "error", "message": "Item not found"}), 404

        success = dspace.update_metadata(item_uuid, md)
        return jsonify({"status": "success"}) if success else (jsonify({"status": "error"}), 500)

    except Exception as e:
        logger.error(f"UPDATE ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500