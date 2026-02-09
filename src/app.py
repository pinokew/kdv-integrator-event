import os
import logging
import shutil
import re
import time  # 🟢 NEW: Потрібно для пауз при повторних спробах
import concurrent.futures
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from io import BytesIO
from pymarc import parse_xml_to_array

from .tasks import task_manager
from .config import setup_logging, KDV_API_TOKEN, KOHA_API_URL, INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED, FOLDER_ERROR, DSPACE_UI_URL
from .mapping import METADATA_RULES, TYPE_CONVERSION
from .koha import KohaClient
from .dspace import DSpaceClient
from .covers import CoverService

setup_logging()
logger = logging.getLogger("KDV-Core")

app = Flask(__name__)
CORS(app)

LIMIT_WARNING = 150 * 1024 * 1024
LIMIT_ERROR = 250 * 1024 * 1024

def get_versioned_path(base_dir, biblionumber):
    """Генерує унікальний шлях для файлу з версійністю."""
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
            return os.path.join(target_dir, f"biblio_{biblionumber}_v999_{os.urandom(4).hex()}.pdf")

def parse_marc_details(xml_data):
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
                extracted_data[dspace_field] = final_values if rule.get('multivalue') else final_values[0]
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

def run_dspace_workflow(biblionumber, file_path, meta):
    """
    THREAD: Critical DSpace Logic
    """
    local_koha = KohaClient()
    local_dspace = DSpaceClient()
    
    logger.info(f"🚀 [DSpace-Thread] Starting metadata & upload for #{biblionumber}")
    
    raw_xml = local_koha._get_biblio_xml(biblionumber)
    md = parse_marc_details(raw_xml)
    md['koha.biblionumber'] = str(biblionumber)
    
    collection_uuid = meta['collection_uuid']
    if not collection_uuid: raise Exception("Collection UUID missing")

    existing_item = local_dspace.find_item_by_biblionumber(biblionumber)
    if existing_item:
        logger.warning(f"🔄 Item already exists (UUID: {existing_item['uuid']}). Linking only.")
        item_uuid = existing_item['uuid']
        handle = existing_item.get('handle')
        final_link = f"{DSPACE_UI_URL}/handle/{handle}" if handle else f"{DSPACE_UI_URL}/items/{item_uuid}"
        return {"handle": final_link, "uuid": item_uuid, "status": "linked_existing"}

    item_data = local_dspace.create_item_direct(collection_uuid, md)
    if not item_data: raise Exception("Failed to create item in DSpace")

    item_uuid = item_data['uuid']
    handle = item_data.get('handle')
    final_link = f"{DSPACE_UI_URL}/handle/{handle}" if handle else f"{DSPACE_UI_URL}/items/{item_uuid}"

    logger.info(f"📤 [DSpace-Thread] Uploading file to Item {item_uuid}")
    if not local_dspace.upload_to_item(item_uuid, file_path):
        raise Exception("Failed to upload file")

    logger.info(f"✅ [DSpace-Thread] Finished for #{biblionumber}")
    return {"handle": final_link, "uuid": item_uuid}

def process_integration_logic(task_id, biblionumber):
    logger.info(f"⚙️ [Core] Processing Biblio #{biblionumber}")
    koha = KohaClient()
    cover_service = CoverService(koha_client=koha)
    current_active_path = None

    try:
        # --- 1. SERIAL PHASE: Checks & Rename ---
        meta = koha.get_biblio_metadata(biblionumber)
        if not meta: raise Exception("No 956 field found")

        file_rel_path = meta['file_path']
        original_full_path = os.path.join(INTEGRATOR_MOUNT_PATH, file_rel_path)
        
        if not os.path.exists(original_full_path):
            koha.set_status(biblionumber, 'error', f"File missing: {file_rel_path}")
            raise Exception("File not found on disk")

        file_size = os.path.getsize(original_full_path)
        if file_size > LIMIT_ERROR:
            msg = f"FILE TOO LARGE ({round(file_size/1024/1024)} MB)"
            koha.set_status(biblionumber, 'error', msg)
            raise Exception(msg)
        if file_size > LIMIT_WARNING:
            koha.set_status(biblionumber, None, f"Warning: {round(file_size/1024/1024)} MB")

        source_dir = os.path.dirname(original_full_path)
        versioned_path = get_versioned_path(source_dir, biblionumber)
        
        logger.info(f"📂 [Core] Renaming to: {versioned_path}")
        shutil.move(original_full_path, versioned_path)
        current_active_path = versioned_path

        # --- ⚡ 2. PARALLEL PHASE: DSpace + Cover ---
        dspace_result = None
        cover_url = None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Task A: Cover
            pdf_dir = os.path.dirname(current_active_path)
            future_cover = executor.submit(cover_service.process_book, str(biblionumber), current_active_path, pdf_dir)
            
            # Task B: DSpace
            future_dspace = executor.submit(run_dspace_workflow, biblionumber, current_active_path, meta)
            
            logger.info("⚡ [Core] Parallel tasks started: Cover + DSpace")

            # Check Critical Task (DSpace)
            try:
                dspace_result = future_dspace.result()
            except Exception as e:
                logger.error(f"❌ [Core] DSpace Thread failed: {e}")
                raise e

            # Check Bonus Task (Cover)
            try:
                cover_res = future_cover.result(timeout=10)
                logger.info(f"🖼️ [Core] Cover result: {cover_res}")
                
                # 🟢 NEW: Retry Logic для отримання URL
                if cover_res.get('status') in ['success', 'skipped']:
                     # Робимо до 3 спроб з паузою, щоб Koha API встигло побачити картинку
                     for attempt in range(3):
                         real_url = koha.get_cover_image_url(biblionumber)
                         if real_url:
                             logger.info(f"🔗 [Core] Resolved Cover URL: {real_url}")
                             cover_url = real_url
                             break
                         else:
                             logger.info(f"⏳ [Core] Waiting for cover API index (attempt {attempt+1}/3)...")
                             time.sleep(1)
                     
            except concurrent.futures.TimeoutError:
                logger.warning("⚠️ [Core] Cover generation timeout.")
            except Exception as e:
                logger.warning(f"⚠️ [Core] Cover Thread warning: {e}")

        # --- 3. FINALIZE ---
        if dspace_result:
            koha.set_success(
                biblionumber, 
                dspace_result['handle'], 
                item_uuid=dspace_result['uuid'],
                cover_url=cover_url 
            )

        return dspace_result

    except Exception as e:
        logger.error(f"❌ [Core] Logic Error processing #{biblionumber}: {e}")
        try: koha.set_status(biblionumber, 'error', str(e))
        except: pass
        
        if current_active_path and os.path.exists(current_active_path):
            try:
                source_dir = os.path.dirname(current_active_path) 
                parent_dir = os.path.dirname(source_dir) 
                error_dir = os.path.join(parent_dir, FOLDER_ERROR)
                os.makedirs(error_dir, exist_ok=True)
                filename = os.path.basename(current_active_path)
                shutil.move(current_active_path, os.path.join(error_dir, filename))
            except Exception as move_err:
                logger.error(f"Failed to move file to Error folder: {move_err}")
        raise e

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
def healthcheck(): return jsonify({"status": "ok", "mode": "v6.5-parallel-covers"})

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['POST'])
def archive_record_async(biblionumber):
    try:
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