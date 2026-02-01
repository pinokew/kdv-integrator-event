import os
import logging
import shutil
import re
from flask import Flask, jsonify, request, abort, make_response
from io import BytesIO
from pymarc import parse_xml_to_array

from .config import setup_logging, KDV_API_TOKEN, INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED, DSPACE_UI_URL
from .koha import KohaClient
from .dspace import DSpaceClient

setup_logging()
logger = logging.getLogger("KDV-Core")

app = Flask(__name__)

def parse_marc_details(xml_data):
    try:
        reader = parse_xml_to_array(BytesIO(xml_data.encode('utf-8')))
        record = reader[0]
        title = record['245']['a'] if '245' in record else "Untitled"
        if '245' in record and 'b' in record['245']:
            title += " " + record['245']['b']
        author = record['100']['a'] if '100' in record else None
        
        # Спроба отримати Handle з 856$u
        handle = None
        if '856' in record and 'u' in record['856']:
            full_url = record['856']['u']
            # Шукаємо паттерн handle/123/456 або items/uuid
            # Для DSpace handle зазвичай виглядає як prefix/suffix
            match = re.search(r'handle/(\d+/\d+)', full_url)
            if match:
                handle = match.group(1)
        
        return title, author, handle
    except Exception as e:
        logger.warning(f"Could not parse MARC details: {e}")
        return "Unknown Title", None, None

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-KDV-TOKEN, Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    return response

@app.before_request
def check_security():
    if request.path.endswith('/health'):
        return
    if request.method == 'OPTIONS':
        return make_response(jsonify({'status': 'cors_ok'}), 200)

    token = request.headers.get('X-KDV-TOKEN')
    if not token or token != KDV_API_TOKEN:
        logger.warning(f"⛔ Unauthorized access attempt from {request.remote_addr} [{request.method} {request.path}]")
        abort(401, description="Invalid or missing X-KDV-TOKEN")

@app.route('/kdv/api/health', methods=['GET'])
def healthcheck():
    return jsonify({"status": "ok", "mode": "production-ready"})

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['POST'])
def archive_record(biblionumber):
    logger.info(f"📥 POST REQUEST: Archive Biblio #{biblionumber}")
    koha = KohaClient()
    dspace = DSpaceClient()

    try:
        raw_xml = koha._get_biblio_xml(biblionumber)
        if not raw_xml: return jsonify({"status": "error", "message": "Biblio not found"}), 404

        meta = koha.get_biblio_metadata(biblionumber)
        if not meta: return jsonify({"status": "error", "message": "No 956 field"}), 400

        if meta['status'] in ['processing', 'imported']:
             return jsonify({"status": "error", "message": f"Item is already {meta['status']}."}), 409

        file_rel_path = meta['file_path']
        full_path = os.path.join(INTEGRATOR_MOUNT_PATH, file_rel_path)
        
        if not os.path.exists(full_path):
             koha.set_status(biblionumber, 'error', f"File not found: {file_rel_path}")
             return jsonify({"status": "error", "message": "File not found"}), 400

        koha.set_status(biblionumber, 'processing', 'Integrator started...')

        title, author, _ = parse_marc_details(raw_xml)
        collection_uuid = meta['collection_uuid']
        if not collection_uuid: raise Exception("Collection UUID missing")

        item_data = dspace.create_item_direct(collection_uuid, title, author)
        if not item_data: raise Exception("Failed to create item in DSpace")

        item_uuid = item_data['uuid']
        handle = item_data.get('handle')
        final_link = f"{DSPACE_UI_URL}/handle/{handle}" if handle else f"{DSPACE_UI_URL}/items/{item_uuid}"

        upload_success = dspace.upload_to_item(item_uuid, full_path)
        if not upload_success: raise Exception("Failed to upload file")

        koha.set_success(biblionumber, final_link)

        try:
            processed_dir = os.path.join(INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED)
            os.makedirs(processed_dir, exist_ok=True)
            shutil.move(full_path, os.path.join(processed_dir, os.path.basename(full_path)))
        except: pass

        return jsonify({"status": "success", "handle": final_link})

    except Exception as e:
        logger.error(f"CRITICAL ERROR on #{biblionumber}: {e}")
        try: koha.set_status(biblionumber, 'error', str(e))
        except: pass
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['PUT'])
def update_record(biblionumber):
    logger.info(f"📥 PUT REQUEST: Update Metadata Biblio #{biblionumber}")
    koha = KohaClient()
    dspace = DSpaceClient()

    try:
        # 1. Get Metadata from Koha
        raw_xml = koha._get_biblio_xml(biblionumber)
        if not raw_xml: return jsonify({"status": "error", "message": "Biblio not found"}), 404
        
        title, author, handle = parse_marc_details(raw_xml)
        
        if not handle:
             return jsonify({"status": "error", "message": "No DSpace Handle found in 856$u. Cannot update."}), 404

        # 2. Find Item UUID by Handle
        logger.info(f"Looking up UUID for handle: {handle}")
        item_uuid = dspace.find_item_uuid_by_handle(handle)
        
        if not item_uuid:
            # Fallback: Maybe the 856$u IS the items link with UUID?
            # Check logic later. For now, strict handle check.
            return jsonify({"status": "error", "message": f"DSpace Item not found for handle {handle}"}), 404

        # 3. Update Metadata
        success = dspace.update_metadata(item_uuid, title, author)
        
        if success:
            return jsonify({"status": "success", "message": "Metadata updated"}), 200
        else:
            return jsonify({"status": "error", "message": "Update failed at DSpace"}), 500

    except Exception as e:
        logger.error(f"UPDATE ERROR on #{biblionumber}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500