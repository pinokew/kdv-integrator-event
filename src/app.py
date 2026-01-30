import os
import logging
import shutil
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from io import BytesIO
from pymarc import parse_xml_to_array

from .config import setup_logging, KDV_API_TOKEN, INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED, FOLDER_ERROR
from .koha import KohaClient
from .dspace import DSpaceClient


# Налаштування логування
setup_logging()
logger = logging.getLogger("KDV-Core")

app = Flask(__name__)
# Дозволяємо CORS
CORS(app)

def parse_marc_details(xml_data):
    try:
        reader = parse_xml_to_array(BytesIO(xml_data.encode('utf-8')))
        record = reader[0]
        title = record['245']['a'] if '245' in record else "Untitled"
        if '245' in record and 'b' in record['245']:
            title += " " + record['245']['b']
        author = record['100']['a'] if '100' in record else None
        return title, author
    except Exception as e:
        logger.warning(f"Could not parse MARC details: {e}")
        return "Unknown Title", None

@app.before_request
def check_security():
    if request.path.endswith('/health'):
        return
    token = request.headers.get('X-KDV-TOKEN')
    if not token or token != KDV_API_TOKEN:
        logger.warning(f"⛔ Unauthorized access attempt from {request.remote_addr}")
        abort(401, description="Invalid or missing X-KDV-TOKEN")

@app.route('/kdv/api/health', methods=['GET'])
def healthcheck():
    return jsonify({
        "status": "ok", 
        "mode": "production-ready"
    })

@app.route('/kdv/api/integrate/<int:biblionumber>', methods=['POST'])
def archive_record(biblionumber):
    logger.info(f"📥 REQUEST: Archive Biblio #{biblionumber}")
    koha = KohaClient()
    dspace = DSpaceClient()

    try:
        raw_xml = koha._get_biblio_xml(biblionumber)
        if not raw_xml:
            return jsonify({"status": "error", "message": "Biblio not found in Koha"}), 404

        meta = koha.get_biblio_metadata(biblionumber)
        if not meta:
            return jsonify({"status": "error", "message": "No 956 field found"}), 400

        if meta['status'] in ['processing', 'imported']:
             return jsonify({
                 "status": "error", 
                 "message": f"Item is already {meta['status']}."
             }), 409

        file_rel_path = meta['file_path']
        full_path = os.path.join(INTEGRATOR_MOUNT_PATH, file_rel_path)
        
        if not os.path.exists(full_path):
             koha.set_status(biblionumber, 'error', f"File not found: {file_rel_path}")
             return jsonify({"status": "error", "message": f"File not found on disk"}), 400

        koha.set_status(biblionumber, 'processing', 'Integrator started...')

        title, author = parse_marc_details(raw_xml)
        collection_uuid = meta['collection_uuid']
        if not collection_uuid:
             raise Exception("Collection UUID (956$x) is missing")

        item_data = dspace.create_item_direct(collection_uuid, title, author)
        if not item_data:
             raise Exception("Failed to create item in DSpace")

        item_uuid = item_data['uuid']
        handle = item_data.get('handle')
        final_link = handle if handle else f"{request.host_url}/items/{item_uuid}"

        upload_success = dspace.upload_to_item(item_uuid, full_path)
        if not upload_success:
             raise Exception("Failed to upload file")

        koha.set_success(biblionumber, final_link)

        try:
            processed_dir = os.path.join(INTEGRATOR_MOUNT_PATH, FOLDER_PROCESSED)
            os.makedirs(processed_dir, exist_ok=True)
            new_path = os.path.join(processed_dir, os.path.basename(full_path))
            shutil.move(full_path, new_path)
            logger.info(f"Moved file to {new_path}")
        except Exception as e:
            logger.warning(f"File move failed: {e}")

        return jsonify({"status": "success", "handle": final_link})

    except Exception as e:
        logger.error(f"CRITICAL ERROR on #{biblionumber}: {e}")
        try: koha.set_status(biblionumber, 'error', str(e))
        except: pass
        return jsonify({"status": "error", "message": str(e)}), 500