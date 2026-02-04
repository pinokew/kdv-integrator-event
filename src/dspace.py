import os
import requests
import logging
import time
from requests.exceptions import RequestException
from .config import DSPACE_API_URL, DSPACE_USER, DSPACE_PASS, TIMEOUT, UPLOAD_TIMEOUT

logger = logging.getLogger("DSpaceClient")

class DSpaceClient:
    def __init__(self):
        self.base_url = DSPACE_API_URL
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.token = None

    def _update_xsrf_header(self):
        csrf_cookie = self.session.cookies.get("DSPACE-XSRF-COOKIE")
        if csrf_cookie:
            self.session.headers.update({"X-XSRF-TOKEN": csrf_cookie})

    def login(self) -> bool:
        auth_url = f"{self.base_url}/authn/login"
        try:
            self.session.get(f"{self.base_url}/authn/status", timeout=TIMEOUT)
            self._update_xsrf_header()
        except Exception as e:
            logger.error(f"⚠️ DSpace is unreachable: {e}")
            return False

        payload = {"user": DSPACE_USER, "password": DSPACE_PASS}
        try:
            resp = self.session.post(auth_url, data=payload, timeout=TIMEOUT)
            if resp.status_code in [200, 204]:
                self.token = resp.headers.get("Authorization")
                self.session.headers.update({"Authorization": self.token})
                self._update_xsrf_header()
                # logger.info(f"✅ DSpace Login Success ({DSPACE_USER})")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Login Exception: {e}")
            return False

    def _request(self, method, endpoint, **kwargs):
        if not self.token and endpoint != "/authn/login":
            if not self.login(): 
                return None
        
        url = f"{self.base_url}{endpoint}"
        current_timeout = kwargs.pop('timeout', TIMEOUT)

        try:
            resp = self.session.request(method, url, timeout=current_timeout, **kwargs)
            self._update_xsrf_header()
            
            if resp.status_code == 401:
                if self.login():
                    resp = self.session.request(method, url, timeout=current_timeout, **kwargs)
            return resp
        except Exception as e:
            logger.error(f"❌ Request Exception [{method} {endpoint}]: {e}")
            return None

    def find_item_uuid_by_handle(self, handle):
        endpoint = "/pid/find"
        resp = self._request("GET", endpoint, params={"id": handle})
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                if data.get('uuid') and data.get('type') == 'item':
                    return data.get('uuid')
            except: pass
        return None

    def find_item_by_biblionumber(self, biblionumber):
        endpoint = "/discover/search/objects"
        query = f"koha.biblionumber:{biblionumber}"
        params = {"query": query, "dsoType": "item"}
        
        resp = self._request("GET", endpoint, params=params)
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                results = data.get('_embedded', {}).get('searchResult', {}).get('_embedded', {}).get('objects', [])
                if results:
                    first_hit = results[0]['_embedded']['indexableObject']
                    return {"uuid": first_hit['uuid'], "handle": first_hit.get('handle')}
            except: pass
        return None

    # 🟢 НОВИЙ МЕТОД
    def get_item_last_modified(self, item_uuid):
        """Повертає рядок lastModified (ISO 8601) для Item"""
        resp = self._request("GET", f"/core/items/{item_uuid}")
        if resp and resp.status_code == 200:
            return resp.json().get('lastModified')
        return None

    def _format_metadata_value(self, value):
        if isinstance(value, list):
            return [{"value": str(v), "language": None} for v in value]
        return [{"value": str(value), "language": None}]

    def update_metadata(self, item_uuid, metadata_dict):
        operations = []
        for key, value in metadata_dict.items():
            if key in ['handle', 'uuid'] or value is None: continue
            dspace_values = self._format_metadata_value(value)
            operations.append({"op": "replace", "path": f"/metadata/{key}", "value": dspace_values})

        if not operations: return True

        headers = {"Content-Type": "application/json-patch+json"}
        resp = self._request("PATCH", f"/core/items/{item_uuid}", json=operations, headers=headers)
        return resp is not None and resp.status_code == 200

    def create_item_direct(self, collection_uuid, metadata_dict):
        # ... (код створення без змін, для скорочення місця, він ідентичний v6.5) ...
        dspace_metadata = {}
        if 'dc.date.issued' not in metadata_dict or not metadata_dict['dc.date.issued']:
             metadata_dict['dc.date.issued'] = str(time.localtime().tm_year)
        if 'dc.type' not in metadata_dict or not metadata_dict['dc.type']:
             metadata_dict['dc.type'] = "Book"

        for key, value in metadata_dict.items():
            if key in ['handle', 'uuid'] or value is None: continue
            dspace_metadata[key] = self._format_metadata_value(value)

        name_val = metadata_dict.get('dc.title', 'Untitled')
        if isinstance(name_val, list): name_val = name_val[0]

        data = { "name": name_val, "metadata": dspace_metadata, "inArchive": True, "discoverable": True }
        resp = self._request("POST", "/core/items", params={"owningCollection": collection_uuid}, json=data)
        
        if resp is not None and resp.status_code in [200, 201]:
            return resp.json()
        return None

    def upload_to_item(self, item_uuid, file_path):
        if not os.path.exists(file_path): return False
        
        bundle_uuid = None
        resp = self._request("GET", f"/core/items/{item_uuid}/bundles")
        if resp is not None:
            for b in resp.json().get('_embedded', {}).get('bundles', []):
                if b['name'] == 'ORIGINAL': bundle_uuid = b['uuid']
        
        if not bundle_uuid:
            resp = self._request("POST", f"/core/items/{item_uuid}/bundles", json={"name": "ORIGINAL"})
            if resp and resp.status_code in [200, 201]: bundle_uuid = resp.json()['uuid']
            else: return False

        old_ct = self.session.headers.pop("Content-Type", None)
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/pdf')}
                resp = self._request("POST", f"/core/bundles/{bundle_uuid}/bitstreams", 
                                     files=files, timeout=UPLOAD_TIMEOUT)
                return resp and resp.status_code in [200, 201]
        except Exception: return False
        finally:
            if old_ct: self.session.headers["Content-Type"] = old_ct