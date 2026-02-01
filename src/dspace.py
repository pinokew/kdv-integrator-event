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
                logger.info(f"✅ DSpace Login Success ({DSPACE_USER})")
                return True
            
            logger.error(f"❌ Login Failed: {resp.status_code} - {resp.text}")
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
                logger.warning("Token expired (401). Retrying login...")
                if self.login():
                    resp = self.session.request(method, url, timeout=current_timeout, **kwargs)
            
            return resp
        except Exception as e:
            logger.error(f"❌ Request Exception [{method} {endpoint}]: {e}")
            return None

    def find_item_uuid_by_handle(self, handle):
        """
        Шукає UUID запису за його Handle.
        Використовує /api/pid/find (Identifier Resolution) замість пошуку.
        """
        # СТРАТЕГІЯ B: PID Resolution
        # Цей запит поверне 303 Redirect на реальний об'єкт (Item)
        # Requests автоматично перейде за редіректом і поверне JSON Item-а.
        endpoint = "/pid/find"
        logger.info(f"🔎 Resolving Handle via PID: '{handle}'")
        
        resp = self._request("GET", endpoint, params={"id": handle})
        
        if resp is not None:
            if resp.status_code == 200:
                # Якщо резолюція успішна, ми отримали сам об'єкт Item
                try:
                    data = resp.json()
                    uuid = data.get('uuid')
                    obj_type = data.get('type')
                    
                    if uuid and obj_type == 'item':
                        logger.info(f"✅ Resolved UUID: {uuid}")
                        return uuid
                    else:
                        logger.warning(f"⚠️ Handle resolved, but object is not an Item (Got {obj_type})")
                except:
                    logger.error(f"❌ Failed to parse JSON from resolved object.")
            elif resp.status_code == 404:
                logger.error(f"❌ Handle '{handle}' does not exist (404 Not Found)")
            else:
                logger.error(f"❌ Resolution failed [Status {resp.status_code}]")
                logger.error(f"Response: {resp.text}")
        else:
             logger.error("❌ Resolution failed: No network response")
             
        return None

    def update_metadata(self, item_uuid, title, author):
        logger.info(f"Updating metadata for {item_uuid}")
        
        operations = []
        operations.append({
            "op": "replace",
            "path": "/metadata/dc.title",
            "value": [{"value": title, "language": None}]
        })

        if author:
            operations.append({
                "op": "replace", 
                "path": "/metadata/dc.contributor.author",
                "value": [{"value": author, "language": None}]
            })

        old_ct = self.session.headers.get("Content-Type")
        self.session.headers["Content-Type"] = "application/json-patch+json"
        
        try:
            resp = self._request("PATCH", f"/core/items/{item_uuid}", json=operations)
            if resp is not None and resp.status_code == 200:
                logger.info("✅ Metadata Updated")
                return True
            else:
                logger.error(f"❌ Update Failed: {resp.text if resp else 'No resp'}")
                return False
        finally:
            if old_ct:
                self.session.headers["Content-Type"] = old_ct
            else:
                del self.session.headers["Content-Type"]

    def create_item_direct(self, collection_uuid, title, author=None, koha_id=None):
        logger.info(f"Creating Item in Collection {collection_uuid}...")
        
        metadata = {
            "dc.title": [{"value": title, "language": None}],
            "dc.date.issued": [{"value": str(time.localtime().tm_year), "language": None}],
            "dc.type": [{"value": "Book", "language": None}]
        }
        if author: 
            metadata["dc.contributor.author"] = [{"value": author, "language": None}]

        data = {
            "name": title, 
            "metadata": metadata, 
            "inArchive": True, 
            "discoverable": True
        }
        
        resp = self._request("POST", "/core/items", params={"owningCollection": collection_uuid}, json=data)
        
        if resp is not None and resp.status_code in [200, 201]:
            logger.info(f"✅ Item Created: {resp.json().get('uuid')}")
            return resp.json()
        
        if resp is not None:
            logger.error(f"❌ Create Failed [Status {resp.status_code}]")
            logger.error(f"Response: {resp.text}")
        else:
            logger.error("❌ Create Failed: No response from DSpace")
            
        return None

    def upload_to_item(self, item_uuid, file_path):
        if not os.path.exists(file_path): 
            logger.error(f"❌ File not found on disk: {file_path}")
            return False
        
        bundle_uuid = None
        resp = self._request("GET", f"/core/items/{item_uuid}/bundles")
        if resp is not None:
            for b in resp.json().get('_embedded', {}).get('bundles', []):
                if b['name'] == 'ORIGINAL': bundle_uuid = b['uuid']
        
        if not bundle_uuid:
            logger.info("Creating ORIGINAL bundle...")
            resp = self._request("POST", f"/core/items/{item_uuid}/bundles", json={"name": "ORIGINAL"})
            if resp is not None and resp.status_code in [200, 201]: 
                bundle_uuid = resp.json()['uuid']
            else: 
                logger.error(f"❌ Failed to create bundle: {resp.text if resp else 'No resp'}")
                return False

        logger.info(f"Uploading file {os.path.basename(file_path)}...")
        old_ct = self.session.headers.pop("Content-Type", None)
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/pdf')}
                resp = self._request("POST", f"/core/bundles/{bundle_uuid}/bitstreams", 
                                     files=files, timeout=UPLOAD_TIMEOUT)
                
                if resp is not None and resp.status_code in [200, 201]:
                    logger.info("✅ Upload Success")
                    return True
                else:
                    logger.error(f"❌ Upload Failed: {resp.text if resp else 'No resp'}")
                    return False
        except Exception as e:
            logger.error(f"Upload Exception: {e}")
            return False
        finally:
            if old_ct: self.session.headers["Content-Type"] = old_ct