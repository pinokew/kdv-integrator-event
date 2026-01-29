import os
import requests
import logging
import time
from src.config import DSPACE_API_URL, DSPACE_USER, DSPACE_PASS, TIMEOUT, UPLOAD_TIMEOUT

logger = logging.getLogger("KDV-API")

class DSpaceClient:
    def __init__(self):
        self.base_url = DSPACE_API_URL
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.token = None

    def _update_xsrf(self):
        cookie = self.session.cookies.get("DSPACE-XSRF-COOKIE")
        if cookie: self.session.headers.update({"X-XSRF-TOKEN": cookie})

    def login(self) -> bool:
        try:
            self.session.get(f"{self.base_url}/authn/status", timeout=TIMEOUT)
            self._update_xsrf()
            
            payload = {"user": DSPACE_USER, "password": DSPACE_PASS}
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
            resp = self.session.post(f"{self.base_url}/authn/login", data=payload, headers=headers, timeout=TIMEOUT)
            
            if resp.status_code in [200, 204]:
                token = resp.headers.get("Authorization")
                if token:
                    self.token = token
                    self.session.headers.update({"Authorization": token})
                    self._update_xsrf()
                    return True
            logger.error(f"❌ DSpace Login Failed: {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"❌ DSpace Connection Error: {e}")
            return False

    def _request(self, method, endpoint, **kwargs):
        if not self.token and endpoint != "/authn/login":
            if not self.login(): return None
        
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"
        kwargs.setdefault('timeout', TIMEOUT)
        
        try:
            resp = self.session.request(method, url, **kwargs)
            self._update_xsrf()
            if resp.status_code == 401:
                logger.warning("Session expired. Re-authenticating...")
                if self.login():
                    return self.session.request(method, url, **kwargs)
            return resp
        except Exception as e:
            logger.error(f"Request Error: {e}")
            return None

    def create_item_direct(self, collection_uuid, title, author=None):
        """Створює архівний ітем напряму."""
        logger.info(f"Creating item in {collection_uuid}...")
        
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
        
        # Використовуємо owningCollection для прямого створення
        resp = self._request("POST", "/core/items", params={"owningCollection": collection_uuid}, json=data)
        
        if resp and resp.status_code in [200, 201]:
            item = resp.json()
            logger.info(f"✅ Item Created! UUID: {item['uuid']}")
            return item
        
        logger.error(f"❌ Create Failed: {resp.status_code if resp else 'None'} {resp.text if resp else ''}")
        return None

    def upload_to_item(self, item_uuid, file_path):
        """Завантажує файл у бандл ORIGINAL."""
        if not os.path.exists(file_path):
            logger.error(f"File missing: {file_path}")
            return False

        # 1. Знайти або створити бандл
        bundle_uuid = None
        resp = self._request("GET", f"/core/items/{item_uuid}/bundles")
        if resp and resp.status_code == 200:
            for b in resp.json().get('_embedded', {}).get('bundles', []):
                if b['name'] == "ORIGINAL":
                    bundle_uuid = b['uuid']
                    break
        
        if not bundle_uuid:
            resp = self._request("POST", f"/core/items/{item_uuid}/bundles", json={"name": "ORIGINAL"})
            if resp and resp.status_code in [200, 201]:
                bundle_uuid = resp.json()['uuid']
            else:
                return False

        # 2. Завантажити
        logger.info(f"Uploading to bundle {bundle_uuid}...")
        orig_ct = self.session.headers.pop("Content-Type", None)
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/pdf')}
                resp = self._request("POST", f"/core/bundles/{bundle_uuid}/bitstreams", files=files, timeout=UPLOAD_TIMEOUT)
                return resp and resp.status_code in [200, 201]
        finally:
            if orig_ct: self.session.headers["Content-Type"] = orig_ct