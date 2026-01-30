import os
import requests
import logging
import time
from requests.exceptions import RequestException
# ВИПРАВЛЕНИЙ ІМПОРТ
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
        try:
            self.session.get(f"{self.base_url}/authn/status", timeout=TIMEOUT)
            self._update_xsrf_header()
        except: return False

        payload = {"user": DSPACE_USER, "password": DSPACE_PASS}
        try:
            resp = self.session.post(f"{self.base_url}/authn/login", data=payload, timeout=TIMEOUT)
            if resp.status_code in [200, 204]:
                self.token = resp.headers.get("Authorization")
                self.session.headers.update({"Authorization": self.token})
                self._update_xsrf_header()
                return True
            return False
        except: return False

    def _request(self, method, endpoint, **kwargs):
        if not self.token and endpoint != "/authn/login":
            if not self.login(): return None
        
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.request(method, url, timeout=TIMEOUT, **kwargs)
            self._update_xsrf_header()
            if resp.status_code == 401:
                if self.login():
                    return self.session.request(method, url, timeout=TIMEOUT, **kwargs)
            return resp
        except: return None

    def create_item_direct(self, collection_uuid, title, author=None, koha_id=None):
        metadata = {
            "dc.title": [{"value": title, "language": None}],
            "dc.date.issued": [{"value": str(time.localtime().tm_year), "language": None}],
            "dc.type": [{"value": "Book", "language": None}]
        }
        if author: metadata["dc.contributor.author"] = [{"value": author, "language": None}]

        data = {"name": title, "metadata": metadata, "inArchive": True, "discoverable": True}
        resp = self._request("POST", "/core/items", params={"owningCollection": collection_uuid}, json=data)
        
        if resp and resp.status_code in [200, 201]:
            return resp.json()
        return None

    def upload_to_item(self, item_uuid, file_path):
        if not os.path.exists(file_path): return False
        
        # Отримати бандл
        bundle_uuid = None
        resp = self._request("GET", f"/core/items/{item_uuid}/bundles")
        if resp:
            for b in resp.json().get('_embedded', {}).get('bundles', []):
                if b['name'] == 'ORIGINAL': bundle_uuid = b['uuid']
        
        if not bundle_uuid:
            resp = self._request("POST", f"/core/items/{item_uuid}/bundles", json={"name": "ORIGINAL"})
            if resp and resp.status_code in [200, 201]: bundle_uuid = resp.json()['uuid']
            else: return False

        # Завантажити файл
        old_ct = self.session.headers.pop("Content-Type", None)
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/pdf')}
                resp = self._request("POST", f"/core/bundles/{bundle_uuid}/bitstreams", 
                                     files=files, timeout=UPLOAD_TIMEOUT)
                return resp and resp.status_code in [200, 201]
        finally:
            if old_ct: self.session.headers["Content-Type"] = old_ct