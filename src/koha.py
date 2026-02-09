import requests
import logging
import pymarc
import os
import re
import json
import time
from io import BytesIO
from urllib.parse import urljoin
from pymarc import parse_xml_to_array, Field, Subfield
from requests.auth import HTTPBasicAuth

# 🟢 NEW: Імпортуємо KOHA_OPAC_URL
from .config import KOHA_API_URL, KOHA_OPAC_URL, KOHA_USER, KOHA_PASS, TIMEOUT

logger = logging.getLogger("KohaClient")

class KohaClient:
    def __init__(self):
        self.base_url = KOHA_API_URL
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(KOHA_USER, KOHA_PASS)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        # Окрема сесія для CGI операцій (емуляція браузера)
        self.cgi_session = requests.Session()
        self.cgi_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    # --- STANDARD MARC API METHODS ---

    def _get_biblio_xml(self, biblio_id):
        url = f"{self.base_url}/api/v1/biblios/{biblio_id}"
        headers = {"Accept": "application/marcxml+xml"}
        try:
            resp = self.session.get(url, headers=headers, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            return None
        except Exception as e:
            logger.error(f"❌ Network error fetching #{biblio_id}: {e}")
            return None

    def get_biblio_metadata(self, biblio_id: int):
        xml_data = self._get_biblio_xml(biblio_id)
        if not xml_data: return None

        record = self._parse_marc(xml_data)
        if not record: return None

        fields_956 = record.get_fields('956')
        if not fields_956: return None
        
        field = fields_956[0]
        return {
            "file_path": self._get_subfield_safe(field, 'u'),
            "collection_uuid": self._get_subfield_safe(field, 'x'),
            "status": self._get_subfield_safe(field, 'y'),
            "dspace_uuid": self._get_subfield_safe(field, '3')
        }
    
    def get_biblio_timestamp(self, biblio_id: int):
        url = f"{self.base_url}/api/v1/biblios/{biblio_id}"
        headers = {"Accept": "application/json"}
        try:
            resp = self.session.get(url, headers=headers, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.json().get('dateupdated')
            return None
        except: return None

    # --- 🟢 ROBUST COVER UPLOAD & SCRAPING ---

    def check_cover_exists(self, biblionumber):
        """
        Перевірка наявності обкладинки через парсинг сторінки інструментів.
        """
        if not self._ensure_cgi_login(): return False
        
        url = f"{self.base_url}/cgi-bin/koha/tools/upload-cover-image.pl"
        try:
            resp = self.cgi_session.get(url, params={'biblionumber': biblionumber}, timeout=10)
            if "imagenumber=" in resp.text:
                return True
        except Exception as e:
            logger.warning(f"Check cover exists scraping failed: {e}")
        return False

    def get_cover_image_url(self, biblionumber):
        """
        Отримує публічний лінк на обкладинку.
        1. Скрапінг Staff-інтерфейсу (tools) для отримання ID.
        2. Формування лінка на OPAC-інтерфейс.
        """
        if not self._ensure_cgi_login(): return None
        
        # Йдемо в адмінку (Staff URL)
        url = f"{self.base_url}/cgi-bin/koha/tools/upload-cover-image.pl"
        try:
            resp = self.cgi_session.get(url, params={'biblionumber': biblionumber}, timeout=10)
            
            # Шукаємо ID картинки через Regex
            match = re.search(r'imagenumber=(\d+)', resp.text)
            if match:
                image_id = match.group(1)
                
                # 🟢 NEW: Використовуємо OPAC URL для формування публічного лінка
                # Чистимо можливі хвости API, якщо користувач вказав base url як api endpoint
                base_host = KOHA_OPAC_URL.split("/api/v1")[0].rstrip('/')
                
                return f"{base_host}/cgi-bin/koha/opac-image.pl?imagenumber={image_id}"
                
        except Exception as e:
            logger.warning(f"Failed to scrape cover URL: {e}")
        return None

    def upload_cover(self, biblionumber, file_path):
        if not os.path.exists(file_path):
            logger.error(f"Cover file not found: {file_path}")
            return False

        if not self._ensure_cgi_login():
            logger.error(f"❌ Failed to login to Koha CGI for #{biblionumber}")
            return False

        upload_tool_url = f"{self.base_url}/cgi-bin/koha/tools/upload-cover-image.pl"
        try:
            resp_tool = self.cgi_session.get(upload_tool_url, params={'biblionumber': biblionumber}, timeout=15)
            tool_csrf = self._extract_csrf(resp_tool.text)
            if not tool_csrf:
                logger.error("❌ Could not get CSRF token from tools page")
                return False
        except Exception as e:
            logger.error(f"❌ Error fetching upload form: {e}")
            return False

        temp_file_id = self._step1_upload_temp(file_path, tool_csrf, upload_tool_url)
        if not temp_file_id:
            logger.error(f"❌ Step 1 (Temp Upload) failed for #{biblionumber}")
            return False
        
        time.sleep(1)
        return self._step2_process_attach(biblionumber, temp_file_id, tool_csrf, upload_tool_url)

    def _step1_upload_temp(self, file_path, csrf_token, referer_url):
        temp_url = f"{self.base_url}/cgi-bin/koha/tools/upload-file.pl?temp=1"
        headers = {
            'Referer': referer_url,
            'CSRF-TOKEN': csrf_token,
            'X-Requested-With': 'XMLHttpRequest'
        }
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'image/jpeg')}
                resp = self.cgi_session.post(temp_url, files=files, headers=headers, timeout=30)
                try:
                    res_json = resp.json()
                    file_id = res_json.get('fileid')
                    if not file_id and 'uploads' in res_json and len(res_json['uploads']) > 0:
                        file_id = res_json['uploads'][0].get('file_id')
                    if file_id: return file_id
                except:
                    logger.error(f"Failed to parse temp upload response.")
        except Exception as e:
            logger.error(f"Temp upload exception: {e}")
        return None

    def _step2_process_attach(self, biblionumber, file_id, csrf_token, tool_url):
        payload = {
            'biblionumber': str(biblionumber),
            'filetype': 'image',
            'op': 'cud-process', 
            'uploadedfileid': str(file_id),
            'replace': '1',
            'csrf_token': csrf_token
        }
        headers = {'Referer': tool_url}
        try:
            resp = self.cgi_session.post(tool_url, data=payload, headers=headers, timeout=30)
            if "mainpage.pl" in resp.url or "upload_results" in resp.text or "successful" in resp.text:
                 logger.info("✅ Cover attach successful.")
                 return True
            return True 
        except Exception as e:
            logger.error(f"Attach exception: {e}")
            return False

    def _ensure_cgi_login(self):
        entry_url = f"{self.base_url}/cgi-bin/koha/mainpage.pl"
        try:
            resp_check = self.cgi_session.get(entry_url, timeout=10)
            if "Log out" in resp_check.text or "Вихід" in resp_check.text: return True
            
            csrf_token = self._extract_csrf(resp_check.text)
            action_match = re.search(r'<form[^>]+action="([^"]+)"', resp_check.text)
            login_url = urljoin(resp_check.url, action_match.group(1)) if action_match else resp_check.url
            
            payload = {
                "csrf_token": csrf_token, "op": "cud-login", "koha_login_context": "intranet",
                "login_userid": KOHA_USER, "login_password": KOHA_PASS, "branch": ""
            }
            resp_login = self.cgi_session.post(login_url, data=payload, headers={'Referer': entry_url}, timeout=15)
            if "mainpage.pl" in resp_login.url or "Вихід" in resp_login.text: return True
            return False
        except Exception as e:
            logger.error(f"Login process error: {e}")
            return False

    def _extract_csrf(self, html):
        match_input = re.search(r'<input\s+type="hidden"\s+name="csrf_token"\s+value="([^"]+)"', html)
        if match_input: return match_input.group(1)
        match_meta = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
        if match_meta: return match_meta.group(1)
        return None

    # --- HELPERS (UPDATE MARC) ---

    def set_status(self, biblio_id, status, msg=None):
        return self._update_956(biblio_id, status=status, log_msg=msg)

    def set_success(self, biblio_id, handle_url, item_uuid=None, cover_url=None):
        return self._update_956(biblio_id, status="imported", handle_url=handle_url, item_uuid=item_uuid, cover_url=cover_url)

    def _update_956(self, biblio_id, status=None, log_msg=None, handle_url=None, item_uuid=None, cover_url=None):
        xml_data = self._get_biblio_xml(biblio_id)
        if not xml_data: return False
        
        record = self._parse_marc(xml_data)
        fields = record.get_fields('956')
        if fields:
            f956 = fields[0]
            for code in ['y', 'z']:
                try: f956.delete_subfield(code)
                except: pass
            
            if status: f956.add_subfield('y', status)
            if log_msg: f956.add_subfield('z', str(log_msg)[:100])
            
            if item_uuid:
                try: f956.delete_subfield('3')
                except: pass
                f956.add_subfield('3', item_uuid)

            if cover_url:
                try: f956.delete_subfield('c')
                except: pass
                f956.add_subfield('c', cover_url)

        if handle_url:
            for f in record.get_fields('856'): record.remove_field(f)
            record.add_ordered_field(Field(
                tag='856', indicators=['4', '0'],
                subfields=[Subfield(code='u', value=handle_url), Subfield(code='y', value='Repo Link')]
            ))

        new_xml = pymarc.record_to_xml(record).decode('utf-8')
        headers = {"Content-Type": "application/marcxml+xml"}
        try:
            resp = self.session.put(f"{self.base_url}/api/v1/biblios/{biblio_id}", 
                             data=new_xml.encode('utf-8'), headers=headers)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Update error: {e}")
            return False

    def _parse_marc(self, xml_string):
        try:
            return parse_xml_to_array(BytesIO(xml_string.encode('utf-8')))[0]
        except: return None
    
    def _get_subfield_safe(self, field, code):
        try: return field.get_subfields(code)[0].strip()
        except: return None