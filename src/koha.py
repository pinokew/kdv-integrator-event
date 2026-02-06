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

from .config import KOHA_API_URL, KOHA_USER, KOHA_PASS, TIMEOUT

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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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

    # --- 🟢 ROBUST COVER UPLOAD (Two-Step CGI) ---

    def check_cover_exists(self, biblionumber):
        """Перевірка наявності обкладинки (швидкий REST GET)."""
        url = f"{self.base_url}/api/v1/biblios/{biblionumber}/cover"
        try:
            resp = self.session.get(url, stream=True, timeout=5)
            if resp.status_code == 200:
                resp.close()
                return True
        except: pass
        return False

    def upload_cover(self, biblionumber, file_path):
        """
        Головний метод завантаження.
        Реалізує складну логіку авторизації та двоетапного завантаження (Temp -> Process).
        """
        if not os.path.exists(file_path):
            logger.error(f"Cover file not found: {file_path}")
            return False

        # 1. Авторизація (якщо сесія втрачена)
        if not self._ensure_cgi_login():
            logger.error(f"❌ Failed to login to Koha CGI for #{biblionumber}")
            return False

        # 2. Отримання сторінки інструментів (для свіжого CSRF)
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

        # 3. КРОК 1: Завантаження в тимчасове сховище (AJAX)
        temp_file_id = self._step1_upload_temp(file_path, tool_csrf, upload_tool_url)
        if not temp_file_id:
            logger.error(f"❌ Step 1 (Temp Upload) failed for #{biblionumber}")
            return False
        
        # Пауза для синхронізації БД (про всяк випадок)
        time.sleep(1)

        # 4. КРОК 2: Прив'язка файлу до запису
        return self._step2_process_attach(biblionumber, temp_file_id, tool_csrf, upload_tool_url)

    def _step1_upload_temp(self, file_path, csrf_token, referer_url):
        """Завантажує файл на сервер і повертає його ID."""
        temp_url = f"{self.base_url}/cgi-bin/koha/tools/upload-file.pl?temp=1"
        
        # 🟢 ВИПРАВЛЕНО: Заголовки строго як у успішному debug_step2_upload.py
        # Прибрано X-Requested-With, додано Sec-Fetch-* та Accept: */*
        headers = {
            'Referer': referer_url,
            'CSRF-TOKEN': csrf_token,
            'Accept': '*/*', 
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'image/jpeg')}
                # data=None (пусто)
                resp = self.cgi_session.post(temp_url, files=files, headers=headers, timeout=30)
                
                try:
                    res_json = resp.json()
                    file_id = res_json.get('fileid')
                    if not file_id and 'uploads' in res_json and len(res_json['uploads']) > 0:
                        file_id = res_json['uploads'][0].get('file_id')
                    
                    if file_id:
                        logger.info(f"✅ Temp file uploaded. ID: {file_id}")
                        return file_id
                    else:
                        logger.error(f"Temp upload returned JSON without ID: {res_json}")
                except:
                    logger.error(f"Failed to parse temp upload response: {resp.text[:200]}")
                    
        except Exception as e:
            logger.error(f"Temp upload exception: {e}")
        
        return None

    def _step2_process_attach(self, biblionumber, file_id, csrf_token, tool_url):
        """Прив'язує завантажений файл (ID) до бібліографічного запису."""
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
            
            # Перевірка успіху: редірект або текст
            if "itemnumber=" in resp.url or "successful" in resp.text or "успішно" in resp.text:
                logger.info(f"✅ Cover successfully attached to #{biblionumber}")
                return True
            elif "upload_results" in resp.text:
                logger.info(f"✅ Cover upload likely successful (found results div)")
                return True
            else:
                logger.warning(f"⚠️ Cover attach response unclear. URL: {resp.url}")
                return True 
                
        except Exception as e:
            logger.error(f"Attach exception: {e}")
            return False

    def _ensure_cgi_login(self):
        """Гарантує, що ми залогінені в CGI сесії."""
        entry_url = f"{self.base_url}/cgi-bin/koha/mainpage.pl"
        
        try:
            # Перевірка: чи ми вже там?
            resp_check = self.cgi_session.get(entry_url, timeout=10)
            if "Log out" in resp_check.text or "Вихід" in resp_check.text:
                return True
            
            # Якщо ні - логінимось
            csrf_token = self._extract_csrf(resp_check.text)
            
            # Шукаємо action форми
            action_match = re.search(r'<form[^>]+action="([^"]+)"', resp_check.text)
            login_url = urljoin(resp_check.url, action_match.group(1)) if action_match else resp_check.url
            
            payload = {
                "userid": KOHA_USER, "password": KOHA_PASS,
                "koha_login_context": "intranet", "op": "cud-login"
            }
            if csrf_token: payload["csrf_token"] = csrf_token
            
            resp_login = self.cgi_session.post(login_url, data=payload, headers={'Referer': entry_url}, timeout=15)
            
            if "Log out" in resp_login.text or "Вихід" in resp_login.text or "mainpage.pl" in resp_login.url:
                logger.info("✅ CGI Login Successful")
                return True
            
            logger.error(f"CGI Login Failed. Final URL: {resp_login.url}")
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

    def set_success(self, biblio_id, handle_url, item_uuid=None):
        return self._update_956(biblio_id, status="imported", handle_url=handle_url, item_uuid=item_uuid)

    def _update_956(self, biblio_id, status=None, log_msg=None, handle_url=None, item_uuid=None):
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
        except Exception as e:
            logger.error(f"MARC Parse Error: {e}")
            return None

    def _get_subfield_safe(self, field, code):
        try:
            val = field.get_subfields(code)[0]
            return val.strip() if val else None
        except: return None