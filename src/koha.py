import requests
import logging
import pymarc
from io import BytesIO
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

    def _get_biblio_xml(self, biblio_id):
        url = f"{self.base_url}/api/v1/biblios/{biblio_id}"
        headers = {"Accept": "application/marcxml+xml"}
        try:
            logger.info(f"Fetching Biblio #{biblio_id} from {url}")
            resp = self.session.get(url, headers=headers, timeout=TIMEOUT)
            
            if resp.status_code == 200:
                return resp.text
            
            # --- DEBUG LOGGING ---
            logger.error(f"❌ Koha API Error [Status {resp.status_code}]")
            logger.error(f"Response: {resp.text[:200]}") # Перші 200 символів відповіді
            return None

        except Exception as e:
            logger.error(f"❌ Network error fetching #{biblio_id}: {e}")
            return None

    def get_biblio_metadata(self, biblio_id: int):
        xml_data = self._get_biblio_xml(biblio_id)
        if not xml_data: return None

        record = self._parse_marc(xml_data)
        if not record: 
            logger.error(f"Failed to parse MARC for #{biblio_id}")
            return None

        fields_956 = record.get_fields('956')
        if not fields_956: 
            logger.warning(f"Field 956 not found in Biblio #{biblio_id}")
            return None
        
        field = fields_956[0]
        return {
            "file_path": self._get_subfield_safe(field, 'u'),
            "collection_uuid": self._get_subfield_safe(field, 'x'),
            "status": self._get_subfield_safe(field, 'y')
        }

    def set_status(self, biblio_id, status, msg=None):
        return self._update_956(biblio_id, status=status, log_msg=msg)

    def set_success(self, biblio_id, handle_url):
        return self._update_956(biblio_id, status="imported", handle_url=handle_url)

    def _update_956(self, biblio_id, status=None, log_msg=None, handle_url=None):
        xml_data = self._get_biblio_xml(biblio_id)
        if not xml_data: return False
        
        record = self._parse_marc(xml_data)
        fields = record.get_fields('956')
        if fields:
            f956 = fields[0]
            for code in ['y', 'z']:
                try: f956.delete_subfield(code)
                except: pass
            
            if status: 
                f956.add_subfield('y', status)
            if log_msg: 
                f956.add_subfield('z', str(log_msg)[:100])

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
            if resp.status_code != 200:
                logger.error(f"Failed to update Koha: {resp.status_code} - {resp.text}")
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