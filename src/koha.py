import requests
import logging
import pymarc
from io import BytesIO
from pymarc import parse_xml_to_array, Record, Field, Subfield
from requests.auth import HTTPBasicAuth
from src.config import KOHA_API_URL, KOHA_USER, KOHA_PASS, TIMEOUT

logger = logging.getLogger("KDV-API")

class KohaClient:
    def __init__(self):
        self.base_url = KOHA_API_URL
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(KOHA_USER, KOHA_PASS)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _request(self, method: str, path: str, **kwargs):
        url = f"{self.base_url}{path}"
        try:
            kwargs.setdefault('timeout', TIMEOUT)
            response = self.session.request(method, url, **kwargs)
            return response
        except Exception as e:
            logger.error(f"❌ Koha Network Error ({method} {path}): {e}")
            return None

    def get_biblio_xml(self, biblio_id: int):
        """Отримує повний MARCXML запис."""
        headers = {"Accept": "application/marcxml+xml"}
        response = self._request("GET", f"/api/v1/biblios/{biblio_id}", headers=headers)
        if response and response.status_code == 200:
            return response.text
        return None

    def get_integration_data(self, biblio_id: int):
        """
        Отримує дані для інтеграції по конкретному ID.
        Парсить XML і повертає словник з шляхом, UUID колекції та заголовком.
        """
        xml_data = self.get_biblio_xml(biblio_id)
        if not xml_data:
            logger.error(f"Bib {biblio_id}: No XML data found.")
            return None

        record = self._parse_marc(xml_data)
        if not record: return None

        # Отримуємо основні дані
        title = record.title() or "No Title"
        author = record.author()

        # Шукаємо поле 956
        fields_956 = record.get_fields('956')
        if not fields_956:
            logger.warning(f"Bib {biblio_id}: Field 956 not found.")
            return None
        
        field = fields_956[0]
        
        # Безпечне читання підполів
        file_path = field['u'] if 'u' in field else None
        collection_uuid = field['x'].strip() if 'x' in field else None
        status = field['y'] if 'y' in field else None

        return {
            "bib_id": biblio_id,
            "title": title,
            "author": author,
            "file_path": file_path,
            "collection_uuid": collection_uuid,
            "status": status
        }

    def update_status(self, biblio_id, status, log_msg="", handle_url=None):
        """Оновлює статус (956$y) та додає посилання (856$u)."""
        xml_data = self.get_biblio_xml(biblio_id)
        if not xml_data: return False
            
        record = self._parse_marc(xml_data)
        if not record: return False
            
        fields_956 = record.get_fields('956')
        if fields_956:
            field_956 = fields_956[0]
            # Видаляємо старі статуси
            for code in ['y', 'z']:
                try: field_956.delete_subfield(code)
                except: pass
            
            # Додаємо нові
            field_956.subfields.append(Subfield(code='y', value=status))
            field_956.subfields.append(Subfield(code='z', value=log_msg))
        
        if handle_url:
            # Перевіряємо чи вже є таке посилання, щоб не дублювати
            existing_856 = record.get_fields('856')
            link_exists = False
            for f in existing_856:
                if f['u'] == handle_url:
                    link_exists = True
                    break
            
            if not link_exists:
                field_856 = Field(
                    tag='856',
                    indicators=['4', '0'], 
                    subfields=[
                        Subfield(code='u', value=handle_url),
                        Subfield(code='y', value='Цифровий репозиторій (PDF)')
                    ]
                )
                record.add_ordered_field(field_856)
        
        new_xml = pymarc.record_to_xml(record).decode('utf-8')
        
        logger.info(f"Updating Koha Biblio {biblio_id} -> Status: {status}")
        headers = {"Content-Type": "application/marcxml+xml"}
        response = self._request("PUT", f"/api/v1/biblios/{biblio_id}", data=new_xml.encode('utf-8'), headers=headers)
        
        return response and response.status_code == 200

    def _parse_marc(self, xml_string):
        try:
            if not isinstance(xml_string, str): xml_string = str(xml_string)
            reader = parse_xml_to_array(BytesIO(xml_string.encode('utf-8')))
            return reader[0] if reader else None
        except Exception as e:
            logger.error(f"MARC Parse Error: {e}")
            return None