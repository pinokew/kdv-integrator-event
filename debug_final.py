import requests
import os
import re
import time
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

# Налаштування
KOHA_BASE_URL = os.getenv("KOHA_API_URL")
KOHA_USER = os.getenv("KOHA_API_USER")
KOHA_PASS = os.getenv("KOHA_API_PASS")

BIBLIO_ID = 9
FILE_PATH = "/mnt/drive/KDV_Integration/Processed/covers/cover_9_v01.jpg"

def debug_final_upload():
    print("--- STARTING FINAL UPLOAD SEQUENCE ---")
    
    if not os.path.exists(FILE_PATH):
        print(f"❌ File not found: {FILE_PATH}")
        return

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
        'Origin': KOHA_BASE_URL,
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })

    # [1] AUTHENTICATION
    print("\n[1] AUTHENTICATION...")
    entry_url = f"{KOHA_BASE_URL}/cgi-bin/koha/mainpage.pl"
    
    resp_init = session.get(entry_url)
    print_cookies("Init", session)
    
    csrf_token = get_csrf(resp_init.text)
    
    action_match = re.search(r'<form[^>]+action="([^"]+)"', resp_init.text)
    login_post_url = urljoin(resp_init.url, action_match.group(1)) if action_match else resp_init.url

    login_payload = {
        "csrf_token": csrf_token,
        "op": "cud-login",
        "koha_login_context": "intranet",
        "login_userid": KOHA_USER,
        "login_password": KOHA_PASS,
        "branch": ""
    }
    
    resp_login = session.post(login_post_url, data=login_payload, headers={'Referer': entry_url})
    print_cookies("Login", session)
    
    if not ("mainpage.pl" in resp_login.url or "Вихід" in resp_login.text):
        print("❌ Login Failed.")
        return
    print("✅ Login Successful.")

    # [2] GET UPLOAD PAGE (CSRF)
    print("\n[2] FETCHING CSRF...")
    tools_url = f"{KOHA_BASE_URL}/cgi-bin/koha/tools/upload-cover-image.pl"
    resp_tools = session.get(tools_url, params={'biblionumber': BIBLIO_ID})
    
    upload_csrf = get_csrf(resp_tools.text)
    print(f"   CSRF: {upload_csrf[:15]}...")

    # [3] UPLOAD TO TEMP
    print("\n[3] UPLOADING TO TEMP...")
    temp_url = f"{KOHA_BASE_URL}/cgi-bin/koha/tools/upload-file.pl?temp=1"
    
    with open(FILE_PATH, 'rb') as f:
        files = {'file': (os.path.basename(FILE_PATH), f, 'image/jpeg')}
        
        # Специфічні заголовки для AJAX
        ajax_headers = {
            'Referer': tools_url,
            'CSRF-TOKEN': upload_csrf,
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        # Відправляємо запит (Cookies requests підставить сам)
        resp_temp = session.post(temp_url, files=files, headers=ajax_headers)
        print_cookies("Temp Upload", session)
        
        try:
            res_json = resp_temp.json()
            file_id = res_json.get('fileid')
            if not file_id and 'uploads' in res_json:
                 file_id = res_json['uploads'][0].get('file_id')

            if not file_id:
                print(f"❌ Temp upload failed. JSON: {res_json}")
                return
            print(f"✅ Temp ID: {file_id}")
            
        except:
            print(f"❌ Failed to parse JSON. Body: {resp_temp.text}")
            return

    # Пауза для синхронізації сесії (на всяк випадок)
    time.sleep(1)

    # [4] FINAL PROCESS
    print(f"\n[4] ATTACHING FILE {file_id}...")
    
    payload = {
        'biblionumber': str(BIBLIO_ID),
        'filetype': 'image',
        'op': 'cud-process', 
        'uploadedfileid': str(file_id),
        'replace': '1',
        'csrf_token': upload_csrf
    }
    
    # Очищаємо AJAX заголовки, залишаємо стандартні
    # Важливо: Referer має бути сторінкою, де була форма
    session.headers.update({'Referer': tools_url})
    
    # Видаляємо заголовки, які можуть заважати звичайному POST
    if 'CSRF-TOKEN' in session.headers: del session.headers['CSRF-TOKEN']
    if 'X-Requested-With' in session.headers: del session.headers['X-Requested-With']

    resp_process = session.post(tools_url, data=payload)
    print_cookies("Final Process", session)
    
    # Зберігаємо результат для аналізу
    with open("final_result.html", "w") as f:
        f.write(resp_process.text)
    
    # АНАЛІЗ РЕЗУЛЬТАТУ
    if "mainpage.pl" in resp_process.url:
         # Іноді успіх редіректить на головну
         print("✅ Redirected to mainpage (Check Koha UI).")
    elif "login_error" in resp_process.text:
         print("❌ Session Timed Out again!")
    elif "upload_results" in resp_process.text:
         print("🎉 SUCCESS! Found upload_results.")
         match = re.search(r'<div id="upload_results"(.*?)</div>', resp_process.text, re.DOTALL)
         if match: print(clean_html(match.group(0)))
    else:
         print("❓ Unknown state. Check final_result.html")

def print_cookies(step, s):
    c = s.cookies.get_dict()
    print(f"   [{step}] Cookies: {c}")

def get_csrf(html):
    match_input = re.search(r'<input\s+type="hidden"\s+name="csrf_token"\s+value="([^"]+)"', html)
    if match_input: return match_input.group(1)
    match_meta = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    if match_meta: return match_meta.group(1)
    return None

def clean_html(html):
    return re.sub(r'<[^>]+>', ' ', html).strip().replace('  ', ' ')

if __name__ == "__main__":
    debug_final_upload()