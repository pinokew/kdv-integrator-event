import requests
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

KOHA_API_URL = os.getenv("KOHA_API_URL")
KOHA_USER = os.getenv("KOHA_USER")
KOHA_PASS = os.getenv("KOHA_PASS")
BIBLIO_ID = 9
FILE_PATH = "/mnt/drive/KDV_Integration/Processed/covers/cover_9_v01.jpg"

def try_endpoint(url, method="POST"):
    print(f"\n🔎 Testing: {method} {url}")
    
    headers = {
        "Content-Type": "image/jpeg",
        "Accept": "application/json"
    }

    try:
        with open(FILE_PATH, 'rb') as f:
            image_data = f.read()

        response = requests.request(
            method,
            url,
            data=image_data,
            headers=headers,
            auth=HTTPBasicAuth(KOHA_USER, KOHA_PASS),
            timeout=15
        )
        
        print(f"   Status: {response.status_code}")
        if response.status_code != 404:
            print(f"   🎉 FOUND! Response: {response.text[:200]}...")
            return True
        else:
            print("   ❌ 404 Not Found")
            return False
            
    except Exception as e:
        print(f"   ⚠️ Exception: {e}")
        return False

def debug_upload():
    print(f"--- SEARCHING FOR COVER UPLOAD ENDPOINT ---")
    
    if not os.path.exists(FILE_PATH):
        print("File not found! Fix path first.")
        return

    # Список кандидатів на ендпоінт (на основі різних версій Koha)
    candidates = [
        # Варіант 1: Стандарт для Koha 23.05+ (множина)
        (f"{KOHA_API_URL}/api/v1/biblios/{BIBLIO_ID}/cover_images", "POST"),
        
        # Варіант 2: Те, що ми пробували (однина)
        (f"{KOHA_API_URL}/api/v1/biblios/{BIBLIO_ID}/cover", "POST"),
        
        # Варіант 3: PUT замість POST (оновлення існуючого)
        (f"{KOHA_API_URL}/api/v1/biblios/{BIBLIO_ID}/cover", "PUT"),
        
        # Варіант 4: PUT на множину
        (f"{KOHA_API_URL}/api/v1/biblios/{BIBLIO_ID}/cover_images", "PUT"),
    ]

    for url, method in candidates:
        if try_endpoint(url, method):
            print(f"\n✅ SUCCESS! Use this endpoint: {url} [{method}]")
            break
    else:
        print("\n❌ ALL FAILED. Your Koha version might not support REST API cover uploads.")
        print("Alternative: Use 'tools/upload-cover-image.pl' (requires cookie auth, hard to automate).")

if __name__ == "__main__":
    debug_upload()