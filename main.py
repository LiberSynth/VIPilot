import requests
import time
import os
from datetime import datetime

TOKEN = "vk1.a.hEoJK06ivbtE0_CnuoWsKKTjXw5gDBlZEDSC-txGC9A4ikBXAjUHnRQjmE9r0IfcNjYJElycVpKxsXZn1B67MUAjPeIzKHuO763HLmGeb-23WBzZyj7aBVRgFVQcOsDVV6jIakX73zMmBb_EDo1plt-CJvwkhbp7rsllPybXobR64TR7PcNFj6S0djJoH9IccQjKKIklSx0Jd3sq5izsTA"
GROUP_ID = 236929597
V = "5.131"
INTERVAL = 300

IMAGES = sorted([
    f"attached_assets/stories/{f}"
    for f in os.listdir("attached_assets/stories")
    if f.endswith(".png")
])

def post_story(image_path):
    r = requests.post("https://api.vk.com/method/stories.getPhotoUploadServer", data={
        "group_id": GROUP_ID,
        "add_to_news": 1,
        "access_token": TOKEN,
        "v": V
    })
    data = r.json()
    if "error" in data:
        print("Ошибка getPhotoUploadServer:", data["error"])
        return False

    upload_url = data["response"]["upload_url"]

    with open(image_path, "rb") as f:
        upload_resp = requests.post(upload_url, files={"file": f}).json()

    if "error" in upload_resp:
        print("Ошибка загрузки:", upload_resp)
        return False

    upload_result = upload_resp.get("response", {}).get("upload_result", "")
    save_resp = requests.post("https://api.vk.com/method/stories.save", data={
        "upload_results": upload_result,
        "access_token": TOKEN,
        "v": V
    })
    result = save_resp.json()
    if "error" in result:
        print("Ошибка stories.save:", result["error"])
        return False

    print(f"[{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}] История опубликована: {os.path.basename(image_path)}")
    return True

index = 0
while True:
    image = IMAGES[index % len(IMAGES)]
    post_story(image)
    index += 1
    print(f"Следующая история через {INTERVAL // 60} мин...")
    time.sleep(INTERVAL)
