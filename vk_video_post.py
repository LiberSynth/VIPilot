import requests
import sys

TOKEN = "vk1.a.54x99rXUBEoxxLPHTWukhgWTfgoVw2yUFMfFJjOYuC6IOl2OL2afVmRM2CZwuQtpcWrkMndt4wvjCzszMuYkfL_t-wWQWNR8sBiHhh3eOu3v6gt5B9WWLiOn83adK90K5Kp5G73rC1j7vs24fcNc_-ShaxRsS8BEKhAzMovA4ozGViX8OV_b_W8l2EM5AukakmWXN7Gp9yowVuObDtZOZQ"
OWNER_ID = -236929597
V = "5.131"

video_path = sys.argv[1]
caption = sys.argv[2] if len(sys.argv) > 2 else "Стройматериалы и ремонт"

save_resp = requests.post("https://api.vk.com/method/video.save", data={
    "name": caption,
    "description": caption,
    "group_id": abs(OWNER_ID),
    "access_token": TOKEN,
    "v": V
}).json()

if "error" in save_resp:
    print("video.save error:", save_resp)
    sys.exit(1)

upload_url = save_resp["response"]["upload_url"]
video_id = save_resp["response"]["video_id"]
owner_id_vid = save_resp["response"]["owner_id"]

with open(video_path, "rb") as f:
    upload_resp = requests.post(upload_url, files={"video_file": f}).json()

print("Upload response:", upload_resp)

post_resp = requests.post("https://api.vk.com/method/wall.post", data={
    "owner_id": OWNER_ID,
    "from_group": 1,
    "message": caption,
    "attachments": f"video{owner_id_vid}_{video_id}",
    "access_token": TOKEN,
    "v": V
}).json()

print("Post response:", post_resp)
