import requests

TOKEN = ""
OWNER_ID = 0

text = input("Введи текст для поста: ")

data = {
    "owner_id": OWNER_ID,
    "from_group": 1,
    "message": text,
    "access_token": TOKEN,
    "v": "5.131"
}

resp = requests.post("https://api.vk.com/method/wall.post", data=data)
print("Status:", resp.status_code)
print("Body:", resp.text)
