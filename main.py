import requests

TOKEN = "vk1.a.54x99rXUBEoxxLPHTWukhgWTfgoVw2yUFMfFJjOYuC6IOl2OL2afVmRM2CZwuQtpcWrkMndt4wvjCzszMuYkfL_t-wWQWNR8sBiHhh3eOu3v6gt5B9WWLiOn83adK90K5Kp5G73rC1j7vs24fcNc_-ShaxRsS8BEKhAzMovA4ozGViX8OV_b_W8l2EM5AukakmWXN7Gp9yowVuObDtZOZQ"
OWNER_ID = -236929597

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
