import requests

url = "https://api.telegram.org/bot{token}/{method}".format(
    token="ur token",
    method = "setWebhook"
    #method="getWebhookinfo"
    #method = "deleteWebhook"
)
data = {"url": "ur url"}
r = requests.post(url, data=data)
print(r.json())
 