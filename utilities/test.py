from prefect.blocks.system import Secret
import requests

apify_api_url_secret = Secret.load("apify-base-api")
apify_bearer_secret = Secret.load("apify-base-bearer")

apify_api_url = apify_api_url_secret.get()
apify_bearer = apify_bearer_secret.get()

prompt = "Tell me a story"

headers = {"Authorization": apify_bearer}
payload = {
    "question": prompt,
}

try:
    response = requests.post(apify_api_url,
                             headers=headers,
                             json=payload,
                             timeout=60)
    output = response.json()
    print(output["text"])
except Exception as e:
    print(e)
