"""
Utilities for APIFY Driven Endpoints
"""
import requests
from prefect.blocks.system import Secret


def the_arbiter(prompt):
    """Chatbot API call to LangChang LLM

    Args:
        prompt (str): the prompt

    Returns:
        str: Text output from the LLM
    """

    apify_api_url_secret = Secret.load("apify-base-api")
    apify_bearer_secret = Secret.load("apify-base-bearer")

    apify_api_url = apify_api_url_secret.get()
    apify_bearer = apify_bearer_secret.get()

    headers = {"Authorization": apify_bearer}
    payload = {
        "question": prompt,
    }

    try:
        response = requests.post(
            apify_api_url, headers=headers, json=payload, timeout=60
        )
        output = response.json()
        return output["text"]
    except Exception as e:
        return "The Arbiter is sleeping: " + str(e)
