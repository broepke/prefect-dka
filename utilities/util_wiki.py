"""
Wikipedia lookup tools
"""
import requests
from datetime import datetime


def fetch_wikidata(params):
    """Use the Wiki Data API to fetch via REST

    Args:
        params (dict): dictionary of REST request params

    Returns:
        json: JSON object of Wiki response
    """
    wikidata_url = "https://www.wikidata.org/w/api.php"
    try:
        response = requests.get(wikidata_url, params=params, timeout=5)
        return response.json()
    except requests.exceptions.RequestException as e:
        return f"There was an error: {e}"


def resolve_redirect(title):
    """Function to assist when Wiki Pages have 1-n redirects

    Args:
        title (str): Page URL title (end of URL)

    Returns:
        str: Fully resolved title
    """
    wikipedia_api_url = "https://en.wikipedia.org/w/api.php"

    def query_wikipedia(t):
        params = {"action": "query", "titles": t, "redirects": 1, "format": "json"}
        response = requests.get(wikipedia_api_url, params=params, timeout=5)
        return response.json()

    data = query_wikipedia(title)

    # Loop to follow through all redirects
    while "redirects" in data["query"]:
        # Get the last redirect target
        redirects = data["query"]["redirects"]
        final_redirect = redirects[-1]["to"]
        data = query_wikipedia(final_redirect)

    if "normalized" in data["query"]:
        final_title = data["query"]["normalized"][0]["to"]
    elif "pages" in data["query"]:
        page_id = next(iter(data["query"]["pages"]))
        final_title = data["query"]["pages"][page_id]["title"]
    else:
        final_title = title  # No normalization or redirects, use the original

    return final_title


def get_wiki_id_from_page(page_title):
    """Function to get the Wikidata ID from a Wikipedia page title

    Args:
        page_title (str): Page URL title (end of URL)

    Returns:
        str: Wiki Data identifier
    """
    final_title = resolve_redirect(page_title)  # Resolve redirects first
    params = {
        "action": "wbgetentities",
        "format": "json",
        "sites": "enwiki",
        "titles": final_title,
        "languages": "en",
        "redirects": "yes",
    }
    data = fetch_wikidata(params)
    if isinstance(data, str) or "entities" not in data or len(data["entities"]) == 0:
        return None

    entity_id = list(data["entities"].keys())[0]
    return entity_id


def get_birth_death_date(identifier, entity_id):
    """Get a birth or death data based

    Args:
        identifier (str): Wiki Data ID
        entity_id (str): P569 (birth) or P570 (death)

    Returns:
        datetime: Date of requested entitiy
    """
    params = {
        "action": "wbgetentities",
        "ids": entity_id,
        "format": "json",
        "languages": "en",
    }

    # Fetch the API
    data = fetch_wikidata(params)

    # Extract birth or death date
    date_str = data["entities"][entity_id]["claims"][identifier][0]["mainsnak"]["datavalue"]["value"]["time"]

    # Remove the '+' or '-' sign from the date string if present
    if date_str.startswith("-") or date_str.startswith("+"):
        date_str = date_str[1:]

    # Check the format of the date string and parse accordingly
    if date_str.endswith("-00-00T00:00:00Z"):  # Year only
        date_obj = datetime.strptime(date_str, "%Y-00-00T00:00:00Z")
    elif date_str[5:7] != "00" and date_str.endswith(
        "-00T00:00:00Z"
    ):  # Year and month only
        date_obj = datetime.strptime(date_str, "%Y-%m-00T00:00:00Z")
    else:  # Full date
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

    return date_obj
