"""
Wikipedia lookup tools
"""

import time
import requests
from functools import lru_cache
from SPARQLWrapper import SPARQLWrapper, JSON
from datetime import datetime


def fetch_wikidata(params, retries=3, delay=2):
    """Fetch Wikidata with retries on failure.

    Args:
        params (dict): Request parameters for the Wikidata API.
        retries (int): Number of retries before giving up.
        delay (int): Delay in seconds between retries.

    Returns:
        dict: JSON response from the API, or None if all retries fail.
    """
    for attempt in range(retries):
        try:
            response = requests.get(
                "https://www.wikidata.org/w/api.php", params=params, timeout=5
            )
            response.raise_for_status()  # Raise an error for HTTP issues
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(delay)  # Wait before retrying

    print("All retries failed.")
    return None  # Return None if all retries fail


def resolve_redirect(title):
    """Function to assist when Wiki Pages have 1-n redirects

    Args:
        title (str): Page URL title (end of URL)

    Returns:
        str: Fully resolved title
    """
    wikipedia_api_url = "https://en.wikipedia.org/w/api.php"

    def query_wikipedia(t):
        params = {
            "action": "query",
            "titles": t,
            "redirects": 1,
            "format": "json",
        }  # noqa: E501
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
    if (
        isinstance(data, str) or "entities" not in data or len(data["entities"]) == 0  # noqa: E501
    ):
        return None

    entity_id = list(data["entities"].keys())[0]
    return entity_id


@lru_cache(maxsize=128)
def get_birth_death_date(wikidata_prop_id, wikidata_q_number):
    """Get a birth or death date from Wikidata.

    Args:
        wikidata_prop_id (str): Property ID, e.g., P569 (birth) or P570 (death).
        wikidata_q_number (str): Wiki Data ID (Q Number).

    Returns:
        datetime: Date of the requested entity, or None if not found.
    """
    params = {
        "action": "wbgetentities",
        "ids": wikidata_q_number,
        "format": "json",
        "languages": "en",
    }

    data = fetch_wikidata(params)

    if not data or "entities" not in data or wikidata_q_number not in data["entities"]:
        print(f"Invalid data for {wikidata_q_number}.")
        return None

    try:
        claims = data["entities"][wikidata_q_number]["claims"]
        if wikidata_prop_id not in claims:
            print(f"Property {wikidata_prop_id} not found for {wikidata_q_number}.")
            return None

        date_str = claims[wikidata_prop_id][0]["mainsnak"]["datavalue"]["value"]["time"]
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error accessing data: {e}")
        print(f"Data received: {data}")
        return None

    if date_str.startswith("-") or date_str.startswith("+"):
        date_str = date_str[1:]

    try:
        if date_str.endswith("-00-00T00:00:00Z"):
            date_obj = datetime.strptime(date_str, "%Y-00-00T00:00:00Z")
        elif date_str[5:7] != "00" and date_str.endswith("-00T00:00:00Z"):
            date_obj = datetime.strptime(date_str, "%Y-%m-00T00:00:00Z")
        else:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return None

    return date_obj


def get_birth_death_date_sparql(entity_id):
    """Fetches the birth and/or death dates of a given entity
    from Wikidata using SPARQL.

    Args:
        entity_id (str): Wikidata entity ID.

    Returns:
        tuple: A tuple containing the birth date and death date as
        datetime objects or None.
    """
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

    # Updated query to optionally match birth and death dates
    query = f"""
    SELECT ?birthDate ?deathDate
    WHERE {{
      OPTIONAL {{ wd:{entity_id} wdt:P569 ?birthDate. }}
      OPTIONAL {{ wd:{entity_id} wdt:P570 ?deathDate. }}
    }}
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()

    birth_date, death_date = None, None

    for result in results["results"]["bindings"]:
        if "birthDate" in result:
            birth_date_str = result["birthDate"]["value"]
            birth_date = datetime.strptime(birth_date_str, "%Y-%m-%dT%H:%M:%SZ")  # noqa: E501
        if "deathDate" in result:
            death_date_str = result["deathDate"]["value"]
            death_date = datetime.strptime(death_date_str, "%Y-%m-%dT%H:%M:%SZ")  # noqa: E501

    return birth_date, death_date
