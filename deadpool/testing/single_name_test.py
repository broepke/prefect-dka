"""
Script to look up a person's birth and death on Wikipedia
"""
import json
import requests
import os


def authenticate_to_wikipedia(username, password):
    """Login to Enterprise Wikimedia Account

    Args:
        username (str): Wiki enterprise username
        password (str): Wiki enterprise password

    Returns:
        str: Access Tonken for Session
    """
    url = "https://auth.enterprise.wikimedia.com/v1/login"
    payload = {"username": username, "password": password}

    response = requests.post(url, json=payload, timeout=5)
    response_json = json.loads(response.text)

    access_token = response_json["access_token"]

    return access_token


def get_infobox(person, access_token):
    """Query Wikipedia and return the Infobox which is the grey box
       on the right hand side of a page which contains all the the
       metadata we're intereted in

    Args:
        person (str): The end of the Wiki URL to the person. "Dick_Van_Dyke"

    Returns:
        json: JSON object of just the infobox from the page.
    """
    url = "https://api.enterprise.wikimedia.com/v2/structured-contents/" + person
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json",
    }
    data = {
        "filters": [{"field": "is_part_of.identifier", "value": "enwiki"}],
        "limit": 1,
    }

    response = requests.post(url, json=data, headers=headers, timeout=5)

    infobox_all = json.loads(response.text)
        
    try:
        infobox = infobox_all[0]["infobox"]

        file_path = person + ".json"

        # Writing the JSON data to a file
        with open(file_path, "w") as file:
            json.dump(infobox_all, file, indent=4)

        return infobox

    except:
        return None


def extract_date(json_data, date_type):
    """
    Extracts birth or death date from a JSON object, handling variations in the key naming.

    Args:
    json_data (list or dict): JSON object representing the data.
    date_type (str): Type of date to extract, 'Born' or 'Died'.

    Returns:
    str: Extracted date or None if not found.
    """
    # Check if the input is a list and iterate through its elements
    if isinstance(json_data, list):
        for item in json_data:
            result = extract_date(item, date_type)
            if result:
                return result

    # Check if the input is a dictionary and process accordingly
    elif isinstance(json_data, dict):
        # Check for both exact match and match with colon in the 'name' key
        # Also check in 'value' key for directly provided dates
        for key, value in json_data.items():
            if key == 'name' and (value.strip().lower() == date_type.lower() or value.strip().lower() == f"{date_type.lower()}:"):
                return json_data.get('value')
            elif key == 'value' and date_type.lower() in value.lower():
                return value

            # Otherwise, iterate through nested structures
            if isinstance(value, (dict, list)):
                result = extract_date(value, date_type)
                if result:
                    return result

    # Return None if the date is not found
    return None



def dead_pool_status_check():
    """Main Flow Logic"""

    # Get the credetials from Prefect

    username = os.environ.get("WIKI_USER")
    password = os.environ.get("WIKI_PASS")

    # Login and Get the Access Token
    access_token = authenticate_to_wikipedia(username=username, password=password)

    wiki_page = "Tina_Turner"
    # wiki_page = "O._J._Simpson"
    # wiki_page = "Kanye_West"
    # wiki_page = "Willie_Mays"
    wiki_page = "Tom_Brokaw"

    # Set the person you wish to check status of
    person = wiki_page.replace("_", " ")

    # Get the Infobox JSON
    infobox = get_infobox(wiki_page, access_token)
    if infobox != None:
        # Initialize variables to hold birth and death dates
        birth_date = None
        death_date = None

        # Get bith and death dates
        birth_date = extract_date(infobox, "Born")
        death_date = extract_date(infobox, "Died")

        print(birth_date)
        print(death_date)
    else:
        print("Didn't find infobox")

if __name__ == "__main__":
    dead_pool_status_check()
