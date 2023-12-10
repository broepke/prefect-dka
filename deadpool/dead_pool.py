"""
Script to look up a person's birth and death on Wikipedia
"""
import requests
import re
import json
from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret


@task(name="Authenticate to Wikipedia")
def authenticate_to_wikipedia(username, password):
    url = "https://auth.enterprise.wikimedia.com/v1/login"
    payload = {"username": username, "password": password}

    response = requests.post(url, json=payload)
    response_json = json.loads(response.text)

    access_token = response_json["access_token"]

    return access_token


@task(name="Get Infobox")
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

    response = requests.post(url, json=data, headers=headers)

    infobox_all = json.loads(response.text)
    infobox = infobox_all[0]["infobox"]

    return infobox


def find_field_in_json(json_data, field_name):
    """Parse JSON Infobox ojbect data for Birth and Death Dates

    Args:
        json_data (json): Wikipedia Infobox JSON
        field_name (str): The JSON item desired

    Returns:
        str: The extracted value from the desired key.
    """
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == field_name:
                return value
            elif isinstance(value, (dict, list)):
                result = find_field_in_json(value, field_name)
                if result is not None:
                    return result
    elif isinstance(json_data, list):
        for item in json_data:
            if isinstance(item, dict):
                if item.get("name") == field_name and item.get("type") == "field":
                    return item.get("value")
                elif "has_parts" in item:
                    result = find_field_in_json(item["has_parts"], field_name)
                    if result is not None:
                        return result
    return None


@task(name="Calculate Age")
def get_age(birth_date, death_date):
    """Get the age of the person based on two datetime objects

    Args:
        birth_date (datetime): Birth Date
        death_date (datetime): Date of death if exists

    Returns:
        int: The person's age
    """
    if death_date:
        age = death_date.year - birth_date.year
    else:
        current_date = datetime.now()
        age = (
            current_date.year
            - birth_date.year
            - (
                (current_date.month, current_date.day)
                < (birth_date.month, birth_date.day)
            )
        )
    return age


@flow(name="Verify Deadpool Alive or Dead and Age")
def dead_pool_status_check():
    logger = get_run_logger()

    # Get the credetials from Prefect
    wiki_user_block = Secret.load("wiki-user")
    wiki_pass_block = Secret.load("wiki-pass")

    username = wiki_user_block.get()
    password = wiki_pass_block.get()

    # Login and Get the Access Token
    access_token = authenticate_to_wikipedia(username=username, password=password)

    # Set the person you wish to check status of
    WIKI_PAGE = "Robert_Durst"
    logger.info("Person: %s", WIKI_PAGE.replace("_", " "))

    # Get the Infobox JSON
    infobox = get_infobox(WIKI_PAGE, access_token)

    # Initialize variables to hold birth and death dates
    birth_date = None
    death_date = None

    # Get bith and death dates
    birth_date = find_field_in_json(infobox, "Born")
    death_date = find_field_in_json(infobox, "Died")

    # Regular expression pattern for dates (assuming format "Month Day, Year")
    date_pattern = r"(\bJanuary|\bFebruary|\bMarch|\bApril|\bMay|\bJune|\bJuly|\bAugust|\bSeptember|\bOctober|\bNovember|\bDecember) \d{1,2}, \d{4}"

    # Start with birth dates
    extracted_birth_date = re.search(date_pattern, birth_date)
    birth_date_str = extracted_birth_date.group(0) if extracted_birth_date else None
    birth_date = datetime.strptime(birth_date_str, "%B %d, %Y")
    logger.info("Birth Date (datetime object): %s", birth_date)

    # Now conditionally do death dates
    if death_date:
        extracted_death_date = re.search(date_pattern, death_date)
        death_date_str = extracted_death_date.group(0) if extracted_death_date else None
        death_date = datetime.strptime(death_date_str, "%B %d, %Y")
        logger.info("Death Date (datetime object): %s", death_date)
        logger.info("DEAD: Deadpool Winning Pick!!!")

    # Calculate the person's age
    age = get_age(birth_date, death_date)
    logger.info("Age: %s", age)

    # If they're not dead yet, log that
    if birth_date and not death_date:
        logger.info("ALIVE: Better Luck Next Time!")


if __name__ == "__main__":
    dead_pool_status_check()
