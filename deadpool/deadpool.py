"""
Script to look up a person's birth and death on Wikipedia
"""
import re
import json
from datetime import datetime
import requests
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from utilities.util_slack import death_notification
from utilities.util_slack import bad_wiki_page
from utilities.util_snowflake import get_existing_values
from utilities.util_snowflake import update_rows
from utilities.util_snowflake import get_snowflake_connection
from utilities.util_twilio import send_sms_via_api


@task(name="Authenticate to Wikipedia")
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
    logger = get_run_logger()

    WIKI_URL = "https://api.enterprise.wikimedia.com/v2/structured-contents/"
    url = WIKI_URL + person
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
        return infobox
    except Exception as e:
        logger.info(e)
        return None


def extract_date(json_data, date_type):
    """
    Extracts birth or death date from a JSON object,
    handling variations in the key naming and ignoring marriage dates.

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
        for key, value in json_data.items():
            if key == "name" and (
                value.strip().lower() == date_type.lower()
                or value.strip().lower() == f"{date_type.lower()}:"
            ):
                return json_data.get("value")
            elif key == "value":
                # Check if the value contains a marriage date and skip it
                if "m." in value and any(char.isdigit() for char in value):
                    continue
                # Check for death date in the value
                if date_type.lower() in value.lower():
                    return value

            # Otherwise, iterate through nested structures
            if isinstance(value, (dict, list)):
                result = extract_date(value, date_type)
                if result:
                    return result

    # Return None if the date is not found
    return None


@task(name="Extract Date Time Object from Wiki Date")
def extract_datetime_object(date_string):
    """Take the format May 24, 2023 and converts to Python
       datetime object

    Args:
        date_string (str): date to be converted

    Returns:
        datetime: Python datetime object
    """
    logger = get_run_logger()
    # Regular expression pattern for dates (assuming format "Month Day, Year")
    date_pattern = r"(?:(\d{1,2}) )?(January|February|March|April|May|June|July|August|September|October|November|December)(?: (\d{1,2}),)? (\d{4})"  # noqa: E501

    logger.info("Date String to Extract %s", date_string)
    extracted_date = re.search(date_pattern, date_string)
    if extracted_date:
        day, month, day2, year = extracted_date.groups()
        day = day or day2  # Use the day that was matched
        date_str = f"{month} {day}, {year}"
        date = datetime.strptime(date_str, "%B %d, %Y")
        return date
    else:
        return None


@task(name="Calculate Age")
def get_age(b_date, d_date):
    """Get the age of the person based on two datetime objects

    Args:
        b_date (datetime): Birth Date
        d_date (datetime): Date of death if exists

    Returns:
        int: The person's age
    """
    if d_date:
        age = d_date.year - b_date.year
        if (d_date.month, d_date.day) < (b_date.month, b_date.day):
            age -= 1
    else:
        current_date = datetime.now()
        age = current_date.year - b_date.year
        if (current_date.month, current_date.day) < (b_date.month, b_date.day):
            age -= 1
    return age


@flow(name="Verify Deadpool Alive or Dead and Age")
def dead_pool_status_check():
    """Main Flow Logic"""
    logger = get_run_logger()

    # Get the credetials from Prefect
    wiki_user_block = Secret.load("wiki-user")
    wiki_pass_block = Secret.load("wiki-pass")
    w_user = wiki_user_block.get()
    w_pass = wiki_pass_block.get()

    # Login and Get the Access Token
    access_token = authenticate_to_wikipedia(username=w_user, password=w_pass)

    connection = get_snowflake_connection("snowflake-dka")

    # Get the full list of people to check
    names_to_check = get_existing_values(
        connection,
        database_name="DEADPOOL",
        schema_name="PROD",
        table_name="PICKS",
        column_name="NAME, WIKI_PAGE",
        conditionals="WHERE DEATH_DATE IS NULL AND YEAR = 2024",
        return_list=False,
    )

    # Iterate over the list of links to update and build the final DF
    for index, row in names_to_check.iterrows():
        name = row["NAME"]
        wiki_page = row["WIKI_PAGE"]

        # Strip leading and trailing spaces just in case there are in the DB
        name = name.strip()
        wiki_page = wiki_page.strip()

        # Get the Infobox JSON
        infobox = get_infobox(wiki_page, access_token)
        if infobox is not None:
            # Initialize variables to hold birth and death dates
            birth_date = None
            death_date = None

            # Get bith and death dates
            birth_date = extract_date(infobox, "Born")
            death_date = extract_date(infobox, "Died")

            if birth_date:
                birth_date = extract_datetime_object(birth_date)
                logger.info("Birth Date (datetime object): %s", birth_date)

            if death_date:
                death_date = extract_datetime_object(death_date)

            if birth_date:
                # Calculate the person's age
                age = get_age(birth_date, death_date)
                logger.info("Age: %s", age)

            # Now conditionally do death dates
            if death_date:
                logger.info("Death Date (datetime object): %s", death_date)
                logger.info("DEAD: Deadpool Winning Pick!!!")

                name = name.replace("'", "''")
                set_string = f"""SET birth_date = '{birth_date}', death_date \
                    = '{death_date}', age = {age}"""
                conditionals = f"""WHERE name = '{name}'
                """
                update_rows(
                    connection=connection,
                    database_name="DEADPOOL",
                    schema_name="PROD",
                    table_name="PICKS",
                    set_string=set_string,
                    conditionals=conditionals,
                )

                death_notification(
                    person=name,
                    birth_date=birth_date,
                    death_date=death_date,
                    age=age,
                    emoji=":skull_and_crossbones:",
                )

                # Send out SMS messages to all Opted in Users
                sms_to_list = get_existing_values(
                    connection=connection,
                    database_name="DEADPOOL",
                    schema_name="PROD",
                    table_name="DRAFT_OPTED_IN",
                    column_name="SMS",
                )

                sms_message = f"{name} has died at the age of {age}"
                send_sms_via_api(sms_message, sms_to_list)

            # If they're not dead yet, log that
            if birth_date and not death_date:
                logger.info("ALIVE: Better Luck Next Time!")

                name = name.replace("'", "''")
                set_string = f"SET birth_date = '{birth_date}', age = {age}"
                conditionals = f"WHERE name = '{name}'"

                update_rows(
                    connection=connection,
                    database_name="DEADPOOL",
                    schema_name="PROD",
                    table_name="PICKS",
                    set_string=set_string,
                    conditionals=conditionals,
                )

        else:
            bad_wiki_page(name, wiki_page, ":memo:")
            logger.info("No valid wiki page for %s", name)


if __name__ == "__main__":
    dead_pool_status_check()
