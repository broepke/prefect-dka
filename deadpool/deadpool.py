"""
Script to look up a person's birth and death on Wikipedia
"""
from datetime import datetime
import requests
import urllib.parse
from prefect import task, flow, get_run_logger
from utilities.util_slack import death_notification
from utilities.util_slack import bad_wiki_page
from utilities.util_snowflake import get_existing_values
from utilities.util_snowflake import update_rows
from utilities.util_snowflake import get_snowflake_connection
from utilities.util_twilio import send_sms_via_api


def fetch_wikidata(params):
    url = "https://www.wikidata.org/w/api.php"
    try:
        response = requests.get(url, params=params)
        return response.json()  # Return JSON content here
    except requests.exceptions.RequestException as e:
        return f"There was an error: {e}"


def get_wiki_id_from_page(page_title):
    params = {
        "action": "wbgetentities",
        "format": "json",
        "sites": "enwiki",  # This specifies the English Wikipedia
        "titles": page_title,
        "languages": "en",  # Language for labels and descriptions
    }

    # Fetch API
    data = fetch_wikidata(params)

    # Extract the Wikidata entity ID
    entity_id = list(data["entities"].keys())[0]
    return entity_id


def get_birth_death_date(identifier, entity_id):
    # Create parameters
    params = {
        "action": "wbgetentities",
        "ids": entity_id,
        "format": "json",
        "languages": "en",
    }

    # Fetch the API
    data = fetch_wikidata(params)

    # Extract birth date
    date_str = data["entities"][entity_id]["claims"][identifier][0]["mainsnak"]["datavalue"]["value"]["time"]

    # Check the format of the date string and parse accordingly
    if date_str.endswith("-00-00T00:00:00Z"):  # Year only
        date_obj = datetime.strptime(date_str, "+%Y-00-00T00:00:00Z")
    else:  # Full date
        date_obj = datetime.strptime(date_str, "+%Y-%m-%dT%H:%M:%SZ")

    return date_obj


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
        wiki_page = urllib.parse.unquote(wiki_page)

        # Initialize variables to hold birth and death dates
        birth_date = None
        death_date = None

        # Fetch the Wiki ID from Wiki Data
        wiki_id = get_wiki_id_from_page(wiki_page)
        logger.info(str(wiki_page) + " : " + str(wiki_id))
        if wiki_id != "-1":
            # Get bith and death dates
            birth_date = get_birth_death_date("P569", wiki_id)
            logger.info("Birth Date: %s", birth_date)

            try:
                death_date = get_birth_death_date("P570", wiki_id)
                logger.info("Death Date: %s", death_date)
            except Exception as e:
                logger.info("No Death Date for: %s code: %s", wiki_page, e)
                None

            if birth_date:
                # Calculate the person's age
                age = get_age(birth_date, death_date)
                logger.info("Age: %s", age)

            # Now conditionally do death dates
            if death_date:
                logger.info("Death Date (datetime object): %s", death_date)

                name = name.replace("'", "''")
                set_string = f"""SET BIRTH_DATE = '{birth_date}', DEATH_DATE \
                    = '{death_date}', AGE = {age}, WIKI_ID = '{wiki_id}'"""
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
                
                logger.info(sms_to_list)

                sms_message = f"{name} has died at the age of {age}"
                send_sms_via_api(sms_message, sms_to_list)

            # If they're not dead yet, log that
            if birth_date and not death_date:
                name = name.replace("'", "''")
                set_string = f"SET BIRTH_DATE = '{birth_date}', AGE = {age}, \
                    WIKI_ID = '{wiki_id}'"
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
