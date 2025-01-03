"""
Script to look up a person's birth and death on Wikipedia
"""

import math
from datetime import datetime
import urllib.parse
import hashlib
from prefect import task, flow, get_run_logger
from utilities.util_slack import death_notification
from utilities.util_slack import bad_wiki_page
from utilities.util_snowflake import get_existing_values
from utilities.util_snowflake import update_rows
from utilities.util_snowflake import get_snowflake_connection
from utilities.util_twilio import send_sms_via_api
from utilities.util_wiki import get_wiki_id_from_page
from utilities.util_wiki import get_birth_death_date


# Function to create a hash from given variables
def create_hash(name, wiki_page, wiki_id, age):
    """Creat a hash digest from a given set of strings

    Args:
        name (str): Person's name
        wiki_page (str): Wiki Page Identifier
        wiki_id (str): Wiki Data Identifier 'Q' number
        age (int): Age of the person

    Returns:
        string: sha256 hash digest value
    """
    # Check if 'age' is NaN before converting and using it in the f-string
    if math.isnan(age):
        age = 0
    else:
        age = int(age)

    combined_string = f"{name}{wiki_page}{wiki_id}{age}"

    return hashlib.md5(combined_string.encode()).hexdigest()


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


@flow(name="Verify Deadpool Alive or Dead and Age", retries=3, retry_delay_seconds=30)
def dead_pool_status_check():
    """Main Flow Logic"""
    logger = get_run_logger()

    connection = get_snowflake_connection("snowflake-dka")

    name = """Linda Nolan"""
    wiki_page = "Linda_Nolan"
    wiki_id = "Q16193271"
    age = 0

    # Strip leading and trailing spaces just in case there are in the DB
    name = name.strip()
    if wiki_page:
        wiki_page = wiki_page.strip()
        wiki_page = urllib.parse.unquote(wiki_page)
    if wiki_id:
        wiki_id = wiki_id.strip()

    hash1 = create_hash(name, wiki_page, wiki_id, age)

    # Initialize variables to hold birth and death dates
    birth_date = None
    death_date = None

    # Fetch the Wiki ID from Wiki Data if we don't already have it
    if not wiki_id:
        wiki_id = get_wiki_id_from_page(wiki_page)
    logger.info(str(wiki_page) + " : " + str(wiki_id))
    if wiki_id != "-1":
        # Get bith and death dates
        try:
            birth_date = get_birth_death_date("P569", wiki_id)
            logger.info("Birth Date: %s", birth_date)
        except KeyError:
            logger.info("No Birth Date for: %s", wiki_page)
            None

        try:
            death_date = get_birth_death_date("P570", wiki_id)
            logger.info("Death Date: %s", death_date)
        except KeyError:
            logger.info("No Death Date for: %s", wiki_page)
            None

        if birth_date:
            # Calculate the person's age
            age = get_age(birth_date, death_date)
            logger.info("Age: %s", age)

        # Now conditionally do death dates
        if death_date:
            logger.info("Death Date (datetime object): %s", death_date)

            name = name.replace("'", "''")
            set_string = f"""SET BIRTH_DATE = '{birth_date}', DEATH_DATE = '{death_date}', AGE = {age}, WIKI_ID = '{wiki_id}'"""  # noqa: E501
            conditionals = f"""WHERE name = '{name}'"""
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

            sms_message = f"{name} has died at the age {age}."
            send_sms_via_api(sms_message, sms_to_list)

            arbiter_sms_message = f"Insult the player about their pick {name} that died at the age {age}. Ensure the message is no more than 15 words.  When you calcuate the points be sure to use the formula (50 + (100-AGE))."  # noqa: E501
            send_sms_via_api(
                arbiter_sms_message, sms_to_list, arbiter=True
            )  # noqa: E501

        hash2 = create_hash(name, wiki_page, wiki_id, age)

        # If they're not dead yet, log that
        if birth_date and not death_date and hash1 != hash2:
            wiki_page = wiki_page.replace("'", "''")
            set_string = f"SET BIRTH_DATE = '{birth_date}', AGE = {age}, WIKI_ID = '{wiki_id}'"  # noqa: E501
            conditionals = f"WHERE WIKI_PAGE = '{wiki_page}'"

            update_rows(
                connection=connection,
                database_name="DEADPOOL",
                schema_name="PROD",
                table_name="PICKS",
                set_string=set_string,
                conditionals=conditionals,
            )
        else:
            logger.info("Skipping DB Write, No values changed.")

    else:
        bad_wiki_page(name, wiki_page, ":memo:")
        logger.info("No valid wiki page for %s", name)


if __name__ == "__main__":
    dead_pool_status_check()
