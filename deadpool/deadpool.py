"""
Script to look up a person's birth and death on Wikipedia
"""
from datetime import datetime
import urllib.parse
from prefect import task, flow, get_run_logger
from utilities.util_slack import death_notification
from utilities.util_slack import bad_wiki_page
from utilities.util_snowflake import get_existing_values
from utilities.util_snowflake import update_rows
from utilities.util_snowflake import get_snowflake_connection
from utilities.util_twilio import send_sms_via_api
from utilities.util_wiki import get_wiki_id_from_page
from utilities.util_wiki import get_birth_death_date


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
    # This will skip any person that doesn't have either wiki page or id
    # and ski anyone who's already dead to avoid processing unknown people
    names_to_check = get_existing_values(
        connection,
        database_name="DEADPOOL",
        schema_name="PROD",
        table_name="PICKS",
        column_name="NAME, WIKI_PAGE, WIKI_ID",
        conditionals="WHERE DEATH_DATE IS NULL AND YEAR = 2024 AND WIKI_PAGE IS NOT NULL AND WIKI_ID IS NOT NULL",  # noqa: E501
        return_list=False,
    )

    # Iterate over the list of links to update and build the final DF
    for index, row in names_to_check.iterrows():
        name = row["NAME"]
        wiki_page = row["WIKI_PAGE"]
        wiki_id = row["WIKI_ID"]

        # Strip leading and trailing spaces just in case there are in the DB
        name = name.strip()
        if wiki_page:
            wiki_page = wiki_page.strip()
            wiki_page = urllib.parse.unquote(wiki_page)
        if wiki_id:
            wiki_id = wiki_id.strip()

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

                sms_message = f"{name} has died at the age {age}."
                send_sms_via_api(sms_message, sms_to_list)

                arbiter_sms_message = f"Insult the player about their pick \
                    {name} that died at the age {age}.  \
                    Ensure the message is no more than 15 words."
                send_sms_via_api(
                    arbiter_sms_message, sms_to_list, arbiter=True
                )  # noqa: E501

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
