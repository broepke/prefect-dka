"""
Script to look up a person's birth and death on Wikipedia
"""
from datetime import datetime
import urllib.parse
# import logging
from prefect import flow, get_run_logger
from utilities.util_snowflake import get_existing_values
from utilities.util_snowflake import update_rows
from utilities.util_snowflake import get_snowflake_connection
from utilities.util_wiki import get_birth_death_date


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


@flow(name="Add Dates to NNDB")
def deadpool_nndb_date_updates():
    """Main Flow Logic"""
    logger = get_run_logger()
    # logger.setLevel(logging.DEBUG)

    connection = get_snowflake_connection("snowflake-dka")

    # Get the full list of people to check
    names_to_check = get_existing_values(
        connection,
        database_name="DEADPOOL",
        schema_name="PROD",
        table_name="NNDB_DATES",
        column_name="ID, WIKI_ID",
        conditionals="WHERE BIRTH_DATE IS NULL",
        return_list=False,
    )

    # Iterate over the list of links to update and build the final DF
    for index, row in names_to_check.iterrows():
        id = row["ID"]
        wiki_id = row["WIKI_ID"]

        # Strip leading and trailing spaces just in case there are in the DB
        id = id.strip()
        wiki_id = wiki_id.strip()
        wiki_id = urllib.parse.unquote(wiki_id)

        # Initialize variables to hold birth and death dates
        birth_date = None
        death_date = None

        logger.info(str(wiki_id))
        if wiki_id != "-1":
            try:
                # Get bith and death dates
                birth_date = get_birth_death_date("P569", wiki_id)
                logger.info("Birth Date: %s", birth_date)

                try:
                    death_date = get_birth_death_date("P570", wiki_id)
                    logger.info("Death Date: %s", death_date)
                except Exception as e:
                    logger.info("No Death Date for: %s code: %s", wiki_id, e)
                    None

                if birth_date:
                    # Calculate the person's age
                    age = get_age(birth_date, death_date)
                    logger.info("Age: %s", age)

                # Now conditionally do death dates
                if death_date:
                    logger.info("Death Date (datetime object): %s", death_date)

                    set_string = f"""SET BIRTH_DATE = '{birth_date}',
                    DEATH_DATE = '{death_date}', \
                    AGE = {age}, WIKI_ID = '{wiki_id}'"""
                    conditionals = f"""WHERE id = '{id}'
                    """
                    update_rows(
                        connection=connection,
                        database_name="DEADPOOL",
                        schema_name="PROD",
                        table_name="NNDB_DATES",
                        set_string=set_string,
                        conditionals=conditionals,
                    )

                # If they're not dead yet, log that
                if birth_date and not death_date:
                    set_string = f"SET BIRTH_DATE = '{birth_date}', \
                        AGE = {age}, WIKI_ID = '{wiki_id}'"
                    conditionals = f"WHERE id = '{id}'"

                    update_rows(
                        connection=connection,
                        database_name="DEADPOOL",
                        schema_name="PROD",
                        table_name="NNDB_DATES",
                        set_string=set_string,
                        conditionals=conditionals,
                    )
            except Exception as e:
                logger.info("No Birth Date Found %s with error: %s",
                            wiki_id, e
                            )
        else:
            logger.info("No valid wiki page for %s", wiki_id)


if __name__ == "__main__":
    deadpool_nndb_date_updates()
