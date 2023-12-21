import re
from datetime import datetime


def extract_datetime_object(date_string):
    """Extracts a date in the format 'Month Day, Year' or 'Day Month Year'
       and converts it to a Python datetime object.

    Args:
        date_string (str): The date to be converted.

    Returns:
        datetime: Python datetime object if the date is found, otherwise None.
    """
    # Regular expression pattern for dates
    date_pattern = r"(?:(\d{1,2}) )?(January|February|March|April|May|June|July|August|September|October|November|December)(?: (\d{1,2}),)? (\d{4})"

    # Extract the date
    extracted_date = re.search(date_pattern, date_string)
    if extracted_date:
        day, month, day2, year = extracted_date.groups()
        day = day or day2  # Use the day that was matched
        date_str = f"{month} {day}, {year}"
        date = datetime.strptime(date_str, "%B %d, %Y")
        return date
    else:
        return None


# Example usage
print(
    extract_datetime_object(
        "Emory Andrew Tate III 1 December 1986 Washington, D.C., U.S."
    )
)
print(extract_datetime_object("Kanye Omari West June 8, 1977 Atlanta, Georgia, U.S."))
print(
    extract_datetime_object(
        "Thomas John Brokaw February 6, 1940 Webster, South Dakota, U.S."
    )
)
