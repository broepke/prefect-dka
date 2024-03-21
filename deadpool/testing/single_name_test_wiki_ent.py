"""
Script to look up a person's birth and death on Wikipedia
"""
import json
import requests
import os
import pandas as pd


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

        # file_path = person + ".json"

        # # Writing the JSON data to a file
        # with open(file_path, "w") as file:
        #     json.dump(infobox_all, file, indent=4)

        return infobox

    except Exception as e:
        print(e)
        return None


def extract_date(json_data, date_type):
    """
    Extracts birth or death date from a JSON object,
    handling variations in the key naming.

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
            if key == "name" and (
                value.strip().lower() == date_type.lower()
                or value.strip().lower() == f"{date_type.lower()}:"
            ):
                return json_data.get("value")
            elif key == "value" and date_type.lower() in value.lower():
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
    access_token = authenticate_to_wikipedia(username, password)

    wiki_pages = [
        # "Lee_Majors",
        # "Glynis_Johns",
        # "Jon_Anderson",
        # "Michael_York",
        # "Chen_Shui-bian",
        # "Jay_Stewart",
        # "Jerry_Kramer",
        # "Carl_XVI_Gustaf",
        # "Gene_Hackman",
        # "Jeff_Garlin",
        # "Mark_Spitz",
        # "Ryan_Adams",
        # "Liam_Payne",
        # "Steve_Perry",
        # "Mark_Ellis_(lawyer)",
        # "Jean-Bertrand_Aristide",
        # "Bob_Newhart",
        # "Robby_Benson",
        # "Clint_Eastwood",
        # "Stevie_Wonder",
        # "Rocco_DiSpirito",
        # "Ron_Paul",
        # "Nichelle_Nichols",
        # "Göran_Persson",
        # "Wanda_Jackson",
        # "Mark_Warner",
        # "Shirley_Muldowney",
        # "Leonard_Boswell",
        # "Bruce_Willis",
        # "Rick_Snyder",
        # "Michael_Gerson",
        # "Toni_Braxton",
        # "Richard_T._Burke",
        # "Gregory_Itzin",
        # "KT_Tunstall",
        # "Jordan",
        # "Paris",
        # "Julius_Genachowski",
        # "Randy_Travis",
        # "Mike_Simpson",
        # "John_Doolittle",
        # "Grace_Napolitano",
        # "Amitabh_Bachchan",
        # "Serena_Williams",
        # "Jeff_Bagwell",
        # "Scott_DesJarlais",
        # "Ben_Nelson",
        # "Dave_Gahan",
        # "Jeff_Bingaman",
        # "Michael_Flatley",
        # "Kenny_Rogers",
        # "John_Garamendi",
        # "William_W._McGuire",
        # "Ira_Magaziner",
        # "Gray_Davis",
        # "Nick_Jonas",
        # "Blythe_Danner",
        # "Chris_Frantz",
        # "Bill_Nelson",
        # "Violent_J",
        # "Dick_Van_Dyke",
        # "Tony_Dorsett",
        "Rosie_O%27Donnell",
        # "Silvio_Berlusconi",
        # "Ryuichi_Sakamoto",
        # "Val_Kilmer",
        # "Junior_Byles",
        # "Chris_Lemmon",
        # "Rosalynn_Carter",
        # "Linda_Evangelista",
        # "Murray_Waas",
        # "Ólafur_Ragnar_Grímsson",
        # "Dick_Armey",
        # "Bobby_Rydell",
        # "Mandy_Patinkin",
        # "Jim_Yong_Kim",
        # "Darryl_Strawberry",
        # "Stuart_Bowen",
        # "Tom_Gegax",
        # "Kurt_Schrader",
        # "Earl_Campbell",
        # "Hosni_Mubarak",
        # "Dick_Cheney",
        # "Hulk_Hogan",
        # "Eddie_Deezen",
        # "Ruth_Buzzi",
        # "Al_Rantel",
        # "Paddy_Considine",
        # "Brian_Littrell",
        # "Dana_White",
        # "Liam_Fox",
        # "Brooks_Robinson",
        # "Mwai_Kibaki",
        # "Tom_Daschle",
        # "Belinda_Carlisle",
        # "Jessie_J",
        # "Renée_Fleming",
        # "Amanda_Bynes",
        # "Louis_Farrakhan",
        # "James_Lankford",
        # "Niki_Taylor",
        # "Brad_Sherman",
        # "Roberta_Flack",
        # "Bret_Hart",
        # "Fred_Upton",
        # "Billy_Corgan",
        # "Marc_Summers",
        # "Regis_Philbin",
        # "Ne-Yo",
        # "Betty_Sutton",
        # "Bono",
        # "Pope_Benedict_XVI",
        # "Harry_Reid",
        # "Chris_Carney",
        # "Paul_Gosar",
        # "Chris_Van_Hollen",
        # "Mark_Hoppus",
        # "Josh_Homme",
        # "Bob_Corker",
        # "Jim_Costa",
        # "Earl_Pomeroy",
        # "Lindsey_Graham",
        # "Mangosuthu_Buthelezi",
        # "Eric_Shinseki",
        # "Chris_Brown",
        # "Bill_Halter",
        # "Herschel_Walker",
        # "Sabrina_Bryan",
        # "Jack_Van_Impe",
        # "Bam_Margera",
        # "John_Milius",
        # "Eric_Braeden",
        # "Bobby_Brown",
        # "Joe_Walsh",
        # "Pat_Toomey",
        # "Howard_Kaylan",
        # "André_Carson",
        # "Mike_Patton",
        # "Brooke_Burns",
        # "Empress_Michiko",
        # "Bob_Uecker",
        # "John_Culberson",
        # "Jeff_Merkley",
        # "Lou_Barletta",
        # "Deryck_Whibley",
        # "Princess_Michael_of_Kent",
        # "Anthony_Kiedis",
        # "Michael_Douglas",
        # "Patty_Murray",
        # "Jimmy_Fallon",
        # "Joycelyn_Elders",
        # "Kelsey_Grammer",
        # "Rosanne_Cash",
        # "Mick_Mars",
        # "Paul_LePage",
        # "Heather_Locklear",
        # "David_Petraeus",
        # "Barbara_Walters",
        # "T._K._Wetherell",
        # "Suzanne_Somers",
        # "Terry_Bradshaw",
        # "Kevin_Rudd",
    ]

    # Initialize an empty DataFrame with column names
    df = pd.DataFrame(columns=["Name", "Birth_Date", "Death_Date"])

    for wiki_page in wiki_pages:
        # Get the Infobox JSON
        infobox = get_infobox(wiki_page, access_token)
        if infobox is not None:
            # Initialize variables to hold birth and death dates
            birth_date = None
            death_date = None

            # Get bith and death dates
            birth_date = extract_date(infobox, "Born")
            death_date = extract_date(infobox, "Died")

            row = {
                "Name": wiki_page,
                "Birth_Date": birth_date,
                "Death_Date": death_date,
            }
            df.loc[len(df)] = row

        else:
            row = {
                "Name": wiki_page,
                "Birth_Date": "********* No Info Box *********",
                "Death_Date": "********* No Info Box *********",
            }
            df.loc[len(df)] = row

    print(df)
    df.to_csv("aribiter_picks.csv")


if __name__ == "__main__":
    dead_pool_status_check()
