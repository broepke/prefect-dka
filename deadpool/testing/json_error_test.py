import json

# Replace 'your_file.json' with the path to your JSON file
file_path = "deadpool/testing/json_samples/no_infobox.json"

# Open the JSON file and load the data
with open(file_path, "r") as file:
    data = json.load(file)


def get_infobox(infobox_all):
    try:
        infobox = infobox_all[0]["infobox"]
        return infobox
    except Exception as e:
        print(e)
        return None



print(get_infobox(data))
