from datetime import datetime
import urllib.parse
from utilities.util_wiki import get_birth_death_date

wiki_id = "Q2252"

birth_date = get_birth_death_date("P569", wiki_id)
death_date = get_birth_death_date("P570", wiki_id)

print(birth_date)
print(death_date)
