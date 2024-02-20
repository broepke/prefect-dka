from utilities.util_wiki import get_birth_death_date_sparql

birth_date, death_date = get_birth_death_date_sparql('Q552806')

print(birth_date)
print(death_date)
