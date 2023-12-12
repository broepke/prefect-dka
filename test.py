def update_rows(
    connection,
    database_name,
    schema_name,
    table_name,
    set_string,
    conditionals=None
):
    statement = f"UPDATE {database_name}.{schema_name}.{table_name} {set_string} {conditionals};"
    print(statement)
    
    return

birth_date = 1
death_date = 2
age = 12
name = "brian"

set_string = f"""SET birth_date = '{birth_date}', death_date = '{death_date}', age = {age}"""
conditionals = f"""WHERE name = '{name}'
"""
update_rows(
    connection="foo",
    database_name="DEADPOOL",
    schema_name="ONE",
    table_name="PICKS",
    set_string=set_string,
    conditionals=conditionals,
)