"""General FX-Data Wragling Functions"""

from prefect import task, get_run_logger
from fuzzywuzzy import fuzz
import ast


@task(name="Deduplicate Dataframe")
def dedupe_dataframe(list_of_titles, df, column_name):
    """Removes any row from a dataframe for companies that are in a supplied list

    Args:
        list_of_titles (list): Existing titles to remove
        df (Pandas Dataframe): Complete set of scraped companies

    Returns:
        Pandas Dataframe: The deduplicated dataframe
    """
    logger = get_run_logger()

    filtered_df = df[~df[column_name].isin(list_of_titles)]

    filtered_df = filtered_df.reset_index(drop=True)

    logger.info("%s new companies to upload", len(filtered_df))

    return filtered_df


@task(name="Fuzzy Match Name")
def has_fuzzy_match(value, value_set, threshold=92):
    """_summary_

    Args:
        value (_type_): _description_
        value_set (_type_): _description_
        threshold (int, optional): _description_. Defaults to 90.

    Returns:
        _type_: _description_
    """
    for item in value_set:
        if fuzz.token_sort_ratio(value.lower(), item.lower()) >= threshold:
            return True
    return False


def convert_names(names):
    names_list = [name.strip() for name in names]
    return set(names_list)


@task(name="Deduplicate Data Frame")
def dedupe_and_validate(df, fx_df):
    """_summary_

    Args:
        df (Dataframe): _description_
        fx_df (Dataframe): _description_

    Returns:
        Dataframe: _description_
    """
    df["CHECKED"] = False
    all_org_names = fx_df["NAME_AND_ALT_NAMES"].explode().tolist()

    all_org_names_combined = [
        name.strip() for entry in all_org_names for name in ast.literal_eval(entry)
    ]

    all_org_names_combined = set(all_org_names_combined)
    org_domains = set(fx_df["DOMAIN_NAME"])

    exists_column = []

    for index, row in df.iterrows():
        company_name = str(row["COMPANY_NAME"])
        domain = str(row["DOMAIN"])
        name_alt_names_match = has_fuzzy_match(
            company_name, all_org_names_combined, threshold=92
        )
        if domain is not None:
            domain_match = domain in org_domains
        else:
            domain_match = False

        exists = name_alt_names_match or domain_match
        exists_column.append(exists)
        df.at[index, "CHECKED"] = True

    df["EXISTS_IN_FX"] = exists_column

    df["ORG_ID"] = None
    for index, row in df.iterrows():
        if row["EXISTS_IN_FX"]:
            domain = row["DOMAIN"]
            company_name = row["COMPANY_NAME"]
            matching_row = fx_df[
                (fx_df["DOMAIN_NAME"] == domain)
                & (
                    fx_df["NAME_AND_ALT_NAMES"].apply(
                        lambda names: company_name in names
                    )
                )
            ]
            if not matching_row.empty:
                df.at[index, "ORG_ID"] = matching_row.iloc[0]["ORG_ID"]

    for index, row in df.iterrows():
        if row["EXISTS_IN_FX"]:
            if not row["ORG_ID"]:
                domain = row["DOMAIN"]
                company_name = row["COMPANY_NAME"]
                matching_row = fx_df[
                    (
                        fx_df["DOMAIN_NAME"].apply(
                            lambda name: fuzz.token_sort_ratio(domain, name) >= 90
                        )
                    )
                    | (
                        fx_df["NAME_AND_ALT_NAMES"].apply(
                            lambda names: company_name.lower() in names.lower()
                        )
                    )
                ]
                if not matching_row.empty:
                    df.at[index, "ORG_ID"] = matching_row.iloc[0]["ORG_ID"]

    for index, row in df.iterrows():
        if row["EXISTS_IN_FX"]:
            if row["ORG_ID"] is None:
                company_name = row["COMPANY_NAME"]
                matching_row = fx_df[
                    (
                        fx_df["NAME_AND_ALT_NAMES"].apply(
                            lambda names: company_name.lower() in names.lower()
                        )
                    )
                ]
                if not matching_row.empty:
                    df.at[index, "ORG_ID"] = matching_row.iloc[0]["ORG_ID"]

    return df
