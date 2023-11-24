"""
Utilities for working with our main MongoDB
"""
from prefect import task, flow, get_run_logger
from utilities.util_mongodb_block import MongoDBCredentials


def connect_to_mongo():
    """
    Utilizing a custom Block in prefect to authenticate to Mongo
    """

    mongodbcredentials_block = MongoDBCredentials.load("mongodb-creds-block")

    client = mongodbcredentials_block.get_client()

    return client


def mint_new_org():
    return
