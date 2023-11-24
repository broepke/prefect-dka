"""
Custom Prefect Block for Storing MongoDB Credentials
See: https://docs.prefect.io/latest/concepts/blocks/#creating-new-block-types

To deploy to Prefect Cloud
prefect block register --file util_mongodb_block.py
"""
from typing import Optional
from pymongo import MongoClient
from prefect.blocks.core import Block
from pydantic.v1 import SecretStr
from urllib.parse import quote_plus


class MongoDBCredentials(Block):
    """Prefect block to store and retreive credentials

    Args:
        Block (Prefect Block): Storage block for credentials
    """

    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    port: Optional[int] = 27017
    db: Optional[str] = None
    authSource: Optional[str] = "admin"

    def get_client(self):
        """Create mongo client connection

        Returns:
            MongoClient: PyMongo Client
        """

        host = self.host
        username = quote_plus(self.username)
        password = quote_plus(self.password.get_secret_value())

        uri = f"mongodb+srv://{username}:{password}@{host}"
        client = MongoClient(uri)

        return client
