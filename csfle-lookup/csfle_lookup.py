"""
This is a toy program to experiment with $lookup and client-side-field-level
encryption in MongoDB.

Author: Joel Odom
"""

import argparse
from bson import STANDARD, CodecOptions
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts


#
# These are global constants that you may have to tweak to use this program
# on your own database.
#

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "star_trek"
STARSHIPS_COLLECTION = "starships"
MISSIONS_COLLECTION = "missions"

STARSHIPS_DATA = [
    {"starship_id": 1, "name": "USS Enterprise", "prefix_code": 31415},
    {"starship_id": 2, "name": "USS Voyager", "prefix_code": 27182},
    {"starship_id": 3, "name": "USS Reliant", "prefix_code": 16309}
]

MISSIONS_DATA = [
    {"mission_id": 101, "title": "Explore Alpha Quadrant", "starship_id": 1},
    {"mission_id": 102, "title": "Patrol Neutral Zone", "starship_id": 1},
    {"mission_id": 103, "title": "Diplomatic Mission to Cardassia", "starship_id": 2},
    {"mission_id": 104, "title": "Exact Revenge on Admiral Kirk", "starship_id": 3}
]

KEY_VAULT_DB = DB_NAME  # Reuse the same database
KEY_VAULT_COLL = "__keyVault"
KEY_VAULT_NAMESPACE = f"{KEY_VAULT_DB}.{KEY_VAULT_COLL}"

# 96 random hardcoded key bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

# The ClientEncryption helper object needs a key provider
KMS_PROVIDERS = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}


def create_parser():
    parser = argparse.ArgumentParser(
        description=
        "A toy program to experiment wtih CSFLE and encryption in MongoDB.")
    
    group = parser.add_mutually_exclusive_group()

    group.add_argument("--setup-database",
                        action="store_true",
                        help="initializes the database before a first run")

    group.add_argument("--destroy-database",
                        action="store_true",
                        help="destroys the database")

    group.add_argument("--demonstrate-lookup",
                        action="store_true",
                        help="demonstrate some lookup scenarios")

    parser.add_argument("--encryption",
                        action="store_true",
                        help="use automatic encryption")

    return parser


def connect_to_mongo(with_encyption):
    """
    Connect to the MongoDB cluster.
    
    Returns a MongoClient.
    """

    auto_encryption_opts = None

    if with_encyption:
        # Get the data key id (there should be only one)
        schemaless_client = MongoClient(MONGO_URI)
        db = schemaless_client[KEY_VAULT_DB]
        key_vault_collection = db[KEY_VAULT_COLL]
        key_document = key_vault_collection.find_one()
        if key_document is None:
            raise Exception(
                "Key not found. Did you create the database with encryption?")
        key_id = key_document['_id']  # returned as a binary string

        # Create the schema map that specifies what to automatically encrypt
        SCHEMA_MAP = {
            f"{DB_NAME}.{STARSHIPS_COLLECTION}": {
                "bsonType": "object",
                "properties": {
                    "prefix_code": {
                        "encrypt": {
                            "bsonType": "int",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            "keyId": [key_id]
                        }
                    }
                }
            }
        }

        # Build the automatic encryption options object from the schema map
        auto_encryption_opts=AutoEncryptionOpts(
            KMS_PROVIDERS,
            KEY_VAULT_NAMESPACE,
            schema_map=SCHEMA_MAP
        )

    client = MongoClient(MONGO_URI, auto_encryption_opts=auto_encryption_opts)
    return client


def setup_database(with_encryption):
    """
    Set up the database for first use.
    """

    # Create a temporary schemaless client for use here
    schemaless_client = MongoClient(MONGO_URI)

    # Check if the database already exists
    if DB_NAME in schemaless_client.list_database_names():
        raise Exception(f"Database {DB_NAME} already exists.")
    
    if with_encryption:
        # A ClientEncryption is attached to the database to perform CSFLE
        client_encryption = ClientEncryption(
            KMS_PROVIDERS,
            KEY_VAULT_NAMESPACE,
            schemaless_client,
            CodecOptions(uuid_representation=STANDARD)
        )

        # Create a data key (which is stored in encrypted form in the database)
        # Use a single data key for everything in this example
        data_key_id = client_encryption.create_data_key("local")

    # Create the database, collections and documents
    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]
    collections_data = [
        (STARSHIPS_COLLECTION, STARSHIPS_DATA),
        (MISSIONS_COLLECTION, MISSIONS_DATA)
        ]
    for collection_name, data in collections_data:
        if collection_name in db.list_collection_names():
            raise Exception(
                f"Collection {collection_name} already exists in {DB_NAME} database.")
        db[collection_name].insert_many(data)

    print("Database setup complete with sample data.")


def destroy_database():
    """
    Destroys the MongoDB database.

    The user will need the dbAdmin role on the database to destroy it.
    """

    # Encryption doesn't matter for destruction
    client = connect_to_mongo(with_encyption=False)

    if DB_NAME not in client.list_database_names():
        raise Exception(f"Database '{DB_NAME}' does not exist.")

    client.drop_database(DB_NAME)

    print("Database destruction complete.")


def perform_lookup(with_encryption):
    """
    Performs a simple lookup for demonstration purposes.
    """
 
    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]

    pipeline = [
        {
            "$lookup": {
                "from": MISSIONS_COLLECTION,  # The collection to join
                "localField": "starship_id",  # The field from the starships collection
                "foreignField": "starship_id",  # The field from the missions collection
                "as": "assigned_missions"  # The field that will hold the joined data
            }
        }
    ]

    results = db.starships.aggregate(pipeline)

    # Print the results
    found_one = False
    for result in results:
        found_one = True
        print(f"Missions for {result["name"]}:")
        for mission in result["assigned_missions"]:
            print(f"  {mission["title"]}")

    if not found_one:
        print("No results found. Did you create the database?")
        return


def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.setup_database:
        print("Setting up database before first use...")
        setup_database(args.encryption)
    elif args.destroy_database:
        print("Destroying the database...")
        destroy_database()
    elif args.demonstrate_lookup:
        print("Showing demonstration lookups...")
        perform_lookup(args.encryption)
    else:
        parser.print_help()
    
    print()

if __name__ == "__main__":
    main()
