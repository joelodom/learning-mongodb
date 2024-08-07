"""
This is a toy program to experiment with $lookup and client-side-field-level
encryption in MongoDB.

Author: Joel Odom

Here are some steps to demonstrate how CSFLE works (and how lookup should work):

  1. Create a database without encryption (see --help)
  2. You can list all the prefix codes
  3. You can perform a lookup on the missions
  4. Destroy the database
  5. Create the database with encryption
  6. You can list all the prefix codes
  7. If you try to list the codes without specifying encryption you see gibberish
  8. You can't do a lookup (--encryption), even though the field isn't even used for the lookup
  9. If you turn off encryption (omit --encryption), you can do the lookup because the field isn't used for the lookup

I also added the ability to lookup starships associated with a mission, which
is the reverse direction (the collection with encryption is the foreign one) in
the lookup. Observe a similar behavior: you can omit the --encryption flag and
it works (because the prefix code is not used), but when you add the encryption
flag it won't work because a collection with automatic encryption tied to it
can't be used in a lookup.

Also notice that you can list episodes and writers, which fails when using an
encrypting client (that is, turning on --encryption) even though neither of the
collections has encrypted fields. This is being tracked as a bug.
"""

import argparse
from bson import STANDARD, CodecOptions
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts


#
# Constants, including the database connection strings and the sample data.
#

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "star_trek"
STARSHIPS_COLLECTION = "starships"
MISSIONS_COLLECTION = "missions"
WRITERS_COLLECTION = "writers"
EPISODES_COLLECTION = "episodes"

RELIANT_PREFIX_CODE = 16309

STARSHIPS_DATA = [
    {"starship_id": 1, "name": "USS Enterprise", "prefix_code": 31415},
    {"starship_id": 2, "name": "USS Voyager", "prefix_code": 27182},
    {"starship_id": 3, "name": "USS Reliant", "prefix_code": RELIANT_PREFIX_CODE}
]

MISSIONS_DATA = [
    {"mission_id": 101, "title": "Explore Alpha Quadrant", "starship_id": 1},
    {"mission_id": 102, "title": "Patrol Neutral Zone", "starship_id": 1},
    {"mission_id": 103, "title": "Diplomatic Mission to Cardassia", "starship_id": 2},
    {"mission_id": 104, "title": "Exact Revenge on Admiral Kirk", "starship_id": 3}
]

WRITERS_DATA = [  # nothing encrypted
    {"episode_number": 28, "writer": "Harlan Ellison"},
    {"episode_number": 44, "writer": "David Gerrold"}
]

EPISODES_DATA = [  # nothing encrypted
    {"episode_number": 28, "title": "The City on the Edge of Forever"},
    {"episode_number": 44, "title": "The Trouble with Tribbles"}
]

KEY_VAULT_DB = DB_NAME  # Reuse the same database for the key vault
KEY_VAULT_COLL = "__keyVault"
KEY_VAULT_NAMESPACE = f"{KEY_VAULT_DB}.{KEY_VAULT_COLL}"

# We use 96 random hardcoded bytes (base64-encoded), because this is only an
# example. Production implementations of QE should use a Key Management System
# or be very thoughtful about how the secret key is secured and injected into
# the client at runtime.
LOCAL_MASTER_KEY = "V2hlbiB0aGUgY2F0J3MgYXdheSwgdGhlIG1pY2Ugd2lsbCBwbGF5LCBidXQgd2hlbiB0aGUgZG9nIGlzIGFyb3VuZCwgdGhlIGNhdCBiZWNvbWVzIGEgbmluamEuLi4u"

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
    
    group.add_argument("--list-missions",
                       action="store_true",
                       help="list all missions")
    
    group.add_argument("--list-prefix-codes",
                       action="store_true",
                       help="list all starships' prefix codes")

    group.add_argument("--list-episodes",
                       action="store_true",
                       help="list all episodes and their writers")

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
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
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

        # Create a data key (which is stored in encrypted form in the database).
        # Use a single data key for everything in this example. Though the
        # returned value isn't used, this step creates the data key in the
        # database.
        data_key_id = client_encryption.create_data_key("local")

    # Create the database, collections and documents
    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]
    collections_data = [
        (STARSHIPS_COLLECTION, STARSHIPS_DATA),
        (MISSIONS_COLLECTION, MISSIONS_DATA),
        (WRITERS_COLLECTION, WRITERS_DATA),
        (EPISODES_COLLECTION, EPISODES_DATA)
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


def perform_mission_lookup(with_encryption):
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

def perform_starship_lookup(with_encryption):
    """
    This is the reverse of the mission lookup.

    The encrypted prefix code is in the foreign collection, not the local
    collection.
    """
    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]

    pipeline = [
        {
            "$lookup": {
                "from": STARSHIPS_COLLECTION,
                "localField": "starship_id",
                "foreignField": "starship_id",
                "as": "starships"
            }
        }
    ]

    results = db.missions.aggregate(pipeline)

    # Print the results
    found_one = False
    for result in results:
        found_one = True
        # The [0] is because the starships field is actually a list of results
        # matching the join, and I don't feel like enforcing uniqueness.
        print(f"{result["starships"][0]["name"]}: {result["title"]}")

    if not found_one:
        print("No results found. Did you create the database?")
        return


def show_prefix_codes(with_encryption):
    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]
    results = db[STARSHIPS_COLLECTION].find()
    found_one = False
    for result in results:
        found_one = True
        print(f"  {result["name"]}: {result["prefix_code"]}")
    if not found_one:
        print("No results found. Did you create the database?")
        return


def list_episodes(with_encryption):
    """
    Perform a lookup on two unencrypted collections using a client that has encryption.
    """

    client = connect_to_mongo(with_encryption)
    db = client[DB_NAME]

    pipeline = [
        {
            "$lookup": {
                "from": WRITERS_COLLECTION,
                "localField": "episode_number",
                "foreignField": "episode_number",
                "as": "episodes"
            }
        }
    ]

    results = db.episodes.aggregate(pipeline)

    # Print the results
    found_one = False
    for result in results:
        found_one = True
        print(f"{result["title"]} ({result["episodes"][0]["writer"]})")

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
        perform_mission_lookup(args.encryption)
    elif args.list_missions:
        print("Listing missions with starship names...")
        perform_starship_lookup(args.encryption)
    elif args.list_prefix_codes:
        print("Showing all starship prefix codes...")
        show_prefix_codes(args.encryption)
    elif args.list_episodes:
        print("Listing episodes...")
        list_episodes(args.encryption)
    else:
        parser.print_help()
    
    print()

if __name__ == "__main__":
    main()
