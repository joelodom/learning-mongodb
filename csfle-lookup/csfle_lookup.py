"""
This is a toy program to experiment with $lookup and client-side-field-level
encryption in MongoDB.

Author: Joel Odom
"""

import argparse
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
import os

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
    {"starship_id": 1, "name": "USS Enterprise", "prefix_id": 31415},
    {"starship_id": 2, "name": "USS Voyager", "prefix_id": 27182},
    {"starship_id": 3, "name": "USS Reliant", "prefix_id": 16309}
]

MISSIONS_DATA = [
    {"mission_id": 101, "title": "Explore Alpha Quadrant", "starship_id": 1},
    {"mission_id": 102, "title": "Patrol Neutral Zone", "starship_id": 1},
    {"mission_id": 103, "title": "Diplomatic Mission to Cardassia", "starship_id": 2},
    {"mission_id": 104, "title": "Exact Revenge on Admiral Kirk", "starship_id": 3}
]


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

    return parser


def connect_to_mongo():
    """
    Connect to the MongoDB cluster.
    
    Returns a MongoClient.
    """

    client = MongoClient(MONGO_URI)
    return client


def setup_database():
    """
    Set up the database for first use.
    """

    client = connect_to_mongo()

    # Check if the database already exists
    if DB_NAME in client.list_database_names():
        raise Exception(f"Database {DB_NAME} already exists.")

    # Create the database
    db = client[DB_NAME]

    collections_data = [
        (STARSHIPS_COLLECTION, STARSHIPS_DATA),
        (MISSIONS_COLLECTION, MISSIONS_DATA)
        ]

    # Check and create collections
    for collection_name, data in collections_data:
        if collection_name in db.list_collection_names():
            raise Exception(
                f"Collection {collection_name} already exists in {DB_NAME} database.")
        db[collection_name].insert_many(data)


def destroy_database():
    """
    Destroys the MongoDB database.

    The user will need the dbAdmin role on the database to destroy it.
    """

    client = connect_to_mongo()

    if DB_NAME not in client.list_database_names():
        raise Exception(f"Database '{DB_NAME}' does not exist.")

    client.drop_database(DB_NAME)


def perform_lookup():
    """
    Performs a simple lookup for demonstration purposes.
    """
 
    client = connect_to_mongo()
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
        setup_database()
        print("Database setup complete with sample data.")
    elif args.destroy_database:
        print("Destroying the database...")
        destroy_database()
        print("Database destruction complete.")
    elif args.demonstrate_lookup:
        print("Showing demonstration lookups...")
        perform_lookup()
    else:
        parser.print_help()
    
    print()

if __name__ == "__main__":
    main()
