"""
Another Joel Odom toy program to experiment with QE in MongoDB.
"""

import random
import os

from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import ClientEncryption

from bson.codec_options import CodecOptions
from bson.binary import STANDARD

#
# Set up everything we will need for implicit encryption
#

KMS_PROVIDER_NAME = "local"  # instead of a cloud provider
KEY_VAULT_NAMESPACE = "encryption.__keyVault"

# 96 random hardcoded bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

KMS_PROVIDER_CREDENTIALS = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}

CRYPT_SHARED_LIB = "/Users/joel.odom/mongo_crypt_shared_v1-macos-arm64-enterprise-7.0.6/lib/mongo_crypt_v1.dylib"

auto_encryption_options = AutoEncryptionOpts(  # use automatic encryption
    KMS_PROVIDER_CREDENTIALS,
    KEY_VAULT_NAMESPACE,
    crypt_shared_lib_path=CRYPT_SHARED_LIB
)

#auto_encryption_options = None  # Remove this to demonstrate the $lookup failure

#
# Connect to the Mongo database
#

PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"
DB_NAME = "lookup-experiment"

mongo_client = MongoClient(URI, auto_encryption_opts=auto_encryption_options)
db = mongo_client[DB_NAME] # This alone is not enough to create a database

#
# Here are some utilities for creating nonsense data
#

def generate_nonsense_word():
    VOWELS = "aeiou"
    CONSONANTS = "bcdfghjklmnpqrstvwxyz"
    WORD_LENGTH = random.randint(4, 8)
    word = ""
    for i in range(WORD_LENGTH):
        if i % 2 == 0:
            # Even indices get a consonant
            word += random.choice(CONSONANTS)
        else:
            # Odd indices get a vowel
            word += random.choice(VOWELS)
    return word

def generate_nonsense_words(count):
    return ' '.join([generate_nonsense_word() for x in range(count)])

#
# Create some items and drop them in rooms
#
# Items go in a colletion called items and have a name and a description.
# Rooms are in a collection called rooms. Each room contains an array of items.
#

def create_some_items(count):
    global db
    created_items_names = []
    created_items_dicts = []
    for x in range(count):
        item_name = generate_nonsense_words(1)
        created_items_names.append(item_name)
        item_to_create = {
            "name": item_name,
            "description": generate_nonsense_words(20)
        }
        created_items_dicts.append(item_to_create)
    # Insert the item into the items collection
    # (creates db and collection, if they don't exist)
    db.items.insert_many(created_items_dicts)
    return created_items_names

def create_a_room(items_to_put_in_room, is_secret = False):
    global db
    room_name = f"Room {generate_nonsense_word()}"
    room_description = generate_nonsense_words(30)
    room = {
        "name": room_name,
        "description": room_description,
        "contents": items_to_put_in_room,
        "fear_factor": random.randint(0, 99)
    }
    if is_secret:
        db.secret_rooms.insert_one(room)
    else:
        db.rooms.insert_one(room)
    return room_name

NUMBER_OF_ROOMS_TO_CREATE = 10
NUMBER_OF_ITEMS_TO_PUT_IN_ROOM = 10

# for x in range(NUMBER_OF_ROOMS_TO_CREATE):
#     created_items = create_some_items(NUMBER_OF_ITEMS_TO_PUT_IN_ROOM)
#     created_room = create_a_room(created_items)
#     print(f"Created a room called {created_room} with {NUMBER_OF_ITEMS_TO_PUT_IN_ROOM} items in it. ({x + 1}/{NUMBER_OF_ROOMS_TO_CREATE})")

#
# Check for duplicate item names using an aggregation pipeline. It would be smarter
# to create an index that enforces uniqueness, but I want to experiment with aggregation.
# This pipeline groups items by name (used for _id) and applies the $sum operator
# to each group. The next pipeline stage is a match stage that only passes groups with
# a count greater than one.
#

# COUNT_PIPELINE = [
#     {
#         "$group": {
#             "_id": "$name",
#             "count": { "$sum": 1 }
#         }
#     },
#     {
#         "$match": {
#             "count": { "$gt": 1 }
#         }
#     }
# ]

# duplicates = db.items.aggregate(COUNT_PIPELINE)
# for item in duplicates:
#     print(f"*** Duplicate item: {item["_id"]}")
#     assert(False) # resolve duplicate item

#
# Use an aggregation pipeline to lookup each item in a room as an object, by its name.
#

if auto_encryption_options is None:  # because $lookup doesn't work with encryption
    LOOKUP_PIPELINE = [
        {
            "$lookup": {
                "from": "items",
                "localField": "contents",
                "foreignField": "name",
                "as": "room_contents"  # this will be an array of item objects
            }
        },
        {
            "$project": {
                "contents": 0  # don't need the contents twice
            }
        }
    ]

    rooms_with_contents = db.rooms.aggregate(LOOKUP_PIPELINE)

    for room in rooms_with_contents:
        #print(f"{room["name"]} Contains:")
        for item in room["room_contents"]:
            pass
            #print(f"  a {item["name"]} ({item["description"]})")

#
# Stick a random item in a random room.
#

SAMPLE_PIPELINE = [
    {
        "$sample": { "size": 1 }  # sample is pseudo random
    }
]

random_item = db.items.aggregate(SAMPLE_PIPELINE).next()
#print(f"{random_item["name"]}")  # yes, it's different every time

random_room = db.rooms.aggregate([
    {
        "$sample": { "size": 1 }
    }
]).next()

db.rooms.update_one(
    {
        "_id": random_room["_id"]
    },
    {
        "$push": { "contents": random_item["name"]}
    }
)

print(f"Added a {random_item["name"]} to {random_room["name"]}.")

#
# Now perform an array query to find rooms with the random item type in it.
#

rooms = db.rooms.find(
    {
        "contents": random_item["name"]
    }
)

for room in rooms:
    print (f"{room["name"]} contains a {random_item["name"]}.")

#
# If necessary, create the encrypted collection for secret rooms.
#
# HERE IS WHERE WE CAN DEMONSTRATE THE ARRAY LIMITATION. If you blow away
# the secret_rooms collection, the code below will try to recreate it. If
# you make the contents array queryable in ENCRYPTED_FIELDS_MAP, you'll get the
# exception.
#

ENCRYPTED_FIELDS_MAP = {  # these are the fields to encrypt automagically
    "fields": [
        {
            "path": "name",
            "bsonType": "string",
            "queries": [ {"queryType": "equality"} ]  # queryable
        },
        # {
        #     "path": "contents",
        #     "bsonType": "array",
        #     #"queries": [ {"queryType": "equality"} ]  # queryable
        # }
    ]
}

ENCRYPTED_ROOMS_COLLECTION = "secret_rooms"

if ENCRYPTED_ROOMS_COLLECTION not in db.list_collection_names():
    # create the (partially) encrypted collection on the first run
    client_encryption = ClientEncryption(  # a kind of helper
        kms_providers=KMS_PROVIDER_CREDENTIALS,
        key_vault_namespace=KEY_VAULT_NAMESPACE,
        key_vault_client=mongo_client,
        codec_options=CodecOptions(uuid_representation=STANDARD)
    )
    CMK_CREDENTIALS = {}  # no creds because using a local key CMK
    client_encryption.create_encrypted_collection(
        mongo_client[DB_NAME],
        ENCRYPTED_ROOMS_COLLECTION,
        ENCRYPTED_FIELDS_MAP,
        KMS_PROVIDER_NAME,
        CMK_CREDENTIALS,
    )

#
# Create a secret room with some stuff in it
#

for x in range(NUMBER_OF_ROOMS_TO_CREATE):
    created_items = create_some_items(NUMBER_OF_ITEMS_TO_PUT_IN_ROOM)
    created_room = create_a_room(created_items, is_secret=True)
    print(f"Created a secret room called {created_room} with {NUMBER_OF_ITEMS_TO_PUT_IN_ROOM} items in it. ({x + 1}/{NUMBER_OF_ROOMS_TO_CREATE})")

#
# Stick a random item in a random secret room
#

SAMPLE_PIPELINE = [
    {
        "$sample": { "size": 1 }  # sample is pseudo random
    }
]

random_item = db.items.aggregate(SAMPLE_PIPELINE).next()
#print(f"{random_item["name"]}")  # yes, it's different every time

random_room = db.secret_rooms.aggregate([
    {
        "$sample": { "size": 1 }
    }
]).next()

db.secret_rooms.update_one(
    {
        "_id": random_room["_id"]
    },
    {
        "$push": { "contents": random_item["name"]}
    }
)

print(f"Added a {random_item["name"]} to secret room {random_room["name"]}.")

#
# Now perform an array query to find rooms with the random item type in it.
#
# TODO: UNDER CONSTRUCTION / UNTESTED -- Won't work until array is fixed?
#

# rooms = db.rooms.find(
#     {
#         "contents": random_item["name"]
#     }
# )

# for room in rooms:
#     print (f"{room["name"]} contains a {random_item["name"]}.")

#
# Query for a room using Queryable Encryption
#

found_room = db.secret_rooms.find(
    {"name": random_room["name"]}
).next()

assert(found_room["name"] == random_room["name"])

print(f"Contents of {found_room["name"]}:")
for item in found_room["contents"]:
    print(f"  {item}")

print()

