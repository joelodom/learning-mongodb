"""
Another Joel Odom toy program to experiment with QE in MongoDB.
"""

import random
import os
from pymongo import MongoClient

#
# Connect to the Mongo database
#

PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"
MONGO_CLIENT = MongoClient(URI)
DB_NAME = "lookup-experiment"
DB = MONGO_CLIENT[DB_NAME] # This alone is not enough to create a database

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
    global DB
    created_items = []
    for x in range(count):
        ITEM_NAME = generate_nonsense_words(1)
        ITEM_TO_CREATE = {
            "name": ITEM_NAME,
            "description": generate_nonsense_words(20)
        }
        # Insert the item into the items collection
        # (creates db and collection, if they don't exist)
        DB.items.insert_one(ITEM_TO_CREATE) # TODO: Use insert_many instead!
        created_items.append(ITEM_NAME)
    return created_items

def create_a_room(items_to_put_in_room):
    global DB
    ROOM_NAME = f"Room {generate_nonsense_word()}"
    ROOM_DESCRIPTION = generate_nonsense_words(30)
    ROOM = {
        "name": ROOM_NAME,
        "description": ROOM_DESCRIPTION,
        "items": items_to_put_in_room
    }
    DB.rooms.insert_one(ROOM)
    return ROOM_NAME

NUMBER_OF_ITEMS_TO_PUT_IN_ROOM = 10
created_items = create_some_items(NUMBER_OF_ITEMS_TO_PUT_IN_ROOM)
created_room = create_a_room(created_items)

print(f"Created a room called {created_room} with {NUMBER_OF_ITEMS_TO_PUT_IN_ROOM} items in it.")

#
# Check for duplicate item names using an aggregation pipeline. It would be smarter
# to create an index that enforces uniqueness, but I want to experiment with aggregation.
# This pipeline groups items by name (used for _id) and applies the $sum operator
# to each group. The next pipeline stage is a match stage that only passes groups with
# a count greater than one.
#

PIPELINE = [
    {
        "$group": {
            "_id": "$name",
            "count": { "$sum": 1 }
        }
    },
    {
        "$match": {
            "count": { "$gt": 1 }
        }
    }
]

duplicates = DB.items.aggregate(PIPELINE)
for item in duplicates:
    print(f"Duplicate item: {item["_id"]}")
    assert(False) # resolve duplicate item

