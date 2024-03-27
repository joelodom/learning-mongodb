"""
Another Joel Odom toy program to experiment with QE in MongoDB.

REMEMBER TO EXPERIMENT WITH IMPLICIT VS EXPLICIT ENCRYPTION
"""

import random
import os
from pymongo import MongoClient

PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")

#uri = os.environ['MONGODB_URI']  # Your connection URI
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"
DB_NAME = "lookup-experiment"
MONGO_CLIENT = MongoClient(URI)
DB = MONGO_CLIENT[DB_NAME] # This alone is not enough to create a database

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
        DB.items.insert_one(ITEM_TO_CREATE) # TODO: Use insert many instead!
        created_items.append(ITEM_NAME)
    return created_items

created_items = create_some_items(10)

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

created_room = create_a_room(created_items)
