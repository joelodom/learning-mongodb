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

def generate_nonsense_words(l):
    return ' '.join([generate_nonsense_word() for x in range(l)])

ITEM_TO_CREATE = {
    "name": generate_nonsense_words(1),
    "description": generate_nonsense_words(20)
}

DB.items.insert_one(ITEM_TO_CREATE)
