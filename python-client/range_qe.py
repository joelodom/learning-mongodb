"""
Experimenting with QE range queries.
"""

import os
from pprint import pprint
import random

from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts

#
# AutoEncryptionOpts helper class setup
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

#
# Connect to the database
#

PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"
DB_NAME = "range_query_testing"

mongo_client = MongoClient(URI, auto_encryption_opts=auto_encryption_options)
db = mongo_client[DB_NAME]

#
# Add some data to the database
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
    return ' '.join([generate_nonsense_word() for i in range(count)])

def create_some_items(count):
    global db
    created_items_dicts = []
    for i in range(count):
        item_name = generate_nonsense_word()
        item_to_create = {
            "name": item_name,
            "description": generate_nonsense_words(20)
        }
        created_items_dicts.append(item_to_create)
    # (creates db and collection, if they don't exist)
    db.items.insert_many(created_items_dicts)

create_some_items(5)

#
# Print stuff
#

results = db.items.find()

print("All items in database:")
for result in results:
    pprint(result)
