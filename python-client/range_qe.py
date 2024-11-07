"""
This is a toy program to experiment with MongoDB Queryable Encryption Range
Queries, to be released in Server 8.0 as GA.

Author: Joel Odom
"""

import os
from pprint import pprint
import random
import time
from bson import STANDARD, CodecOptions, Decimal128, Int64
from pymongo.encryption import ClientEncryption
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
import traceback
import readline # allows input to use up and down arrows
from decimal import Decimal, Context, Inexact


print("Welcome to the QE Range sandbox. Type 'help' for a list of commands. Type 'exit' to quit.")



#
# AutoEncryptionOpts is a helper class that we pass to the MongoClient. It
# provides the KMS credentials and the namespace (database and collection)
# for the keys. Note that all of the keys in the key vault are encrypted
# on the client side, so the server can't access the keys or the data encrypted
# by them.
#

print("Setting up the AutoEncryptionOpts helper class...")

KMS_PROVIDER_NAME = "local"  # instead of a KMS
KEY_VAULT_DATABASE = "encryption"
KEY_VAULT_COLLECTION = "__keyVault"
KEY_VAULT_NAMESPACE = f"{KEY_VAULT_DATABASE}.{KEY_VAULT_COLLECTION}"

# We use 96 random hardcoded bytes (base64-encoded), because this is only an
# example. Production implementations of QE should use a Key Management System
# or be very thoughtful about how the secret key is secured and injected into
# the client at runtime.
LOCAL_MASTER_KEY = "V2hlbiB0aGUgY2F0J3MgYXdheSwgdGhlIG1pY2Ugd2lsbCBwbGF5LCBidXQgd2hlbiB0aGUgZG9nIGlzIGFyb3VuZCwgdGhlIGNhdCBiZWNvbWVzIGEgbmluamEuLi4u"

KMS_PROVIDER_CREDENTIALS = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}

# ref https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library/
CRYPT_SHARED_LIB = "/Users/joel.odom/lib/mongo_crypt_v1.dylib"

auto_encryption_options = AutoEncryptionOpts(
    KMS_PROVIDER_CREDENTIALS,
    KEY_VAULT_NAMESPACE,
    crypt_shared_lib_path=CRYPT_SHARED_LIB
)



#
# Connect to MongoDB using the usual MongoClient paradigms.
#

print("Creating the MongoClient using the connection string in the source code...")

URI = "mongodb://127.0.0.1:27017/"

USE_ATLAS = False
if USE_ATLAS:
    PASSWORD=os.getenv("JOEL_ATLAS_PWD")
    if PASSWORD is None:
        raise Exception("Password not set in environment.")
    URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "range_query_testing"

mongo_client = MongoClient(URI, auto_encryption_opts=auto_encryption_options)



#
# Here is where we define the experimental schema. This is pushed to the server.
# It's also possible to use a client-side schema for enforcement on the client
# side.
#

SECRET_INT_MIN = 1
SECRET_INT_MAX = 1000

SECRET_LONG_MIN = 474836472147483600
SECRET_LONG_MAX = 474836472147483700

SECRET_DECIMAL_MIN = 3.00000000
SECRET_DECIMAL_MAX = 3.14159265

ENCRYPTED_FIELDS_MAP = {  # these are the fields to encrypt automagically
    "fields": [
        {
            "path": "secret_int",
            "bsonType": "int",
            "queries":
            [
                {
                    "queryType": "range",
                    "min": SECRET_INT_MIN,
                    "max": SECRET_INT_MAX
                },
                # {
                #     "queryType": "equality",
                # }
            ]
        },
        {
            "path": "secret_long",
            "bsonType": "long",
            "queries":
            [ {
                "queryType": "range",
            } ]  # range queryable
        },
        {
            "path": "secret_decimal",
            "bsonType": "decimal",  # decimal128
            "queries":
            [ {
                "queryType": "range",
                "min": Decimal128(str(SECRET_DECIMAL_MIN)),
                "max": Decimal128(str(SECRET_DECIMAL_MAX)),
                "precision": 10
            } ]  # range queryable
        },
        # {
        #     "path": "arrayField.arrayObjectField1",
        #     "bsonType": "string",
        #     "queries": { "queryType": "equality" },
        # },
    ]
}

ENCRYPTED_ITEMS_COLLECTION = "items"



def create_encrypted_collection():
    if does_collection_exist(DB_NAME, ENCRYPTED_ITEMS_COLLECTION):
        print("It appears that the encrypted collection already exists. Doing nothing.")
        return
    client_encryption = ClientEncryption(  # a kind of helper
        kms_providers=KMS_PROVIDER_CREDENTIALS,
        key_vault_namespace=KEY_VAULT_NAMESPACE,
        key_vault_client=mongo_client,
        codec_options=CodecOptions(uuid_representation=STANDARD)
    )
    CMK_CREDENTIALS = {}  # no creds because using a local key CMK
    client_encryption.create_encrypted_collection(
        mongo_client[DB_NAME],
        ENCRYPTED_ITEMS_COLLECTION,
        ENCRYPTED_FIELDS_MAP,
        KMS_PROVIDER_NAME,
        CMK_CREDENTIALS,
    )
    if does_collection_exist(DB_NAME, ENCRYPTED_ITEMS_COLLECTION):
        print("Encrypted collection created.")
    else:
        print("It appears that something went wrong...")



def does_database_exist(db_name):
    global mongo_client
    database_names = mongo_client.list_database_names()
    return db_name in database_names


def does_collection_exist(db_name, collection_name):
    global mongo_client
    collection_names = mongo_client[db_name].list_collection_names()
    return collection_name in collection_names


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


COLLECTION_DOESNT_EXIST_MESSAGE = """
So. Here's the thing. You're asking me to create items, but it appears that the
encrypted collection doesn't exist yet. That's not so great. What will happen
is that the secret stuff will be inserted without encryption, so I'm not going
to do this. You should create the encrypted collection with server-side schema
enforcement and the right permissions so this doesn't happen.
"""


def create_some_items():
    global mongo_client

    if not does_collection_exist(DB_NAME, ENCRYPTED_ITEMS_COLLECTION):
        print(COLLECTION_DOESNT_EXIST_MESSAGE)
        return
    ITEMS_TO_CREATE = 200
    print(f"Creating {ITEMS_TO_CREATE} random items...")
    created_items_dicts = []
    for i in range(ITEMS_TO_CREATE):
        item_name = generate_nonsense_word()
        item_to_create = {
            "name": item_name,
            "description": generate_nonsense_words(20),
            "secret_int": random.randint(SECRET_INT_MIN, SECRET_INT_MAX),
            "secret_long": random.randint(SECRET_LONG_MIN, SECRET_LONG_MAX),
            "secret_decimal": Decimal128(str(random.uniform(
                SECRET_DECIMAL_MIN, SECRET_DECIMAL_MAX))),  # not really right
            "arrayField": [ { "arrayObjectField1": "foo" } ]
        }
        created_items_dicts.append(item_to_create)
    mongo_client[DB_NAME].get_collection(ENCRYPTED_ITEMS_COLLECTION).insert_many(created_items_dicts)
    print("Items created.")





def help():
    print("Available commands:")
    print("  status                      -  show general status about the experiment")
    print("  create-encrypted-collection -  creates a collection with automatic encryption")
    print("  create-some-items           -  creates a handful of random items")
    print("  test-query                  -  test querying the database")    
    print("  destroy-databases           -  destroys the databasees (including the key vault)")
    print("  exit / quit                 -  exits the program")
    print()


def status():
    print(f"DB_NAME is {DB_NAME}.")
    print(f"{DB_NAME} {'is' if does_database_exist(DB_NAME) else 'is not'} created.")
    print(f"ENCRYPTED_ITEMS_COLLECTION is {ENCRYPTED_ITEMS_COLLECTION}.")
    exists = does_collection_exist(DB_NAME, ENCRYPTED_ITEMS_COLLECTION)
    print(f"{ENCRYPTED_ITEMS_COLLECTION} {'is' if exists else 'is not'} created.")


def destroy_databases():
    confirm = input("If you really mean it, say please: ")
    if confirm == "please":
        print("Destroying databases...")
        mongo_client.drop_database(DB_NAME)
        mongo_client.drop_database(KEY_VAULT_DATABASE)
    else:
        print("You didn't say please. Databases not destroyed.")


QUERY = {
    "secret_int": {
        "$gt": (SECRET_INT_MAX + SECRET_INT_MIN)//2
    },
    "secret_long": {
        "$gte": Int64((SECRET_LONG_MAX + SECRET_LONG_MIN)//2)
    },
    "secret_decimal": {
        "$lt": Decimal128(str((SECRET_DECIMAL_MAX + SECRET_DECIMAL_MIN)/2))
    }
}

def test_query():
    global mongo_client
    PROJECTION = { "__safeContent__": 0, "_id": 0 }
    start_time = time.time()
    results = mongo_client[DB_NAME][ENCRYPTED_ITEMS_COLLECTION].find(QUERY, PROJECTION)
    print(f"find took {1000*(time.time() - start_time):.1f} ms")
    count = 0
    one_result = None
    for result in results:
        count += 1
        one_result = result
    print(f"Iterated over {count} results.")
    print()
    if one_result is not None:
        print("Here is one example:")
        print()
        pprint(one_result)


while True:  # not with a bang, but with a loop
    try:
        user_input = input(">>> ").strip()
        
        if not user_input:
            continue
        
        command, *args = user_input.split()
        command = command.lower()

        start_time = time.time()

        if command in ["exit", "quit"]:
            print("Goodbye!")
            print()
            exit(0)
        elif command == "help":
            help()
        elif command == "status":
            status()
        elif command == "create-encrypted-collection":
            create_encrypted_collection()
        elif command == "create-some-items":
            create_some_items()
        elif command == "test-query":
            test_query()
        elif command == "destroy-databases":
            destroy_databases()
        else:
            print(f"Unknown command: {command}. Type 'help' for a list of commands.")

        elapsed_time = 1000*(time.time() - start_time)

        print()
        print(f"Elapsed time: {elapsed_time:.1f} ms")
        print()

    except Exception as e:
        traceback.print_exc()

