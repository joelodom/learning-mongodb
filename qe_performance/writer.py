"""
This is a toy program to experiment with MongoDB Queryable Encryption
performance. I don't claim it to be super scientific.

Author: Joel Odom

TODO:

  * Upgrade to the lastest driver and shared library.
  * Inspect to make sure that the inserted items are actually encrypted as expected.
  * Test against Atlas since this has no network latency between any parts of the system.
"""

import time
from bson import STANDARD, CodecOptions
from pymongo.encryption import ClientEncryption
from utils import create_client, DB_NAME, KMS_PROVIDER_CREDENTIALS, KEY_VAULT_NAMESPACE, KMS_PROVIDER_NAME, KEY_VAULT_DATABASE, ENCRYPTED_COLLECTION, write_line_to_csv

print("Welcome to the QE performance experiment writer.")

mongo_client = create_client()

#
# Make sure we're starting with a clean database
#

def does_collection_exist(db_name, collection_name):
    global mongo_client
    collection_names = mongo_client[db_name].list_collection_names()
    return collection_name in collection_names

assert(not does_collection_exist(DB_NAME, ENCRYPTED_COLLECTION))

#
# Create the encrypted collection and key vault
#

ENCRYPTED_FIELDS_MAP = {  # these are the fields to encrypt automagically
    "fields": [
        {
            "path": "encrypted_string",
            "bsonType": "string",
            "queries":
            [ {
                "queryType": "equality",
            } ]  # equality queryable
        }
    ]
}

client_encryption = ClientEncryption(  # a kind of helper
    kms_providers=KMS_PROVIDER_CREDENTIALS,
    key_vault_namespace=KEY_VAULT_NAMESPACE,
    key_vault_client=mongo_client,
    codec_options=CodecOptions(uuid_representation=STANDARD)
)

CMK_CREDENTIALS = {}  # no creds because using a local key CMK
client_encryption.create_encrypted_collection(
    mongo_client[DB_NAME],
    ENCRYPTED_COLLECTION,
    ENCRYPTED_FIELDS_MAP,
    KMS_PROVIDER_NAME,
    CMK_CREDENTIALS,
)

assert(does_collection_exist(DB_NAME, ENCRYPTED_COLLECTION))

#
# Insert a bunch of random data including an encrypted string
#

ITEMS_TO_CREATE = 200
ITERATIONS = 1000

overall_start_time = time.time()

for x in range(ITERATIONS):
    print(f"Creating {ITEMS_TO_CREATE} random items... Iteration {x + 1} of {ITERATIONS}...")

    created_items_dicts = []

    for i in range(ITEMS_TO_CREATE):
        item_name = f"Item {i}"
        item_to_create = {
            "name": item_name,
            "description": f"This is item{i}.",
            "encrypted_string": f"{i}"
        }
        created_items_dicts.append(item_to_create)

    start_time = time.time()
    mongo_client[DB_NAME].get_collection(ENCRYPTED_COLLECTION).insert_many(created_items_dicts)
    end_time = time.time()

    elapsed = 100*(end_time - start_time)
    print(f"Items created. Elapsed time is {elapsed} ms.")

    PERF_FILE = "writer_output.csv"
    # iteration number, items created, elapsed time (ms), ms per item
    write_line_to_csv(PERF_FILE, [x + 1, ITEMS_TO_CREATE, elapsed, elapsed/ITEMS_TO_CREATE])  # save the perf data

overall_time = 100*(time.time() - overall_start_time)

print(f"It took {overall_time} ms, which is about {overall_time / ITEMS_TO_CREATE / ITERATIONS} ms / record.")

#
# Clean up
#

mongo_client.drop_database(DB_NAME)
mongo_client.drop_database(KEY_VAULT_DATABASE)

assert(not does_collection_exist(DB_NAME, ENCRYPTED_COLLECTION))

mongo_client.close()
