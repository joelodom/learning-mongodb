"""
This is a toy program to experiment with MongoDB Queryable Encryption
performance. I don't claim it to be super scientific. See the performance
listener program, which is a monitor for this program to collect more
real-world-like data.

Author: Joel Odom

TODO:

  * Upgrade to the lastest driver and shared library.
  * Inspect to make sure that the inserted items are actually encrypted as expected.
"""

import socket
import time
from bson import STANDARD, CodecOptions
from pymongo.encryption import ClientEncryption
from qe_performance_create_connection import create_client, DB_NAME, KMS_PROVIDER_CREDENTIALS, KEY_VAULT_NAMESPACE, KMS_PROVIDER_NAME, KEY_VAULT_DATABASE, ENCRYPTED_COLLECTION

print("Welcome to the QE performance experiment.")


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
# Interlude: a signaling system so another client can be the database reader
#

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def send_signal(signal_str, host="localhost", port=3141):
    global sock
    sock.sendto(signal_str.encode("utf-8"), (host, port))


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
ITERATIONS = 10

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
    send_signal("start-insert")
    mongo_client[DB_NAME].get_collection(ENCRYPTED_COLLECTION).insert_many(created_items_dicts)
    send_signal("end-insert")
    end_time = time.time()

    print(f"Items created. Elapsed time is {end_time - start_time} ms.")

#
# RESULT: About 1.1 ms per iteration with no noticable increase in time as the
# collection grows. HOWEVER, if I change the string to be unencrypted, we're
# talking 0.005 ms... I wonder if there is client-side caching or threading
# happening there.
#


#
# Check the size of the collection on disk
#








#
# Perform a query
#






#
# Clean up
#

mongo_client.drop_database(DB_NAME)
mongo_client.drop_database(KEY_VAULT_DATABASE)

assert(not does_collection_exist(DB_NAME, ENCRYPTED_COLLECTION))

mongo_client.close()
sock.close()
