"""
This is a toy program to experiment with MongoDB Queryable Encryption
performance. I don't claim it to be super scientific.

Author: Joel Odom

TODO:

  * Upgrade to the lastest driver and shared library.
"""

from bson import STANDARD, CodecOptions
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import ClientEncryption

print("Welcome to the QE performance experiment.")


#
# AutoEncryptionOpts is a helper class that we pass to the MongoClient. It
# provides the KMS credentials and the namespace (database and collection)
# for the keys. Note that all of the keys in the key vault are encrypted
# on the client side, so the server can't access the keys or the data encrypted
# by them.
#

print("Setting up the AutoEncryptionOpts helper class...")

KMS_PROVIDER_NAME = "local"  # instead of a KMS
KEY_VAULT_DATABASE = "qe_performance_vault"
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
CRYPT_SHARED_LIB = "/Users/joel.odom/mongo_crypt_shared_v1-macos-arm64-enterprise-8.0.0-rc9/lib/mongo_crypt_v1.dylib"

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

DB_NAME = "qe_performance_testing"

mongo_client = MongoClient(URI, auto_encryption_opts=auto_encryption_options)


#
# Make sure we're starting with a clean database
#

ENCRYPTED_COLLECTION = "encrypted_collection"

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
            } ]  # range queryable
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
# Clean up
#

mongo_client.drop_database(DB_NAME)
mongo_client.drop_database(KEY_VAULT_DATABASE)

assert(not does_collection_exist(DB_NAME, ENCRYPTED_COLLECTION))

mongo_client.close()
