"""
This utilities module is kind of a mess...
"""


#
# AutoEncryptionOpts is a helper class that we pass to the MongoClient. It
# provides the KMS credentials and the namespace (database and collection)
# for the keys. Note that all of the keys in the key vault are encrypted
# on the client side, so the server can't access the keys or the data encrypted
# by them.
#

from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts


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

def create_client():
    return MongoClient(URI, auto_encryption_opts=auto_encryption_options)





ENCRYPTED_COLLECTION = "encrypted_collection"



def write_line_to_csv(filename, data):
    """
    Writes a single line of data to a CSV file.

    Args:
        filename (str): The name or path of the CSV file.
        data (list): A list of values representing a single row in the CSV file.
    """

    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)
