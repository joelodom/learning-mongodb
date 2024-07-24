from bson import STANDARD, CodecOptions
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption
import os
from pymongo.encryption_options import AutoEncryptionOpts

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "lookup_workaround"
KEY_VAULT_COLL = "__keyVault"  # Reuse the same database for the key vault
KEY_VAULT_NAMESPACE = f"{DB_NAME}.{KEY_VAULT_COLL}"

# We use 96 random hardcoded bytes (base64-encoded), because this is only an
# example. Production implementations of QE should use a Key Management System
# or be very thoughtful about how the secret key is secured and injected into
# the client at runtime.
LOCAL_MASTER_KEY = "V2hlbiB0aGUgY2F0J3MgYXdheSwgdGhlIG1pY2Ugd2lsbCBwbGF5LCBidXQgd2hlbiB0aGUgZG9nIGlzIGFyb3VuZCwgdGhlIGNhdCBiZWNvbWVzIGEgbmluamEuLi4u"

# The ClientEncryption helper object needs a key provider
KMS_PROVIDERS = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}

# A schemaless client to set up the key collection
schemaless_client = MongoClient(MONGO_URI)

# Initialize a ClientEncryption object
client_encryption = ClientEncryption(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    schemaless_client,
    CodecOptions(uuid_representation=STANDARD)
)

# Create a new data encryption key, which will appear in the database
# after this statement.
data_key_id = client_encryption.create_data_key("local")

# Sample documents and encryption schemas

employee_docs = [
    {"name": "James T. Kirk", "salary": 50000, "department_id": 1, "secret_code": { "code": 16309 }},
    {"name": "Spock", "salary": 60000, "department_id": 2, "secret_code": { "code": 31415}},
    {"name": "Leonard McCoy", "salary": 58000, "department_id": 1, "secret_code": { "code": 27182}}
]

department_docs = [
    {"department_id": 1, "name": "Engineering", "budget": 150000, "secret_code": { "code": 12345}},
    {"department_id": 2, "name": "Science", "budget": 130000, "secret_code": { "code": 54321}}
]

schema_map = {
    f"{DB_NAME}.employees": {
        "bsonType": "object",
        "properties": {
            "salary": {
                "encrypt": {
                    "bsonType": "int",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    "keyId": [data_key_id]
                }
            },
            "secret_code": {
                "bsonType": "object",
                "properties": {
                    "code": {
                        "encrypt": {
                            "bsonType": "int",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            "keyId": [data_key_id]
                        }
                    }
                }
            }
        }
    },
    f"{DB_NAME}.departments": {
        "bsonType": "object",
        "properties": {
            "budget": {
                "encrypt": {
                    "bsonType": "int",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    "keyId": [data_key_id]
                }
            },
            "secret_code": {
                "bsonType": "object",
                "properties": {
                    "code": {
                        "encrypt": {
                            "bsonType": "int",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            "keyId": [data_key_id]
                        }
                    }
                }
            }
        }
    }
}

# Instantiate connections for each of the collections

auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    schema_map=schema_map
)

client = MongoClient(
    MONGO_URI, auto_encryption_opts=auto_encryption_opts)

# Insert documents into the collections
client[DB_NAME].employees.insert_many(employee_docs)
client[DB_NAME].departments.insert_many(department_docs)

print("Sample documents have been inserted into employees and departments collections.")
