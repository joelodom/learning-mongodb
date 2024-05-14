from bson import STANDARD, CodecOptions
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption
import os
from pymongo.encryption_options import AutoEncryptionOpts

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "lookup_workaround"  # Reuse the same database
KEY_VAULT_COLL = "__keyVault"
KEY_VAULT_NAMESPACE = f"{DB_NAME}.{KEY_VAULT_COLL}"

# 96 random hardcoded key bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

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

employee_schema = {
    f"{DB_NAME}.employees": {
        "bsonType": "object",
        "properties": {
            "salary": {
                "encrypt": {
                    "bsonType": "int",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    "keyId": [data_key_id]
                }
            },
            "secret_code": {
                "bsonType": "object",
                "properties": {
                    "code": {
                        "encrypt": {
                            "bsonType": "int",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            "keyId": [data_key_id]
                        }
                    }
                }
            }
        }
    }
}

department_schema = {
    f"{DB_NAME}.departments": {
        "bsonType": "object",
        "properties": {
            "budget": {
                "encrypt": {
                    "bsonType": "int",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    "keyId": [data_key_id]
                }
            },
            "secret_code": {
                "bsonType": "object",
                "properties": {
                    "code": {
                        "encrypt": {
                            "bsonType": "int",
                            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            "keyId": [data_key_id]
                        }
                    }
                }
            }
        }
    }
}

# Instantiate connections for each of the collections

employee_auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    schema_map=employee_schema
)

employee_client = MongoClient(
    MONGO_URI, auto_encryption_opts=employee_auto_encryption_opts)

department_auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    schema_map=department_schema
)

department_client = MongoClient(
    MONGO_URI, auto_encryption_opts=department_auto_encryption_opts)

# Insert documents into the collections
employee_client[DB_NAME].employees.insert_many(employee_docs)
department_client[DB_NAME].departments.insert_many(department_docs)

print("Sample documents have been inserted into employees and departments collections.")
