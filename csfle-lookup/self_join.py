"""
Experimenting with self join.
"""

import os
from bson import STANDARD, CodecOptions
from pymongo import MongoClient
from pprint import pprint
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts


print("Connecting to Atlas...")

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "self_join_experiment"
COLLECTION_NAME = "employees"

client = MongoClient(MONGO_URI)


print("Destroying old database (if it exists)...")

if DB_NAME in client.list_database_names():
    client.drop_database(DB_NAME)


print("Setting up automatic encryption...")

KEY_VAULT_DB = DB_NAME  # Reuse the same database
KEY_VAULT_COLL = "__keyVault"
KEY_VAULT_NAMESPACE = f"{KEY_VAULT_DB}.{KEY_VAULT_COLL}"

# 96 random hardcoded key bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

KMS_PROVIDERS = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}

client_encryption = ClientEncryption(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    client,
    CodecOptions(uuid_representation=STANDARD)
)

data_key_id = client_encryption.create_data_key("local")

SCHEMA_MAP = {
    f"{DB_NAME}.{COLLECTION_NAME}": {
        "bsonType": "object",
        "properties": {
            "position": {
                "encrypt": {
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    "keyId": [data_key_id]
                }
            }
        }
    }
}

auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    schema_map=SCHEMA_MAP
)

# Replace client with one that has automatic encryption
client = MongoClient(MONGO_URI, auto_encryption_opts=auto_encryption_opts)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


print("Adding sample data...")

collection.insert_many([
  {
    "_id": 1,
    "name": "Alice",
    "position": "CEO",
    "manager_id": None
  },
  {
    "_id": 2,
    "name": "Bob",
    "position": "CTO",
    "manager_id": 1
  },
  {
    "_id": 3,
    "name": "Charlie",
    "position": "CFO",
    "manager_id": 1
  },
  {
    "_id": 4,
    "name": "David",
    "position": "Engineer",
    "manager_id": 2
  },
  {
    "_id": 5,
    "name": "Eve",
    "position": "Engineer",
    "manager_id": 2
  },
  {
    "_id": 6,
    "name": "Frank",
    "position": "Accountant",
    "manager_id": 3
  }
])


print("Trying a self-join with a subpipeline...")

pipeline = [
    {
        "$lookup": {
            "from": "employees",
            "localField": "manager_id",
            "foreignField": "_id",
            "as": "manager_info",
            "pipeline": [
                {
                    "$match": { "position": "CTO" }
                }
            ]
        }
    },

    # At this point every record in the collection now has a manager_info
    # document on it, generated by the sub-pipeline. The records that don't
    # match in the subpipeline are [], whereas the ones that do match are
    # an array of all of the documents that match. If I comment out the pipeline
    # then the only difference is that every document in the pipeline has a
    # filled-out manager_info array. So I think the way this works is that the
    # $lookup always adds a new sub-document to the document that is the
    # join for all matches, and the sub-pipeline can be used to manage details
    # about the sub-collections that are added in the lookup.

    # {
    #     "$unwind": {
    #         "path": "$manager_info",
    #         "preserveNullAndEmptyArrays": True
    #     }
    # },
    # {
    #     "$project": {
    #         "employee_name": "$name",
    #         "manager_name": "$manager_info.name",
    #         "manager_position": "$manager_info.position",
    #         "manager_id": "$manager_id"
    #     }
    # }
]

results = collection.aggregate(pipeline)

print("Results:")
for result in results:
    pprint(result)
