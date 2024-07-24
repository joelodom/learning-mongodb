"""
This sample program demonstrates a workaround for $lookup and MongoDB CSFLE when
you only need to join on an unencrypted field and not write back to the database
in the aggregation pipeline.

See setup.py for how to set up the database for this program, including
the schema map that specifies the encrypted fields.

by Joel Odom
"""

import base64
from pprint import pprint
from uuid import UUID
from bson import STANDARD, Binary, CodecOptions
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption, Algorithm
from pymongo.encryption_options import AutoEncryptionOpts

#
# Little Parameters
#

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "lookup_workaround"  # Reuse the same database
KEY_VAULT_COLL = "__keyVault"
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

#
# Connect to MongoDB and perform a $lookup where both collections have an
# encrypted field. Notice that we don't have to specify a schema map or key ids
# because we are only doing reads and CSFLE handles that automagically. The
# $lookup wouldn't work if we didn't bypass automatic encryption because we
# don't (yet) support automatic encryption for CSFLE. We perform a match and a
# merge to add some additional color.
#

employee_auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    bypass_auto_encryption=True  # secret sauce to make $lookup possible
)

client = MongoClient(
    MONGO_URI, auto_encryption_opts=employee_auto_encryption_opts)
db = client[DB_NAME]

# client_encryption = ClientEncryption(
#     KMS_PROVIDERS,
#     KEY_VAULT_NAMESPACE,
#     client,
#     CodecOptions(uuid_representation=STANDARD)
# )

# data_key_id = Binary(b"ID\xf2\x07\xe8VL\xff\x8dG\xc2\xb4]\x8a\xcbC", 4)

# encrypted_salary = client_encryption.encrypt(
#     value=50000,
#     algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
#     key_id=data_key_id
# )

# r = db.employees.find_one( { "salary": encrypted_salary } )
# print(r)  # Why no results???

pipeline = [

    { "$match": { "name": "Spock" } },
    # { "$match": { "salary": encrypted_salary } },

    {
        "$lookup": {
            "from": "departments",
            "localField": "department_id",
            "foreignField": "department_id",
            "as": "department_info"
        },
    },
    {
        "$set": {
        "department_info": { "$first": "$department_info" }
        }
    },
    {
        "$set": {
            "secret_code": {
            "$mergeObjects": [
                "$$ROOT.secret_code",
                "$department_info.secret_code"
                ]
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "department_info._id": 0
        }
    }
]

results = db.employees.aggregate(pipeline)

for result in results:
    pprint(result)
    print()