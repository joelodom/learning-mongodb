#
# Rough and dirty experiment with encrypting QE arrays
#





from pymongo.encryption_options import AutoEncryptionOpts
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
from bson.codec_options import CodecOptions
from bson.binary import STANDARD
import random

# KMS provider name should be one of the following: "aws", "gcp", "azure", "kmip" or "local"
kms_provider_name = "local"

PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")

#uri = os.environ['MONGODB_URI']  # Your connection URI
uri = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

key_vault_database_name = "encryption"
key_vault_collection_name = "__keyVault"
key_vault_namespace = f"{key_vault_database_name}.{key_vault_collection_name}"
encrypted_database_name = "qe_arrays"
encrypted_collection_name = "medications"

# 96 random hardcoded bytes, because it's only an example
local_master_key = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

kms_provider_credentials = {
    "local": {
        "key": local_master_key
    },
}

CRYPT_SHARED_LIB = "/Users/joel.odom/mongo_crypt_shared_v1-macos-arm64-enterprise-7.0.6/lib/mongo_crypt_v1.dylib"

auto_encryption_options = AutoEncryptionOpts(
    kms_provider_credentials,
    key_vault_namespace,
    crypt_shared_lib_path=CRYPT_SHARED_LIB
)

encrypted_client = MongoClient(
    uri, auto_encryption_opts=auto_encryption_options)

encrypted_fields_map = {
    "fields": [
        {
            "path": "patientRecord.ssn",
            "bsonType": "string",
            "queries": [{"queryType": "equality"}] # queryable
        },
        {
            "path": "patientRecord.billing", # encrypted, not queryable
            "bsonType": "object",
        },
        {
            "path": "medications",
            "bsonType": "array"
        }
    ]
}

client_encryption = ClientEncryption(
    kms_providers=kms_provider_credentials,
    key_vault_namespace=key_vault_namespace,
    key_vault_client=encrypted_client,
    codec_options=CodecOptions(uuid_representation=STANDARD)
)

customer_master_key_credentials = {} # no creds because using a local key CMK

# client_encryption.create_encrypted_collection(
#     encrypted_client[encrypted_database_name],
#     encrypted_collection_name,
#     encrypted_fields_map,
#     kms_provider_name,
#     customer_master_key_credentials,
# )

SECRET_SSN = f"{random.randint(0, 999999999):09d}"

patient_document = {
    "patientName": "Jon Doe",
    "patientId": 12345678,
    "patientRecord": {
        "ssn": SECRET_SSN,
        "billing": {
            "type": "Visa",
            "number": "4111111111111111",
        },
    },
    "medications": [
        "Tylenol",
        "Asprin"
    ]
}

encrypted_collection = encrypted_client[encrypted_database_name][encrypted_collection_name]
result = encrypted_collection.insert_one(patient_document)
print(f"One record inserted: {result.inserted_id}")
print()

# # At this point in working through the quick start, I can see reconds appear
# # in Atlas. w00t!

# find_result = encrypted_collection.find_one({
#     "patientRecord.ssn": SECRET_SSN
# })

# print(find_result)
# print()

# # At this point in working through the quick start, I was able to retrieve the
# # result. w00t!
# #
# # If I run the program multiple times, I see new objects being created in Atlas.
# # I do not see a proliferation of keys in the vault collection, unlike my FLE
# # experiment. I'll have to think about the expected / desired behavior.













# #
# # Create a database schema that specifies what to encrypt and how.
# #
# # TO SHOW THE WORKING NON-ENCRYPTING ANALOG, COMMENT OUT THE PART OF THE 
# # SCHEMA THAT SPECIFIES ENCRYPTING THE ARRAY.
# #

# DATABASE = "array_encryption_database"
# COLLECTION = "array_encryption_collection"

# data_key_id = csflearrays.create_data_key()

# schema_map = {
#     f"{DATABASE}.{COLLECTION}": {
#         "bsonType": "object",
#         "properties": {
#             "encryptedArray": {
#                 "encrypt": {
#                     "keyId": [ data_key_id ],
#                     "bsonType": "array",
#                     "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
#                 }
#             }
#         }
#     }
# }

# #
# # Create a record in the database that includes an encrypted array.
# #

# client = csflearrays.create_mongo_client(schema_map)

# doc = {
#     "name": "John Doe",
#     "email": "john.doe@example.com",
#     "encryptedArray": [ "Super", "secret", "stuff" ]
#     }

# client[DATABASE][COLLECTION].insert_one(doc)

# #
# # In MongoDB 7 the line above fails (when using encryption) with,
# # "Cannot encrypt element of type: array" because we don't support it yet.
# #

# results = client[DATABASE][COLLECTION].find(
#     {
#         "encryptedArray": {
#             "$in": [ "stuff" ]
#         }
#     }
# )

# for result in results:
#     print(result)

# print()
