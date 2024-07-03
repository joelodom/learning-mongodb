from pymongo import MongoClient, ASCENDING
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import ClientEncryption, MongoCryptOptions
import base64
import os
from bson.codec_options import CodecOptions
from bson.binary import STANDARD, UUID
import os

from pymongo import MongoClient
from pymongo.encryption import (
    Algorithm,
    AutoEncryptionOpts,
    ClientEncryption,
    QueryType,
)
import pprint



CRYPT_SHARED_LIB = "/Users/joel.odom/mongo_crypt_shared_v1-macos-arm64-enterprise-7.0.6/lib/mongo_crypt_v1.dylib"




# # 96 random hardcoded bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

provider = "local"

kms_providers = {
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}
# end-kmsproviders






# # start-datakeyopts
# # end-datakeyopts


# # start-create-index
# connection_string = "mongodb://127.0.0.1:27017/"

# key_vault_coll = "__keyVault"
# key_vault_db = "encryption"
# key_vault_namespace = f"{key_vault_db}.{key_vault_coll}"
# key_vault_client = MongoClient(connection_string
#                                )
# # Drop the Key Vault Collection in case you created this collection
# # in a previous run of this application.
# key_vault_client.drop_database(key_vault_db)

# key_vault_client[key_vault_db][key_vault_coll].create_index(
#     [("keyAltNames", ASCENDING)],
#     unique=True,
#     partialFilterExpression={"keyAltNames": {"$exists": True}},
# )
# # end-create-index


# # start-create-dek
# client = MongoClient(connection_string)
# client_encryption = ClientEncryption(
#     kms_providers,  # pass in the kms_providers variable from the previous step
#     key_vault_namespace,
#     client,
#     CodecOptions(uuid_representation=STANDARD),
# )

# data_key_id_1 = client_encryption.create_data_key(provider, key_alt_names=["dataKey1"])
# data_key_id_2 = client_encryption.create_data_key(provider, key_alt_names=["dataKey2"])
# data_key_id_3 = client_encryption.create_data_key(provider, key_alt_names=["dataKey3"])
# data_key_id_4 = client_encryption.create_data_key(provider, key_alt_names=["dataKey4"])
# # end-create-dek


# # start-create-enc-collection
# encrypted_db_name = "medicalRecords"
# encrypted_coll_name = "patients"
# encrypted_fields_map = {
#     f"{encrypted_db_name}.{encrypted_coll_name}": {
#         "fields": [
#             {
#                 "keyId": data_key_id_1,
#                 "path": "patientId",
#                 "bsonType": "int",
#                 "queries": {"queryType": "equality"},
#             },
#             {
#                 "keyId": data_key_id_2,
#                 "path": "medications",
#                 "bsonType": "array",
#             },
#             {
#                 "keyId": data_key_id_3,
#                 "path": "patientRecord.ssn",
#                 "bsonType": "string",
#                 "queries": {"queryType": "equality"},
#             },
#             {
#                 "keyId": data_key_id_4,
#                 "path": "patientRecord.billing",
#                 "bsonType": "object",
#             },
#         ],
#     },
# }

# key_vault_namespace = "encryption.__keyVault"


# auto_encryption = AutoEncryptionOpts(
#     kms_providers,
#     key_vault_namespace,
#     encrypted_fields_map=encrypted_fields_map,
#     crypt_shared_lib_path=CRYPT_SHARED_LIB,
# )

# secure_client = MongoClient(connection_string, auto_encryption_opts=auto_encryption)
# # Drop the encrypted collection in case you created this collection
# # in a previous run of this application.
# secure_client.drop_database(encrypted_db_name)
# encrypted_db = secure_client[encrypted_db_name]
# encrypted_db.create_collection(encrypted_coll_name)
# print("Created encrypted collection!")
# # end-create-enc-collection





















# start-key-vault
key_vault_namespace = "encryption.__keyVault"
key_vault_db_name, key_vault_coll_name = key_vault_namespace.split(".", 1)
# end-key-vault

# start-retrieve-deks
connection_string = "mongodb://127.0.0.1:27017/"
client = MongoClient(connection_string)
key_vault = client[key_vault_db_name][key_vault_coll_name]

data_key_id_1 = key_vault.find_one({"keyAltNames": "dataKey1"})["_id"]
data_key_id_2 = key_vault.find_one({"keyAltNames": "dataKey2"})["_id"]
# end-retrieve-deks

# start-extra-options
opts = AutoEncryptionOpts(
    kms_providers,
    key_vault.full_name,
    bypass_query_analysis=True,
    key_vault_client=client,
    crypt_shared_lib_path=CRYPT_SHARED_LIB,
)
# end-extra-options

# start-client
encrypted_client = MongoClient(connection_string, auto_encryption_opts=opts)
db = encrypted_client.medicalRecords
coll = db.patients
# end-client

# start-client-enc
client_encryption = ClientEncryption(
    kms_providers, key_vault_namespace, client, client.codec_options
)
# end-client-enc

# start-insert
patientId = 12345678
medications = ["Atorvastatin", "Levothyroxine"]
indexed_insert_payload = client_encryption.encrypt(
    patientId, Algorithm.INDEXED, data_key_id_1, contention_factor=1
)
unindexed_insert_payload = client_encryption.encrypt(
    medications, Algorithm.UNINDEXED, data_key_id_2
)
coll.insert_one(
    {
        "firstName": "Jon",
        "patientId": indexed_insert_payload,
        "medications": unindexed_insert_payload,
    }
)
# end-insert

# start-find
find_payload = client_encryption.encrypt(
    patientId,
    Algorithm.INDEXED,
    data_key_id_1,
    query_type=QueryType.EQUALITY,
    contention_factor=1,
)
doc = coll.find_one({"encryptedIndexed": find_payload})
print("\nReturned document:\n")
pprint.pprint(doc)
# end-find

client_encryption.close()
encrypted_client.close()
client.close()








