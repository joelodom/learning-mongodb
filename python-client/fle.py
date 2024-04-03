#
# Joel's lab for learning more about queryable encryption in MongoDB.
#

from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
from bson.codec_options import CodecOptions
from bson.binary import STANDARD, UUID
import base64
from pymongo.encryption_options import AutoEncryptionOpts



PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")

CONNNECTION_STRING = f"mongodb+srv://joelodom:{PASSWORD}@joelcluster0.t7jymnq.mongodb.net/?retryWrites=true&w=majority&appName=JoelCluster0"
DATABASE = "JoelDatabase"
COLLECTION = "JoelCollection"


#
# Field-level encryption experiment
# Ref https://www.mongodb.com/docs/manual/core/csfle/quick-start/#std-label-csfle-quick-start
#

# 96 random hardcoded bytes, because it's only an example
local_master_key = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

kms_providers = {
    "local": {
        "key": local_master_key
    },
}

# I think the key vault is where keys are stored, but are all encrypted with the local
# master key.

key_vault_database = "encryption"
key_vault_collection = "__keyVault"
key_vault_namespace = f"{key_vault_database}.{key_vault_collection}"
key_vault_client = MongoClient(CONNNECTION_STRING)

client_encryption = ClientEncryption(
    kms_providers,
    key_vault_namespace,
    key_vault_client,
    CodecOptions(uuid_representation=STANDARD),
)

# Creating the data key at this point is enough to cause encryption.__keyVault
# to appear in my cluster. I think I need to avoid re-creating this every
# run of the program (see the quick start guide for more about this).
data_key_id = client_encryption.create_data_key("local")

# base_64_data_key_id = base64.b64encode(data_key_id)
# print("DataKeyId [base64]: ", base_64_data_key_id)

# I think the schema map specifics what should be encrypted.

schema_map = {
    f"{DATABASE}.{COLLECTION}": {
        "bsonType": "object",
        "properties": {
            "encryptedField": {
                "encrypt": {
                    "keyId": [data_key_id],
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                }
            }
        }
    }
}


client = MongoClient(
    CONNNECTION_STRING,
    auto_encryption_opts=AutoEncryptionOpts(
        kms_providers,
        key_vault_namespace,
        schema_map=schema_map
    )
)

#client = MongoClient(CONNNECTION_STRING)
db = client[DATABASE]
collection = db[COLLECTION]


def insert_a_doc():
    """
    Insert a document into the database.
    """
    print("Inserting a record with an encrypted field...")
    doc = {
        "name": "John Doe",
        "email": "john.doe@example.com",
        "encryptedField": "Super secret stuff."
        }
    result = collection.insert_one(doc)
    print(f"One record inserted: {result.inserted_id}")
    print()

def dump_all_docs():
    """
    Dumps the entire collection, document by document.
    """
    print("All documents:")
    for doc in collection.find():
        print(doc)
    print()

def delete_all_docs():
    """
    Deletes all of the documents from my collection.
    """
    print("Deleting all documents...")
    result = collection.delete_many({})
    print(f"Number of documents deleted: {result.deleted_count}")
    print()

def delete_all_keys():
    """
    Deletes all of the keys in my key vault.
    """
    print("Deleting all keys...")
    key_vault_db = key_vault_client[key_vault_database]
    result = key_vault_db[key_vault_collection].delete_many({})
    print(f"Number of keys deleted: {result.deleted_count}")
    print()


insert_a_doc()
dump_all_docs()
# delete_all_docs()
# delete_all_keys()
