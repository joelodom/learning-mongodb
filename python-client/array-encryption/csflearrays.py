"""
Joel's cheap, messy, non-production class that is shared by these experiments.
"""

import os
from bson import STANDARD, CodecOptions
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts

#
# Connect to my Atlas instance, create a ClientEncryption helper, and create the
# data key.
#

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

# 96 random hardcoded key bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

kms_providers = {  # This is where the ClientEncryption will fetch keys
    "local": {
        "key": LOCAL_MASTER_KEY
    },
}

KEY_VAULT_DB = "array_encryption_keys"  # all encrypted with the CMK
KEY_VAULT_COLL = "__keyVault"
key_vault_namespace = f"{KEY_VAULT_DB}.{KEY_VAULT_COLL}"

key_vault_client = MongoClient(URI)

client_encryption = ClientEncryption(
    kms_providers,
    key_vault_namespace,
    key_vault_client,
    CodecOptions(uuid_representation=STANDARD),
)

def create_data_key():
    # Creating the data key at this point is enough to cause the key vault database
    # to appear in my cluster. I should avoid re-creating this same key
    # at each run of the program (see the quick start guide for more about this),
    # but it's just an experiment.

    data_key_id = client_encryption.create_data_key("local")
    return data_key_id

def create_mongo_client(schema_map):
    #
    # Create a seprate MongoClient because the steps above are just for creating
    # a key database and wouldn't be done again in a real program. This uses the
    # caller-supplied schema map for automatic encryption.
    #

    client = MongoClient(
        URI,
        auto_encryption_opts=AutoEncryptionOpts(
            kms_providers,
            key_vault_namespace,
            schema_map=schema_map
        )
    )

    return client
