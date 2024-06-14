"""
This is a quick and dirty experiment to see if we can support a case with
rotating keys and search in the same collection. This is not production
code nor is this pattern advisable. It's just an experiment.
"""


from pprint import pprint
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
from bson.codec_options import CodecOptions
from bson.binary import STANDARD
import random
from pymongo.errors import EncryptedCollectionError


PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")

AZURE_CLIENT_SECRET=os.getenv("JOEL_AZURE_CLIENT_SECRET")
if AZURE_CLIENT_SECRET is None:
    raise Exception("Azure client secret not set in environment.")

uri = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

key_vault_database_name = "medicalRecordsExperimentEncryption"
key_vault_collection_name = "__keyVault"
key_vault_namespace = f"{key_vault_database_name}.{key_vault_collection_name}"
encrypted_database_name = "medicalRecordsExperiment"
encrypted_collection_name = "patients"

kms_provider_credentials = {
    "azure": {
        "tenantId": "c96563a8-841b-4ef9-af16-33548de0c958",
        "clientId": "4da15843-a913-4d0d-880a-657596ca8eea",
        "clientSecret": AZURE_CLIENT_SECRET
    }
}

customer_master_key_credentials = {
    "keyName": "joel-qe-key",
    "keyVaultEndpoint": "https://joel-key-vault.vault.azure.net/keys/joel-qe-key/6672e5ebdf8a4607ad0f7049642a8169"
}

CRYPT_SHARED_LIB = "/Users/joel.odom/mongo_crypt_shared_v1-macos-arm64-enterprise-7.0.6/lib/mongo_crypt_v1.dylib"

auto_encryption_options = AutoEncryptionOpts(
    kms_provider_credentials,
    key_vault_namespace,
    crypt_shared_lib_path=CRYPT_SHARED_LIB
)

encrypted_mongo_client = MongoClient(
    uri, auto_encryption_opts=auto_encryption_options)

encrypted_fields_map = {
    "fields": [
        {
            "path": "patientRecord.ssn",
            "bsonType": "string",
            "queries": [{"queryType": "equality"}] # queryable
        },
        {
            "path": "patientRecord.billing.number",
            "bsonType": "string",
            "queries": [{"queryType": "equality"}] # queryable
        }
    ]
}

client_encryption = ClientEncryption(
    kms_providers=kms_provider_credentials,
    key_vault_namespace=key_vault_namespace,
    key_vault_client=encrypted_mongo_client,
    codec_options=CodecOptions(uuid_representation=STANDARD)
)

try:
    client_encryption.create_encrypted_collection(
        encrypted_mongo_client[encrypted_database_name],
        encrypted_collection_name,
        encrypted_fields_map,
        "azure",
        customer_master_key_credentials,
    )
except EncryptedCollectionError:
    pass

SECRET_SSN = f"{random.randint(0, 999999999):09d}"
SECRET_NUMBER = "4111111111111111"

patient_document = {
    "patientName": "Jon Doe",
    "patientId": 12345678,
    "patientRecord": {
        "ssn": SECRET_SSN,
        "billing": {
            "type": "Visa",
            "number": SECRET_NUMBER,
        },
    },
}

encrypted_collection = encrypted_mongo_client[encrypted_database_name][encrypted_collection_name]
result = encrypted_collection.insert_one(patient_document)
print(f"One record inserted: {result.inserted_id}")
print()

results = encrypted_collection.find({
    "patientRecord.billing.number": SECRET_NUMBER
})

count = 0
for result in results:
    pprint(result)
    count += 1

print()
print(f"Found {count} records.")
print()
