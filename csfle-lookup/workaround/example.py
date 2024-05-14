from bson import STANDARD, CodecOptions
from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption

DEMONSTRATE_DECRYPTION = True  # Switch to false to show encrypted values

#
# Parameters
#

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

#
# Connect to MongoDB and perform a $lookup where both collections are encrypted.
# The results will contain the encrypted values because we can't do
# automatic encryption in a $lookup.
#

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Perform a $lookup to join employees with departments
pipeline = [
    {
        "$lookup": {
            "from": "departments",
            "localField": "department_id",
            "foreignField": "department_id",
            "as": "department_info"
        }
    }
]

# Execute the aggregation
results = db.employees.aggregate(pipeline)

#
# Now use explicit decryption to decrypt salaries and budgets. Notice that I 
# can use the same ClientEncryption for both.
#

client_encryption = ClientEncryption(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    MongoClient(MONGO_URI), # A dedicated encryption client
    CodecOptions(uuid_representation=STANDARD)
)

# Decrypting fields manually
for result in results:
    if DEMONSTRATE_DECRYPTION:
        if "salary" in result:
            decrypted_value = client_encryption.decrypt(result["salary"])
            result["salary"] = decrypted_value
        if "budget" in result["department_info"][0]:
            decrypted_value = client_encryption.decrypt(result["department_info"][0]["budget"])
            result["department_info"][0]["budget"] = decrypted_value

    print(result)

# Clean up
client_encryption.close()
client.close()
