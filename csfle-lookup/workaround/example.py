"""
This sample program demonstrates a workaround for $lookup and MongoDB CSFLE when
you only need to join on an unencrypted field and not write back to the database
in the aggregation pipeline.

See setup.py for how to set up the database for this program, including
the schema map that specifies the encrypted fields.

by Joel Odom
"""

from pymongo import MongoClient
import os
from pymongo.encryption import ClientEncryption
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

# 96 random hardcoded key bytes, because it's only an example
LOCAL_MASTER_KEY = b";1\x0f\x06%\x97\x99\xa5\xaen\xb4\x8b<T3v\x0b\\\xeb\x9f\x13\xa8\xb9\xc0[\xa0\xc3\xb9\xa7\x0e|\x8e3o5\x1a\xd8\x08H\x0b \xf1\xc1Eb\xeb\x0b\x8e\xde\xe4Oz\xe3\x0bs%$R\x13?\x9aI\x1d\xd0'\xee\xd8\x06\x85\x16\x90\xb0\x9ec#\x9c=Y\x8f\xc5\xc211\xc5\x15\x07\xae\xd2\xc6\xdb\xc5\x9c^S\xae,"

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
# don't (yet) support automatic encryption for CSFLE.
#

employee_auto_encryption_opts=AutoEncryptionOpts(
    KMS_PROVIDERS,
    KEY_VAULT_NAMESPACE,
    bypass_auto_encryption=True  # secret sauce
    )

client = MongoClient(
    MONGO_URI, auto_encryption_opts=employee_auto_encryption_opts)
db = client[DB_NAME]

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

results = db.employees.aggregate(pipeline)

for result in results:
    print(result)
