"""
This is a toy program by Joel Odom to demonstrate MongoDB CSFLE
and searching in arrays by index.
"""

import csflearrays

#
# Create a database schema that specifies what to encrypt and how.
#
# TO SHOW THE WORKING NON-ENCRYPTING ANALOG, COMMENT OUT THE PART OF THE 
# SCHEMA THAT SPECIFIES ENCRYPTING THE ARRAY.
#

DATABASE = "array_encryption_database"
COLLECTION = "array_encryption_collection"

data_key_id = csflearrays.create_data_key()

schema_map = {
    f"{DATABASE}.{COLLECTION}": {
        "bsonType": "object",
        "properties": {
            "encryptedArray": {
                "encrypt": {
                    "keyId": [ data_key_id ],
                    "bsonType": "array",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                }
            }
        }
    }
}

#
# Create a record in the database that includes an encrypted array.
#

client = csflearrays.create_mongo_client(schema_map)

doc = {
    "name": "John Doe",
    "email": "john.doe@example.com",
    "encryptedArray": [ "Super", "secret", "stuff" ]
    }

client[DATABASE][COLLECTION].insert_one(doc)

#
# In MongoDB 7 the line above fails (when using encryption) with,
# "Cannot encrypt element of type: array" because we don't support it yet.
#

results = client[DATABASE][COLLECTION].find(
    {
        "encryptedArray.2": "stuff"
    }
)

cnt = 0
for result in results:
    print(result)
    cnt += 1

print()
print(f"{cnt} results.")
print()
