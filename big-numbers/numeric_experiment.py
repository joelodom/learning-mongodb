from pymongo import MongoClient
import os

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelcluster0.t7jymnq.mongodb.net/?retryWrites=true&w=majority&appName=JoelCluster0"

client = MongoClient(URI)
db = client['big_numbers']
collection = db['numeric_experiment']

NUMERIC = {
    "type": "numeric",
    "reduced": True,
    "components": [ 314159265, 358979323 ]
}

result = collection.insert_one(NUMERIC)

print(f"Inserted {result.inserted_id}")
