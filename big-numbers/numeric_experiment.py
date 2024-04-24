from bson import ObjectId
from pymongo import MongoClient
import os

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelcluster0.t7jymnq.mongodb.net/?retryWrites=true&w=majority&appName=JoelCluster0"

client = MongoClient(URI)
db = client["big_numbers"]
COLLECTION_NAME = "numeric_experiment"
collection = db[COLLECTION_NAME]

NUMERIC = {
    "type": "numeric",
    "reduced": True,
    "components": [ 314159265, 358979323, 846264338 ]
}

result = collection.insert_one(NUMERIC)

print(f"Inserted {result.inserted_id}")

results = collection.aggregate([

    # The first stage matches the inserted ID

    {
        "$match": {
            "_id": result.inserted_id
        }
    },

    # Now use $project to create a new field on the matched document,
    # stringRepresentation. $reduce "Applies an expression to each element
    # in an array and combines them into a single value."

    {
        "$project": {
        "stringRepresentation": {
            "$reduce": {
                "input": "$components", # the input array to process
                "initialValue": "",
                "in": { "$concat": [ "$$value", { "$toString": "$$this" } ] }
            }
        }
        }
    },

    # Drop all except stringRepresentation

    {
        "$project": {
        "_id": 0,
        "stringRepresentation": 1
        }
    },

    # Create a new document with the representation
    # (this empties the aggregation pipeline, btw)

    {
        "$merge": {
        "into": COLLECTION_NAME,
        "whenMatched": "fail",
        "whenNotMatched": "insert"
        }
    }
]);

print(f"Aggregation results:")

for result in results:
    print(result) # Nothing because of $merge at the end

print(f"Done.")
