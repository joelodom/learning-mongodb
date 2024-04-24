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

BASE = 1000000000

#
# Take a reduced numeric and save its string representation
#

REDUCED_NUMERIC = {
    "type": "numeric",
    "reduced": True,
    "components": [ 314159265, 358979323, 846264338 ]
}

result = collection.insert_one(REDUCED_NUMERIC)

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

print(f"Done showing aggregation results.")

#
# Now let's see if we can reduce a numeric using an aggregation pipeline
#

UNREDUCED_NUMERIC = {
    "type": "numeric",
    "reduced": True,
    "components": [ 314159265, 0, 358979323846264338 ]  # base 1,000,000,000
}

result = collection.insert_one(UNREDUCED_NUMERIC)

print(f"Inserted {result.inserted_id}")

results = collection.aggregate([

    # The first stage matches the inserted ID

    {
        "$match": {
            "_id": result.inserted_id
        }
    },

    # Now use $addFields and $reverseArray to add reversedComponents

     {
        "$addFields": {
            "reversedComponents": { "$reverseArray": "$components" }
        }
    },

    #
    # EXPERIMENTING with using aggregation pipeline to reduce the numeric.
    # There are several problems with this: (1) leading zeros should not
    # be dropped (applies to above as well), (2) there could still be overflow,
    # and (3) there are unneeded zeros. SO THIS ALL STILL NEEDS WORK.

    {
        "$addFields": {
            "modulos": {
                "$map": {
                    "input": "$reversedComponents",
                    "as": "component",
                    "in": { "$mod": [ "$$component", BASE ] }
                }
            }
        }
    },

    {
        "$addFields": {
            "modulosWithZeroAppended": {
                "$concatArrays": [ "$modulos", [0] ]
            }
        }
    },

    {
        "$addFields": {
            "quotients": {
                "$map": {
                    "input": "$reversedComponents",
                    "as": "component",
                    "in": { "$toInt": { "$floor": { "$divide": [ "$$component", BASE ] } } }
                }
            }
        }
    },

    {
        "$addFields": {
            "quotientsWithZeroPrepended": {
                "$concatArrays": [ [0], "$quotients" ]
            }
        }
    },

    {
        "$addFields": {
            "summedArray": {
                "$map": {
                    "input": { "$range": [0, { "$size": "$modulosWithZeroAppended" }]},
                    "as": "index",
                    "in": {
                        "$add": [
                            {"$arrayElemAt": ["$modulosWithZeroAppended", "$$index"]},
                            {"$arrayElemAt": ["$quotientsWithZeroPrepended", "$$index"]}
                        ]
                    }
                }
            }
        }
    },

    {
        "$addFields": {
            "reduced": { "$reverseArray": "$summedArray" }
        }
    },
]);

print(f"Aggregation results:")

for result in results:
    print(result) # Nothing because of $merge at the end

print(f"Done showing aggregation results.")

