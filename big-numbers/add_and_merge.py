from bson import ObjectId
from pymongo import MongoClient
import os

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
URI = f"mongodb+srv://joelodom:{PASSWORD}@joelcluster0.t7jymnq.mongodb.net/?retryWrites=true&w=majority&appName=JoelCluster0"

# Connect to MongoDB
client = MongoClient(URI)
db = client['big_numbers']
collection = db['big_num_experiment']



result = collection.find_one()
print(result)




# Define a document with an array of integers
document1 = {
    "large_numbers": [
        3,
        1,
        4,
        1
    ]
}

document2 = {
    "large_numbers": [
        2,
        7,
        1,
        8
    ]
}

# Insert the document into the collection
result = collection.insert_one(document1)
id1 = result.inserted_id

# Output the ID of the new document
print("Document inserted with ID:", id1)

# Insert the document into the collection
result = collection.insert_one(document2)
id2 = result.inserted_id

# Output the ID of the new document
print("Document inserted with ID:", id2)




# Aggregation pipeline to sum the first elements of arrays from two documents and insert as a new document
pipeline = [
    {
        "$match": {
            "_id": { "$in": [id1, id2] }  # Filter to only include the specified documents
        }
    },
    {
        "$project": {
            "first_element": { "$arrayElemAt": [ "$large_numbers", 0] }  # Extract the first element of the array
        }
    },
    {
        "$group": {
            "_id": None,
            "sum": { "$sum": "$first_element" }  # Sum the first elements
        }
    },
    {
        "$project": {
            "_id": 0,
            "sum": 1
        }
    },
    {
        "$merge": {
            "into": "big_num_experiment",
            "on": "_id",  # Field to identify documents; this example might need adjusting
            "whenMatched": "replace",  # Replace the existing document in the target collection
            "whenNotMatched": "insert"  # Insert as a new document if no match exists
        }
    }
]

# Execute the aggregation pipeline
collection.aggregate(pipeline)

print("Aggregation and insertion complete.")
