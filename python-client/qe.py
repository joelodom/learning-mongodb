from pymongo import MongoClient
import os



PASSWORD=os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")

CONNNECTION_STRING = f"mongodb+srv://joelodom:{PASSWORD}@joelcluster0.t7jymnq.mongodb.net/?retryWrites=true&w=majority&appName=JoelCluster0"
DATABASE = "JoelDatabase"
COLLECTION = "JoelCollection"

client = MongoClient(CONNNECTION_STRING)

# Specify the database and collection
db = client[DATABASE]
collection = db[COLLECTION]

# Document to insert
user_record = {"name": "John Doe", "email": "john.doe@example.com"}

# Inserting the document
result = collection.insert_one(user_record)
print(f"One record inserted: {result.inserted_id}")
