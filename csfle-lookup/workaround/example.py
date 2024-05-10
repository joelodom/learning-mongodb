from pymongo import MongoClient
import os

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "lookup_workaround"  # Reuse the same database

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

# Print the results (encrypted data will still be encrypted)
for result in results:
    print(result)
