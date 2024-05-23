"""
Experimenting with self join.
"""

import os
from pymongo import MongoClient
from pprint import pprint

print("Connecting to Atlas...")

PASSWORD = os.getenv("JOEL_ATLAS_PWD")
if PASSWORD is None:
    raise Exception("Password not set in environment.")
MONGO_URI = f"mongodb+srv://joelodom:{PASSWORD}@joelqecluster.udwxc.mongodb.net/?retryWrites=true&w=majority&appName=JoelQECluster"

DB_NAME = "self_join_experiment"
COLLECTION_NAME = "employees"

client = MongoClient(MONGO_URI, auto_encryption_opts=None)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("(Re-)creating database...")

if DB_NAME in client.list_database_names():
    client.drop_database(DB_NAME)

collection.insert_many([
  {
    "_id": 1,
    "name": "Alice",
    "position": "CEO",
    "manager_id": None
  },
  {
    "_id": 2,
    "name": "Bob",
    "position": "CTO",
    "manager_id": 1
  },
  {
    "_id": 3,
    "name": "Charlie",
    "position": "CFO",
    "manager_id": 1
  },
  {
    "_id": 4,
    "name": "David",
    "position": "Engineer",
    "manager_id": 2
  },
  {
    "_id": 5,
    "name": "Eve",
    "position": "Engineer",
    "manager_id": 2
  },
  {
    "_id": 6,
    "name": "Frank",
    "position": "Accountant",
    "manager_id": 3
  }
])

print("Trying self-join...")

results = collection.aggregate([
  {
    "$lookup": {
      "from": "employees",
      "localField": "manager_id",
      "foreignField": "_id",
      "as": "manager_info"
    }
  },
  {
    "$unwind": {
      "path": "$manager_info",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$project": {
      "employee_name": "$name",
      "manager_name": "$manager_info.name",
      "manager_id": "$manager_id"
    }
  }
])

print("Results:")
for result in results:
    pprint(result)
