#
# Joel's lab for learning more about queryable encryption in MongoDB.
#

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
DB = client[DATABASE]
COLLECTION = DB[COLLECTION]


def insert_a_doc():
    """
    Insert a document into the database.
    """
    doc = {"name": "John Doe", "email": "john.doe@example.com"}
    result = COLLECTION.insert_one(doc)
    print(f"One record inserted: {result.inserted_id}")

def dump_all_docs():
    """
    Dumps the entire collection, document by document.
    """

    for doc in COLLECTION.find():
        print(doc)


#insert_a_doc()
dump_all_docs()
