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



# # Define the aggregation pipeline
# pipeline = [
#     {
#         '$addFields': {
#             'sum': {'$add': ['$number1', '$number2']}  # Calculate the sum of number1 and number2
#         }
#     },
#     {
#         '$project': {
#             'number1': 1,  # Optional: Include if you want to keep the original number
#             'number2': 1,  # Optional: Include if you want to keep the original number
#             'sum': 1       # Include the sum field
#         }
#     },
#     {
#         '$merge': {
#             'into': 'yourCollectionName',  # You can change this to another collection if needed
#             'on': '_id',  # Merge on the _id field
#             'whenMatched': 'replace',  # Replace the existing document
#             'whenNotMatched': 'insert'  # Insert if no matching document is found
#         }
#     }
# ]

# # Run the aggregation pipeline
# result = collection.aggregate(pipeline)

# # Since the output of aggregate is a cursor, you might want to iterate through
# # to perform additional operations or to simply confirm the operation
# for doc in result:
#     print(doc)  # This will typically not output anything unless there's an error or additional stages

# print("Aggregation complete.")
