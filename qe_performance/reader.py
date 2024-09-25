import time
from utils import create_client, DB_NAME, ENCRYPTED_COLLECTION
from pprint import pprint

mongo_client = create_client()

#
# Perform repeated queries against the encrypted database
#

ITERATIONS = 100000

for i in range(ITERATIONS):
    print(f"Performing query number {i + 1} of {ITERATIONS}...")

    #
    # SWITCH TO AN ENCRYPTED QUERY!
    #

    super_secret_search_string = f"{i}"

    start_time = time.time()
    results = mongo_client[DB_NAME].get_collection(ENCRYPTED_COLLECTION).find({
        "secret_string": super_secret_search_string
    })
    count = 0
    for result in results:
        pprint(result)
        count += 1
    end_time = time.time()

    elapsed = end_time - start_time
    print(f"Query and iteration over {count} results took {elapsed} ms.")


