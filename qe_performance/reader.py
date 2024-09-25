import random
import time
from utils import create_client, DB_NAME, ENCRYPTED_COLLECTION, write_line_to_csv
from pprint import pprint

mongo_client = create_client()

#
# Perform repeated queries against the encrypted database
#

ITERATIONS = 10**9
MAX_SECRET_NUMBER = 199 # based on what's being inserted

for i in range(ITERATIONS):
    print(f"Performing query number {i + 1} of {ITERATIONS}...")

    #
    # SWITCH TO AN ENCRYPTED QUERY!
    #

    search_int = random.randint(0, MAX_SECRET_NUMBER)
    super_secret_search_string = f"{search_int}"

    start_time = time.time()
    results = mongo_client[DB_NAME].get_collection(ENCRYPTED_COLLECTION).find({
        "encrypted_string": super_secret_search_string
    })
    count = 0
    for result in results:
        assert(int(result["encrypted_string"]) == search_int)
        count += 1
    end_time = time.time()

    elapsed = 100*(end_time - start_time)
    print(f"Query and iteration over {count} results took {elapsed} ms.")

    PERF_FILE = "reader_output.csv"
    # iteration number, results count, elapsed time (ms)
    write_line_to_csv(PERF_FILE, [i + 1, count, elapsed])  # save the perf data
