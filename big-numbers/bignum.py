# proof_of_concept.py

import pymongo
from pymongo import MongoClient
from multiprocessing import Pool
import gmpy2
from gmpy2 import mpz
import math

# Configuration
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'big_number_db'
CHUNK_SIZE = 1000  # Number of digits per chunk
BASE = 2**32  # Radix for higher base representation

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
chunks_collection = db['number_chunks']

def store_number(number_id, number_str):
    """
    Stores a large number in MongoDB in chunks.
    """
    # Convert the number string to an integer
    number = mpz(number_str)
    # Convert the number to base BASE
    digits = []
    while number > 0:
        digits.append(int(number % BASE))
        number = number // BASE
    # Split digits into chunks
    num_chunks = math.ceil(len(digits) / CHUNK_SIZE)
    for i in range(num_chunks):
        chunk_digits = digits[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
        chunk_doc = {
            'number_id': number_id,
            'chunk_index': i,
            'data': chunk_digits,
            'base': BASE,
            'length': len(chunk_digits)
        }
        chunks_collection.insert_one(chunk_doc)
    print(f"Stored number {number_id} in {num_chunks} chunks.")

def get_number_chunks(number_id):
    """
    Retrieves all chunks of a number from MongoDB.
    """
    chunks = list(chunks_collection.find({'number_id': number_id}).sort('chunk_index', pymongo.ASCENDING))
    return chunks

def add_chunk(args):
    """
    Adds two chunks together and returns the result along with the carry.
    """
    chunk1, chunk2 = args
    data1 = chunk1['data']
    data2 = chunk2['data']
    max_length = max(len(data1), len(data2))
    result_data = []
    carry = 0

    # Pad shorter list with zeros
    data1.extend([0] * (max_length - len(data1)))
    data2.extend([0] * (max_length - len(data2)))

    # Perform addition
    for a, b in zip(data1, data2):
        total = a + b + carry
        carry = total // BASE
        result_data.append(total % BASE)

    return {
        'chunk_index': chunk1['chunk_index'],
        'data': result_data,
        'carry': carry
    }

def store_result_chunks(number_id, result_chunks):
    """
    Stores the result chunks back into MongoDB.
    """
    for chunk in result_chunks:
        chunk_doc = {
            'number_id': number_id,
            'chunk_index': chunk['chunk_index'],
            'data': chunk['data'],
            'base': BASE,
            'length': len(chunk['data']),
            'carry': chunk['carry']
        }
        chunks_collection.insert_one(chunk_doc)
    print(f"Stored result number {number_id}.")

def propagate_carry(result_chunks):
    """
    Propagates carry over across chunks sequentially.
    """
    carry = 0
    for chunk in result_chunks:
        data = chunk['data']
        # Add previous carry to the least significant digit
        data[0] += carry
        carry = 0
        # Check for new carry within this chunk
        for i in range(len(data)):
            if data[i] >= BASE:
                data[i] -= BASE
                if i + 1 < len(data):
                    data[i + 1] += 1
                else:
                    carry = 1
            else:
                break  # No further carry in this chunk
        carry += chunk['carry']
    if carry > 0:
        # Append a new chunk for the remaining carry
        result_chunks.append({
            'chunk_index': result_chunks[-1]['chunk_index'] + 1,
            'data': [carry],
            'base': BASE,
            'length': 1,
            'carry': 0
        })
    return result_chunks

def assemble_number(chunks):
    """
    Assembles the full number string from chunks.
    """
    digits = []
    for chunk in reversed(chunks):
        digits.extend(chunk['data'])
    # Convert list of digits to a single number
    number = mpz(0)
    for digit in reversed(digits):
        number = number * BASE + digit
    return number.digits()

def main():
    # Clear previous data
    chunks_collection.delete_many({})

    # Example numbers (these can be replaced with any large numbers)
    num1_str = '1' + '0' * 10000  # A 10001-digit number
    num2_str = '9' * 10000        # A 10000-digit number

    # Store numbers in MongoDB
    store_number('number1', num1_str)
    store_number('number2', num2_str)

    # Retrieve chunks
    chunks1 = get_number_chunks('number1')
    chunks2 = get_number_chunks('number2')

    # Ensure both numbers have the same number of chunks
    max_chunks = max(len(chunks1), len(chunks2))
    if len(chunks1) < max_chunks:
        # Pad with empty chunks
        for i in range(len(chunks1), max_chunks):
            chunks1.append({
                'number_id': 'number1',
                'chunk_index': i,
                'data': [0],
                'base': BASE,
                'length': 1
            })
    if len(chunks2) < max_chunks:
        for i in range(len(chunks2), max_chunks):
            chunks2.append({
                'number_id': 'number2',
                'chunk_index': i,
                'data': [0],
                'base': BASE,
                'length': 1
            })

    # Prepare arguments for parallel addition
    args = list(zip(chunks1, chunks2))

    # Perform addition in parallel
    with Pool() as pool:
        result_chunks = pool.map(add_chunk, args)

    # Propagate carry over between chunks
    result_chunks = sorted(result_chunks, key=lambda x: x['chunk_index'])
    result_chunks = propagate_carry(result_chunks)

    # Store result in MongoDB
    store_result_chunks('result_number', result_chunks)

    # Retrieve and assemble the result number
    result_chunks_db = get_number_chunks('result_number')
    result_number_str = assemble_number(result_chunks_db)

    # Print the result
    print("Addition Result:")
    print(result_number_str)

if __name__ == '__main__':
    main()
