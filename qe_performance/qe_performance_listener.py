import socket
import time
from qe_performance_create_connection import create_client, DB_NAME, ENCRYPTED_COLLECTION
from pprint import pprint

mongo_client = create_client()

def start_receiver(host="0.0.0.0", port=3141):
    """Start the UDP server to receive signals."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    print(f"Listening for signals on {host}:{port}...")
    while True:
        data, addr = sock.recvfrom(1024)  # Buffer size is 1024 bytes
        signal_str = data.decode('utf-8')
        print(f"Received signal from {addr}: {signal_str}")
        # Act on the received signal
        handle_signal(signal_str)

def handle_signal(signal_str):
    """Handle the received signal."""
    if signal_str == "start-insert":
        handle_start_insert()
    elif signal_str == "end-insert":
        handle_end_insert()
    else:
        assert(False)  # unknown signal

start_insert_time = None
end_insert_time = None

def handle_start_insert():
    global start_insert_time
    start_insert_time = time.time()

def handle_end_insert():
    global start_insert_time, end_insert_time, mongo_client
    count = 0
    results = mongo_client[DB_NAME].get_collection(ENCRYPTED_COLLECTION).find({})
    for result in results:
        count += 1
    end_insert_time = time.time()
    print(count)

# Run the receiver
if __name__ == "__main__":
    start_receiver()
