import socket

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
    print(signal_str)
    # # Define actions based on the signal string
    # if signal_str == 'start':
    #     print("Starting the process...")
    #     # Implement your starting logic here
    # elif signal_str == 'stop':
    #     print("Stopping the process...")
    #     # Implement your stopping logic here
    # else:
    #     print(f"Unknown signal: {signal_str}")

# Run the receiver
if __name__ == "__main__":
    start_receiver()
