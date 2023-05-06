import socket

# Create a UDP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind the socket to a local address and port
# local_address = ('localhost', 1234)
# sock.bind(local_address)

sock.bind(('0.0.0.0', 0))

# Send a message to a remote device
remote_address = ('localhost', 1235)
message = b'Hello, world!'
sock.sendto(message, remote_address)

# Receive a message from a remote device

