import socket


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

local_address = ('localhost', 1235)
sock.bind(local_address)

while(True):
    buffer_size = 1024
    data, address = sock.recvfrom(buffer_size)
    print(f"Received message: {data.decode()} from {address}")