import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
hostname = socket.gethostname()

# Get the IP address of the local machine
ip_address = socket.gethostbyname(hostname)
sock.bind((ip_address, 0))
print(ip_address)
remote_address = ("192.168.137.1", 1234)

message = b'Hello world'
sock.sendto(message, remote_address)