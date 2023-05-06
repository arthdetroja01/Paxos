import socket
import json

client_list = []
time_stamp = 0
total = 0

if __name__ == '__main__':

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('localhost', 1234))

    while True:
        buffer_size = 1024
        data, address = sock.recvfrom(buffer_size)
        print(f"Received message: {data.decode()} from {address}")
        msg = data.decode()

        if msg == "Hello world":
            client_list.append(address)
            encoded_client_list = json.dumps(client_list).encode()
            total += 1
            # for client in client_list:
            #     sock.sendto(encoded_client_list, client)

        elif msg == "Fetch list":
            encoded_client_list = json.dumps(client_list).encode()
            time_stamp += 1

            data = encoded_client_list + b';' + str(time_stamp).encode() + b';' + str(total).encode()
            sock.sendto(data, address)
            # sock.sendto(encoded_client_list, address)
            # sock.sendto((str(time_stamp)).encode(), address)
            # sock.sendto((str(total)).encode(), address)

        elif msg == "Delete me":
            total -= 1
            client_list.remove(address)
        
        else:
            print("Invalid message")