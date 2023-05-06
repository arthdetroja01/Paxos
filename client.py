import socket
import json
import time
import threading
from threading import Thread
import random
import pickle

"""
1. When a client.py is launched, it should send a message to all the other clients in the 
   system to let them know that it has joined the system and add it in the list of clients.

2. Each of the client should have the option of giving input for the consensus.

3. As soon as it gives the input it should trigger the paxos and return the appropriate result
   to all the clients in the system.

4. The client should be able to leave the system and the other clients should be notified about
   and should be removed from the list of clients.
"""

client_list = []
total = 0
logs = []
timestamp = 0

class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        # self.timestamp = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.message = None 
        self.promise_count = 0
        self.accept_count = 0
        self.peers = []
        self.accepted = False
        self.commited = False
        # self.active_request = False
        self.lock = threading.Lock()
    
    def set_peers(self, peers):
        self.peers = peers
        return

    def status(self):
        print("address: " + str(self.address))
        print("timestamp: " + str(timestamp))
        print("promise_count: " + str(self.promise_count))
        print("accept_count: " + str(self.accept_count))
        print("message: " + str(self.message))
        return

    def propose_value(self, message):
        # self.timestamp += 1
        global logs
        global timestamp
        self.accepted = False
        self.commited = False
        timestamp += 1
        self.accept_count = 0
        self.message = message
        self.promise_count = 0
        # self.active_request = True
        prepare_message = {
            'type': 'propose',
            'timestamp': timestamp,
            'sender_ip': self.ip,
            'sender_port': self.port,
        }
        # print(f"Node {self.id} proposed value {message}\n")
        # print("Printing the logs generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print(logs)
        logs.append(f"Node {self.ip} proposed value {message}\n")
        
        self.send_propose_messages(prepare_message)
        return

    def send_propose_messages(self, message):
        global logs
        for peer in self.peers:
            # print(f"Node {self.id} sent propose message to Node {peer.id} with timestamp {message['timestamp']} \n")
            # logs.append(f"Node {self.id} sent propose message to Node {peer.id} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
        return
            # thread.join()

    def send_message(self, peer, message):
        # global logs
        peer_ip = peer.ip
        peer_port = peer.port
        self.socket.sendto(json.dumps(message).encode(), (peer_ip, peer_port))
        return
    
    def receive_message(self, message):
        # global logs
        with self.lock:
            if message['type'] == 'propose':
                self.receive_propose_message(message)
            elif message['type'] == 'promise':
                self.receive_promise_message(message)
            elif message['type'] == 'accept':
                self.receive_accept_message(message)
            elif message['type'] == 'accepted':
                self.receive_accepted_message(message)
            elif message['type'] == 'commit':
                self.receive_commit_message(message)
            else:
                print("unknown message type")
            return
            # elif message['type'] == 'commit':
            #     self.handle_commit_message(message)
    
    def receive_propose_message(self, message):
        global logs
        if message['timestamp'] >= timestamp:
            # print(f"Received the proposed message from {message['sender']} and replying promise\n")
            logs.append(f"Received the proposed message from {message['sender_ip']} and replying promise\n")
            
            promise_message = {
                'type': 'promise', 
                'timestamp': timestamp, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            self.send_message(nodes[message['sender_ip']], promise_message)
            self.socket.sendto(promise_message.encode(), (nodes[message['sender_ip']], nodes[message['sender_port']]))
        return
    
    def receive_promise_message(self, message):
        global logs
        if self.accepted:
            # print("Already accepted!!")
            return 
        # with self.lock:
        cnt = self.promise_count
        cnt += 1
        self.promise_count = cnt
        # print("*******" + str(self.promise_count) + "*******")
        # self.lock.release() 
        # print(f"Received the promise method from {message['sender']} to {self.id} and count is {self.promise_count} \n")
        logs.append(f"Received the promise method from {message['sender']} to {self.ip} and count is {self.promise_count} \n")
        
        accept_message = {
            'type': 'accept', 
            'timestamp': timestamp, 
            'sender_ip': self.ip,
            'sender_port': self.port
        }
            # print("************" + str(total) + "************")
        if self.promise_count >= (total)//2:  
            # print(f"Finally sending the accept message to all \n")  
            logs.append(f"Finally sending the accept message to all \n")
            
            self.accepted = True
            self.send_accept_messages(accept_message)
        return 
    
    def send_accept_messages(self, message):
        global logs
        # print(f"Sending accept message to all the nodes \n")
        logs.append(f"Sending accept message to all the nodes \n")
        
        for peer in self.peers:
            # print(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.ip} sent accept message to Node {peer.ip} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
            # thread.join()
        return

    def receive_accept_message(self, message):
        global logs
        # print(f"Received the accept message from {message['sender']} to {self.id} \n")
        logs.append(f"Received the accept message from {message['sender']} to {self.id} \n")
        
        if message['timestamp'] >= timestamp:
            accepted_message = {
                'type': 'accepted', 
                'timestamp': timestamp, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            self.send_message(nodes[message['sender_ip']] , accepted_message)
            self.socket.sendto(accepted_message.encode(), (nodes[message['sender_ip']], nodes[message['sender_port']]))
        return

    def receive_accepted_message(self, message):  
        global logs     
        if self.commited:
            return
        # print(f"Receiving the accepted messages to establish the quorum \n")
        logs.append(f"Receiving the accepted messages to establish the quorum \n")
        
        self.accept_count += 1
        if self.accept_count == (total)//2:
            self.commited = True
            forward_message = {
                'timestamp': message['timestamp'], 
                'message': self.message, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            self.commit(forward_message)
        return

    def commit(self, message):
        global logs
        # print(f"Quorum has been established and the message agreed upon is {message['message']}\n")
        logs.append(f"Quorum has been established and the message agreed upon is {message['message']}\n")
        
        commit_message = {
            'type': 'commit', 
            'sender_ip': self.ip, 
            'message': self.message,
            'sender_port': self.port
        }
        for peer in self.peers:
            # print(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.ip} sent accept message to Node {peer.ip} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, commit_message))
            thread.start()
        return

    def receive_commit_message(self, message):
        global logs
        # print(f"Received the commit message from {message['sender']} to {self.id} \n")
        logs.append(f"Received the commit message from {message['sender']} to {self.ip} \n")
        
        self.message = message['message']
        return 

    def listen(self, node): 
        # listens for incoming Paxos messages on a separate thread 
        while True: 
            buffer_size = 10240
            message_bytes, address = node.socket.recvfrom(buffer_size) 
            message = json.loads(message_bytes) 
            node.receive_message(message)

if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    hostname = socket.gethostname()

    # Get the IP address of the local machine
    ip_address = socket.gethostbyname(hostname)
    sock.bind((ip_address, 0))
    remote_address = (ip_address, 1234)

    message = b'Hello world'
    sock.sendto(message, remote_address)
   #  data, address = sock.recvfrom(4096)
   #  client_list = json.loads(data.decode())

    while True:
        print("Enter appropriate option:")
        print("1. Ask for consensus")
        print("2. Leave the system")
        option = int(input("Enter option: "))
        if option == 1:
            message = input("Enter message: ")
            buffer_size = 4096

            message = b'Fetch list'

            sock.sendto(message, remote_address)
            
            data, address = sock.recvfrom(buffer_size)
            data_str = data.decode()
            fields = data_str.split(';')
            client_list = json.loads(fields[0])
            time_stamp = fields[1]
            total = int(fields[2])

            # Print the individual fields
            # print(client_list)
            # print(time_stamp)
            # print(total)
            
            nodes = []
            #timestamp = 0
            cnt = 0
            ip_address, port_number = sock.getsockname()
            for ip, port in client_list:
                node = Node(ip, port)
                nodes.append(node)
                #Starts thread for each of the client
                if(ip == ip_address and port == port_number):
                    curr_node = Node(ip_address, port_number)
                    thread = Thread(target=curr_node.listen, args=(node))
                    thread.start()

            for i in range(len(nodes)):
                nodes[i].set_peers(nodes[:i] + nodes[i+1:])


        elif option == 2:
            message = b'Delete me'
            sock.sendto(message, remote_address)
            break

"""
Last done: Added the algorithm in the code. Now understand the code and make the nodes the physical clients 
and then connect them with each other.
"""