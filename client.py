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
remote_address = ('0.0.0.0', 0)
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
        # print("Setting peers")
        self.peers = peers
        return

    def status(self):
        print("address: " + str(self.ip))
        print("timestamp: " + str(timestamp))
        print("promise_count: " + str(self.promise_count))
        print("accept_count: " + str(self.accept_count))
        print("message: " + str(self.message))
        return

    def propose_value(self, message):
        # self.timestamp += 1
        # print("Proposing value")
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
        # print(f"Node {self.ip} proposed value {message}\n")
        # print("Printing the logs generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print(logs)
        logs.append(f"Node {self.ip} proposed value {message}\n")
        
        self.send_propose_messages(prepare_message)
        return

    def send_propose_messages(self, message):
        # print("Sending propose messages")
        global logs
        for peer in self.peers:
            # print(f"Node {self.ip} sent propose message to Node {peer[1]} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.ip} sent propose message to Node {peer[0]} with timestamp {message['timestamp']} \n")
            print(f"Sending the message to {peer[0]} and {peer[1]} and the message is {message}")
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
        return
            # thread.join()

    def send_message(self, peer, message):
        print("Sending message")
        global logs
        peer_ip = peer[0]
        peer_port = peer[1]
        print("Printing the peers")
        print(peer[0], peer[1])
        self.socket.sendto(json.dumps(message).encode(), (peer_ip, peer_port))
        return
    
    def receive_message(self, message):
        # print("Receiving message")
        global logs
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
        # print("Receiving propose message")
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
            # self.send_message(nodes[message['sender_ip']], promise_message)
            print(nodes[message['sender_ip']], nodes[message['sender_port']])
            self.socket.sendto(json.dumps(promise_message).encode(), (nodes[message['sender_ip']], nodes[message['sender_port']]))
        return
    
    def receive_promise_message(self, message):
        # print("Receiving promise message")
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
        # print(f"Received the promise method from {message['sender']} to {self.ip} and count is {self.promise_count} \n")
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
        # print("Sending accept messages")
        global logs
        # print(f"Sending accept message to all the nodes \n")
        logs.append(f"Sending accept message to all the nodes \n")
        
        for peer in self.peers:
            # print(f"Node {self.ip} sent accept message to Node {peer[1]} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.ip} sent accept message to Node {peer[0]} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
            # thread.join()
        return

    def receive_accept_message(self, message):
        # print("Receiving accept message")
        global logs
        # print(f"Received the accept message from {message['sender']} to {self.ip} \n")
        logs.append(f"Received the accept message from {message['sender']} to {self.ip} \n")
        
        if message['timestamp'] >= timestamp:
            accepted_message = {
                'type': 'accepted', 
                'timestamp': timestamp, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            # self.send_message(nodes[message['sender_ip']] , accepted_message)
            self.socket.sendto(json.dumps(accepted_message).encode(), (nodes[message['sender_ip']], nodes[message['sender_port']]))
        return

    def receive_accepted_message(self, message):  
        # print("Receiving accepted message")
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
        # print("Committing")
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
            # print(f"Node {self.ip} sent accept message to Node {peer[1]} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.ip} sent accept message to Node {peer[0]} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, commit_message))
            thread.start()
        return

    def receive_commit_message(self, message):
        print("Receiving commit message")
        global logs
        # print(f"Received the commit message from {message['sender']} to {self.ip} \n")
        logs.append(f"Received the commit message from {message['sender']} to {self.ip} \n")
        
        self.message = message['message']
        return 

    def listen(self, node): 
        global client_list
        global remote_address
        global total
        global timestamp
        # listens for incoming Paxos messages on a separate thread 
        # print("##################### Started the thread #####################")
        # print(node.ip, node.port)

        #Starting the background thread to listen to the incoming messages
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.ip, 0))
        while True: 
            buffer_size = 1024
            # print("##################### Listening #####################")            
            message_bytes, address = sock.recvfrom(buffer_size) 

            #IF the message is from the server then do the operation according to it so that the queue can be cleared and 
            #further messages can be processed.
            if address[0] == remote_address[0] and address[1] == remote_address[1]:
                data_str = message_bytes.decode()
                fields = data_str.split(';')
                new_client_list = json.loads(fields[0])
                print(client_list)
                timestamp = int(fields[1])
                total = int(fields[2])

                for ip, port in new_client_list:
                    if (ip, port) in client_list:
                        continue
                    else:
                        client_list.append((ip, port))
                        node = Node(ip, port)
                        client_list.append((ip, port))                        
                        thread = Thread(target=node.listen, args=(node),)
                        thread.start()
                peers_list = client_list
                peers_list.remove((self.ip, self.port))
                self.peers = peers_list
                continue
            elif node.ip != address[0] or node.port != address[1]:
                #If the node ip does not match with the incoming messages address then ignore it as some other thread will pick it up.
                continue
            else:
                #If the message is sent by the intended address then decode it and then forward it to the receive message function
                message = json.loads(message_bytes.decode())
                # print("********************Printing the message: ") 
                # print(message)
                self.receive_message(message)

if __name__ == '__main__':
    # global remote_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    hostname = socket.gethostname()

    # Get the IP address of the local machine
    ip_address = socket.gethostbyname(hostname)
    sock.bind((ip_address, 0))

    #set the IP address of the server
    remote_address = (ip_address, 1234)

    #Send hello world to the server to join the client list on the server
    message = b'Hello world'
    sock.sendto(message, remote_address)
   #  data, address = sock.recvfrom(4096)
   #  client_list = json.loads(data.decode())

   #First fetch list request to get the list of client already on the network
    message = b'Fetch list'

    sock.sendto(message, remote_address)
    buffer_size = 4096    
    data, address = sock.recvfrom(buffer_size)
    data_str = data.decode()
    # print("**********" + data_str + "**********")
    fields = data_str.split(';')
    client_list = json.loads(fields[0])
    print(client_list)
    timestamp = int(fields[1])
    total = int(fields[2])
    nodes = []
    #timestamp = 0
    cnt = 0
    ip_address, port_number = sock.getsockname()

    #make the nodes of the clients received from the server
    for ip, port in client_list:
        node = Node(ip, port)
        nodes.append(node)
        #Starts thread for each of the client
        # if(ip == ip_address and port == port_number):
        # curr_node = Node(ip_address, port_number)
        # thread_started = threading.Event()
        thread = Thread(target=node.listen, args=(node,))
        thread.start()
        # thread_started.wait()
    
    #Set peers for the current node
    peers_list = []
    for ip, port in client_list:
        peers_list.append((ip, port))
    peers_list.remove((ip_address, port_number))
    print("Printing the client list: ///////////////////////////////////////")
    print(peers_list)
    curr_node = Node(ip_address, port_number)
    curr_node.set_peers(peers_list)

    while True:        

        print("Enter appropriate option:")
        print("1. Ask for consensus")
        print("2. Leave the system")
        option = int(input("Enter option: "))
        if option == 1:       
            message = b'Fetch list'
            sock.sendto(message, remote_address)    

            message = input("Enter message: ")                   

            # Print the individual fields
            # print(client_list)
            # print(timestamp)
            # print(total)
            
            #Propose the value and start the paxos algortihm
            curr_node.propose_value(message)
            print(logs)

        elif option == 2:
            #Remove the node from the server client list.
            message = b'Delete me'
            sock.sendto(message, remote_address)
            break

"""
Last done: Connected the nodes of differnt address, now need to resolve the threads of different nodes that they working
perfectly by sending and receiving the messages 
"""