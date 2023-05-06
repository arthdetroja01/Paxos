import socket
import json
import time
import threading
from threading import Thread
import random
import pickle

client_list = []
total = 0
logs = []
timestamp = 0

class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.message = None 
        self.promise_count = 0
        self.accept_count = 0
        self.peers = []
        self.accepted = False
        self.commited = False
        self.lock = threading.Lock()
    
    def set_peers(self, peers):
        self.peers = peers
        return

    def propose_value(self, message):
        global logs
        global timestamp
        self.accepted = False
        self.commited = False
        timestamp += 1
        self.accept_count = 0
        self.message = message
        self.promise_count = 0
        prepare_message = {
            'type': 'propose',
            'timestamp': timestamp,
            'sender_ip': self.ip,
            'sender_port': self.port,
        }
        
        logs.append(f"Node {self.ip} proposed value {message}\n")
        
        self.send_propose_messages(prepare_message)
        return

    def send_propose_messages(self, message):
        global logs
        for peer in self.peers:
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
        return

    def send_message(self, peer, message):
        peer_ip = peer.ip
        peer_port = peer.port
        self.socket.sendto(json.dumps(message).encode(), (peer_ip, peer_port))
        return
    
    def receive_message(self, message):
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
    
    def receive_propose_message(self, message):
        global logs
        if message['timestamp'] >= timestamp:
            promise_message = {
                'type': 'promise', 
                'timestamp': timestamp, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            self.send_message(nodes[message['sender_ip']], promise_message)
        return
    
    def receive_promise_message(self, message):
        global logs
        if self.accepted:
            return 
        cnt = self.promise_count
        cnt += 1
        self.promise_count = cnt
        
        accept_message = {
            'type': 'accept', 
            'timestamp': timestamp, 
            'sender_ip': self.ip,
            'sender_port': self.port
        }
            
        if self.promise_count >= (total)//2:  
            self.accepted = True
            self.send_accept_messages(accept_message)
        return 
    
    def send_accept_messages(self, message):
        global logs
        for peer in self.peers:
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
        return

    def receive_accept_message(self, message):
        global logs
        if message['timestamp'] >= timestamp:
            accepted_message = {
                'type': 'accepted', 
                'timestamp': timestamp, 
                'sender_ip': self.ip,
                'sender_port': self.port
            }
            self.send_message(nodes[message['sender_ip']] , accepted_message)
        return

    def receive_accepted_message(self, message):  
        global logs     
        if self.commited:
            return
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
        for peer in self.peers:
            thread = Thread(target=self.send_message, args=(peer, {'type': 'commit', 'message': message}))
            thread.start()
        logs.append(f"Node {self.ip} committed value {message['message']}\n")
        return

    def receive_commit_message(self, message):
        global logs
        logs.append(f"Node {self.ip} received commit message for value {message['message']}\n")
        return

def create_nodes():
    global client_list
    global nodes
    global total
    global logs

    nodes = {
        '127.0.0.1': Node('127.0.0.1', 1234),
        '127.0.0.2': Node('127.0.0.1', 1235),
        '127.0.0.3': Node('127.0.0.1', 1236),
        '127.0.0.4': Node('127.0.0.1', 1237),
        '127.0.0.5': Node('127.0.0.1', 1238),
        '127.0.0.6': Node('127.0.0.1', 1239),
    }

    for node in nodes.values():
        node.set_peers([peer for peer in nodes.values() if peer != node])
        client_list.append(node.socket)
        node.socket.bind((node.ip, node.port))
        thread = Thread(target=listen_client, args=(node,))
        thread.start()

    total = len(nodes)

    return 

def listen_client(node):
    global logs
    while True:
        data, addr = node.socket.recvfrom(1024)
        message = json.loads(data.decode())
        node.receive_message(message)
    return

if __name__ == '__main__':
    create_nodes()

    node = nodes['127.0.0.1']

    while True:
        input_value = input("Enter a value to propose or type 'exit' to quit: ")
        if input_value == 'exit':
            break
        node.propose_value(input_value)
    print(logs)
    logs_pickle = pickle.dumps(logs)
    # with open('logs.pickle', 'wb') as f:
    #     f.write(logs_pickle)