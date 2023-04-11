import time
import threading
from threading import Thread
import random

class Node:
    def __init__(self, id):
        self.id = id
        self.timestamp = 0
        self.message = None 
        self.promise_count = 0
        self.accept_count = 0
        self.peers = []
        self.lock = threading.Lock()
    
    def set_peers(self, peers):
        self.peers = peers

    def status(self):
        print("id: " + str(self.id))
        print("timestamp: " + str(self.timestamp))
        print("promise_count: " + str(self.promise_count))
        print("accept_count: " + str(self.accept_count))
        print("message: " + str(self.message))

    def propose_value(self, message):
        self.timestamp += 1
        self.accept_count = 0
        self.message = message
        self.promise_count = 0
        prepare_message = {
            'type': 'propose',
            'timestamp': self.timestamp,
            'sender': self.id
        }
        print(f"Node {self.id} proposed value {message}\n")
        self.send_propose_messages(prepare_message)
    
    def send_propose_messages(self, message):
        for peer in self.peers:
            print(f"Node {self.id} sent propose message to Node {peer.id} with timestamp {message['timestamp']} \n")
            message['timestamp'] += 1
            self.timestamp = message['timestamp']
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
            # thread.join()
    
    def send_message(self, peer, message):
        time.sleep(random.uniform(1, 0.1))
        # print(peer)
        peer.receive_message(message)
    
    def receive_message(self, message):
        with self.lock:
            if message['type'] == 'propose':
                self.handle_propose_message(message)
            elif message['type'] == 'promise':
                self.handle_promise_message(message)
            elif message['type'] == 'accept':
                self.handle_accept_message(message)
            elif message['type'] == 'commit':
                self.handle_commit_message(message)
    
    def handle_propose_message(self, message):
        if message['timestamp'] >= self.timestamp:
            self.timestamp = message['timestamp']+1
            response = {'type': 'promise', 'timestamp': self.timestamp, 'sender': self.id}
            self.send_message(nodes[message['sender']], response)
            print(f"Node {self.id} handled propose message with timestamp {message['timestamp']} \n")
            self.status()
        else:
            return
        
    def handle_promise_message(self, message):
        if message['timestamp'] >= self.timestamp:
            self.promise_count += 1
            self.timestamp = message['timestamp']+1            
            if self.promise_count > len(self.peers) // 2:
                print(f"Node {self.id} handled promise message with timestamp {message['timestamp']} \n")
                self.send_accept_messages(message)
                self.status()
            else:
                return

    def send_accept_messages(self, message):
        # message = {'type': 'accept', 'timestamp': self.timestamp, 'message': self.message, 'sender': self.id}
        for peer in self.peers:
            print(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            message['timestamp'] += 1
            self.timestamp = message['timestamp']
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
            # thread.join()
        
    def handle_accept_message(self, message):
        if message['timestamp'] >= self.timestamp:
            self.timestamp = message['timestamp']+1
            print(f"Node {self.id} handled accept message with timestamp {message['timestamp']} \n")
            message['type'] = 'commit'
            self.send_message(nodes[message['sender']], message)
            self.status()

    def handle_commit_message(self, message):
        if message['timestamp'] >= self.timestamp:
            self.accept_count += 1
            self.timestamp = message['timestamp']+1
            if self.accept_count > len(self.peers) // 2:
                print(f'Node {self.id} agreed on value: {self.message}\n')





if __name__ == '__main__':

    nodes = []
    num_nodes = 4

    for i in range(num_nodes):
        nodes.append(Node(i))

    for i in range(len(nodes)):
        nodes[i].set_peers(nodes[:i] + nodes[i+1:])

    nodes[0].propose_value('hello world')