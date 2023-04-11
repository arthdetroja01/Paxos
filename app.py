from flask import Flask, render_template, request, jsonify, Response
import time
import threading
from threading import Thread
import random


app = Flask(__name__)
timestamp = 0
total = 0

latest_message = ""

agreement_values = ['Real Madrid', 'Barcelona', 'PSG', 'Manchester United', 'Manchester City', 'Chelsea', 'Bayern Munich', 'Liverpool', 'Juventus', 'Inter']
logs = []

class Node:
    def __init__(self, id):
        self.id = id
        # self.timestamp = 0
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
        print("id: " + str(self.id))
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
            'sender': self.id
        }
        # print(f"Node {self.id} proposed value {message}\n")
        # print("Printing the logs generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print(logs)
        logs.append(f"Node {self.id} proposed value {message}\n")
        
        self.send_propose_messages(prepare_message)
        return

    def send_propose_messages(self, message):
        global logs
        for peer in self.peers:
            # print(f"Node {self.id} sent propose message to Node {peer.id} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.id} sent propose message to Node {peer.id} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
        return
            # thread.join()

    def send_message(self, peer, message):
        global logse
        time.sleep(random.uniform(1, 2))
        # print("***********"+str(peer)+"*************")
        peer.receive_message(message)
        return 
    
    def receive_message(self, message):
        global logs
        with self.lock:
            if message['type'] == 'propose':
                # print(f"received the propose message and sending it forward to {self.id}\n")
                logs.append(f"received the propose message and sending it forward to {self.id}\n")
                
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
            logs.append(f"Received the proposed message from {message['sender']} and replying promise\n")
            
            promise_message = {'type': 'promise', 'timestamp': timestamp, 'sender': self.id}
            self.send_message(nodes[message['sender']], promise_message)
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
        logs.append(f"Received the promise method from {message['sender']} to {self.id} and count is {self.promise_count} \n")
        
        accept_message = {'type': 'accept', 'timestamp': timestamp, 'sender': self.id}
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
            logs.append(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, message))
            thread.start()
            # thread.join()
        return

    def receive_accept_message(self, message):
        global logs
        # print(f"Received the accept message from {message['sender']} to {self.id} \n")
        logs.append(f"Received the accept message from {message['sender']} to {self.id} \n")
        
        if message['timestamp'] >= timestamp:
            accepted_message = {'type': 'accepted', 'timestamp': timestamp, 'sender': self.id}
            self.send_message(nodes[message['sender']] , accepted_message)
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
            forward_message = {'timestamp': message['timestamp'], 'message': self.message, 'sender': self.id}
            self.commit(forward_message)
        return

    def commit(self, message):
        global logs
        # print(f"Quorum has been established and the message agreed upon is {message['message']}\n")
        logs.append(f"Quorum has been established and the message agreed upon is {message['message']}\n")
        
        commit_message = {'type': 'commit', 'sender': self.id, 'message': self.message}
        for peer in self.peers:
            # print(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            logs.append(f"Node {self.id} sent accept message to Node {peer.id} with timestamp {message['timestamp']} \n")
            
            thread = Thread(target=self.send_message, args=(peer, commit_message))
            thread.start()
        return

    def receive_commit_message(self, message):
        global logs
        # print(f"Received the commit message from {message['sender']} to {self.id} \n")
        logs.append(f"Received the commit message from {message['sender']} to {self.id} \n")
        
        self.message = message['message']
        return 

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/logs')
def get_logs():
    # temp = logs
    # logs.clear()
    return Response(logs)



@app.route('/', methods=['POST', "GET"])
def handle_data():
    while(True):
        # print(request)
        # print(data)
        data = request.form.get('dataInput')
        # print(type(data))
        nodes[int(data)-1].propose_value(agreement_values[int(data)-1])
        return render_template('index.html')
    

if __name__ == '__main__':
    nodes = []
    num_nodes = 10
    total = num_nodes
    #timestamp = 0
    for i in range(num_nodes):
        node = Node(i)
        nodes.append(node)
        
    for i in range(len(nodes)):
        nodes[i].set_peers(nodes[:i] + nodes[i+1:])
    
    app.run(debug = True)