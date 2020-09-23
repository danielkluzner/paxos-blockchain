import json
import socket
import pickle
import sys
from _thread import *
import hashlib
import os.path as path


class Node:
    def __init__(self, block):
        self.block = block
        self.next = None
        self.prev = None

    def __repr__(self):
        prev_pointer = hex(id(self.prev)) if self.prev else "None"
        return "[prev_ptr: " + str(prev_pointer) + ", block: " + str(self.block) + "]"

class LinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.length = 0
    
    def append(self, node):
        if self.tail == None:
            self.head = self.tail = node
        else:
            self.tail.next = node
            temp = self.tail
            self.tail = node
            node.prev = temp
        self.length += 1
    
    def appendAll(self, other_list, startIndex):
        count = 0
        walker = other_list.head
        while count < startIndex and walker:
            walker = walker.next
            count += 1
        
        if not walker:
            return
        while walker:
            self.append(Node(walker.block))
            walker = walker.next

    def __repr__(self):
        walker = self.head
        nodes = []
        while walker:
            nodes.append(str(walker))
            walker = walker.next
        nodes.append("None")
        return "\n->\n".join(nodes)


def failLink(connections, hasConnection, dest_process_id):
    connections[dest_process_id].sendall(pickle.dumps({
        "type":"failLink"
        }))
    hasConnection[dest_process_id] = False


def fixLink(connections, hasConnection, dest_process_id):
    connections[dest_process_id].sendall(pickle.dumps({
        "type":"fixLink"
        }))
    hasConnection[dest_process_id] = True
    
    
def failProcess(state, process_id, connections, hasConnection):
    for p_id in connections:
        if hasConnection[p_id]:
            failLink(connections, hasConnection, p_id)
    
    pickle.dump(state, open("p"+process_id+"_save.pkl", "wb"))
    sys.exit()


def setup(process_id):

    # Step 1: Load previous state if exist
    state_file = "p"+process_id+"_save.pkl"
    if path.exists(state_file):
        state = pickle.load(open(state_file, "rb"))
    else:
        state = {}

    # Step 2: Make connections
    processes = {}
    connections = {}

    # read all process IP/PORT from config
    with open('config.txt', 'r') as json_file:
        data = json.load(json_file)
        for p_id, addr in data.items():
            processes[p_id] = {'IP': addr[0], 'PORT': addr[1]}
    
    TCP_IP = processes[process_id]['IP']
    TCP_PORT = processes[process_id]['PORT']

    # connect to IP/PORT
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((TCP_IP, TCP_PORT))

    # establish client sockets
    for p_id, addr in processes.items():
        if p_id != process_id:
            try:
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_sock.connect((addr['IP'], addr['PORT']))
                client_sock.sendall(pickle.dumps({"src": process_id}))
                connections[p_id] = client_sock
            except:
                pass
                #print("[ERR] Cannot connect to process " + p_id)
            
    return processes, connections, server_sock, state


def hash(inputs):
    h = hashlib.sha256()
    for i in inputs:
        h.update(str(i).encode())
    return h.hexdigest()
    
