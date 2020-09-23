import socket
from _thread import *
import threading
import queue
import time
import sys
import pickle
import json
import random

from helpers import *

lock = threading.Lock()

processes = {}
connections = {}
hasConnection = {str(id): False for id in range(5)}

MONEY = 100
BCHAIN = LinkedList()
transactions = []

promises = []
accept_replies = 0

BallotNum = [0, "0", 0]
myVal = None

AcceptNum = [0, "0", 0]
AcceptVal = None


def checkState():
    print(MONEY, BCHAIN, transactions, promises, accept_replies, BallotNum, myVal, AcceptNum, AcceptVal)


def send(dest_process_id, msg):
    if hasConnection[dest_process_id]:
        connections[dest_process_id].sendall(pickle.dumps(msg))
        print("sent")
        return True
    return False


def new_conn_listener(connections, server_sock):
    server_sock.listen(5)
    while True:
        stream, addr = server_sock.accept()
        msg = pickle.loads(stream.recv(1024))
        p_id = msg["src"]
        connections[p_id] = stream
        start_new_thread(listener, (p_id,))
        hasConnection[p_id] = True
        send(p_id, {"type":"BCHAIN", "BCHAIN":BCHAIN})


def paxos_queue():
    while True:
        if len(transactions) > 0:
            paxos()


def listener(p_id):
    global promises, accept_replies, AcceptNum, AcceptVal, BCHAIN, BallotNum, hasConnection, MONEY

    while True:
        try:
            data = connections[p_id].recv(1024)
        except OSError:
            break
        if not data:
            break
        msg = pickle.loads(data)
        
        print(msg)

        # startup
        if msg["type"] == "BCHAIN":
            if BCHAIN.length < msg["BCHAIN"].length:
                walker = msg["BCHAIN"].head
                while walker:
                    for transaction in walker.block[0]:
                        if transaction[1] == process_id:
                            with lock: MONEY += transaction[2]
                    walker = walker.next
                BCHAIN.appendAll(msg["BCHAIN"], BCHAIN.length)
        
        # from a follower (Propose)
        if msg["type"] == "promise":
            promises.append(msg)
            
        if msg["type"] == "accepted":
            accept_replies += 1
        
        # from a leader (Prepare)
        if msg["type"] == "prepare":
            promise(msg)
            
        if msg["type"] == "accept":
            accepted(msg)
            
        if msg["type"] == "decide":
            print("decision " + str(msg["myVal"]))
            BCHAIN.append(Node([msg["myVal"], msg["nonce"], msg["prev_hash"]]))
            AcceptNum = [0, "0", 0]
            AcceptVal = None
            BallotNum = [0, "0", 0]
            txns = msg["myVal"]
            for t in txns:
                if t[1] == process_id:
                    with lock: MONEY += t[2]
            
        
        if msg["type"] == "failLink":
            hasConnection[p_id] = False
        
        if msg["type"] == "fixLink":
            hasConnection[p_id] = True
        
     
# ------------ leader ------------
def prepare():
    global BallotNum
    print('prepare stage')
    
    forward_bal = 0
    with lock:
        time.sleep(5)
        BallotNum[0] += 1
        BallotNum[1] = process_id
        BallotNum[2] = BCHAIN.length

        forward_bal = BallotNum

        for p_id in connections:
            send(p_id, {
                "type":"prepare",
                "BallotNum":BallotNum,
                "src_id":process_id,
                })
    
    start = time.time()
    while len(promises) < 2 and time.time() - start < 15:
        pass
    
    print("\t\tpromises: " + str(promises))
    no_maj = (len(promises) < 2)
    
    return forward_bal, no_maj
     

def accept(bal):
    global myVal, transactions
    print('accept stage')
    
    vals = [p["AcceptVal"] for p in promises]
    non_empty_vals = [v for v in vals if v != None]
    
    if len(non_empty_vals) != 0:
        bals = [p["AcceptNum"] for p in promises]
        myVal = vals[bals.index(max(bals))]
        my_trans = False
    else:
        myVal = transactions[:]
        transactions = []
        my_trans = True
    
    time.sleep(5)
    # send to every other process
    for p_id in connections:
        send(p_id, {
            "type":"accept",
            "BallotNum":bal,
            "myVal":myVal,
            "src_id":process_id,
            })
    
    start = time.time()
    while accept_replies < 2 and time.time() - start < 20:
        pass
        
    no_maj = accept_replies < 2
    
    return bal, no_maj, my_trans
    
    
def decide(bal, nonce, prev_hash):
    print('decide stage')
    time.sleep(5)
    for p_id in connections:
        send(p_id, {
            "type":"decide",
            "BallotNum":bal,
            "myVal":myVal,
            "src_id":process_id,
            "nonce":nonce,
            "prev_hash":prev_hash
            })
# --------------------------------

# ------------ follower ------------
def promise(msg):
    global BallotNum
    print('promise stage')
    
    bal = msg["BallotNum"]
    with lock:
        # bal[2] < BallotNum[2]
        if bal[2] < BCHAIN.length:
            send(msg["src_id"], {"type":"BCHAIN", "BCHAIN":BCHAIN})
        elif bal >= BallotNum:
            time.sleep(5)
            BallotNum = bal
            send(msg['src_id'], {
                "type":"promise",
                "bal":bal,
                "AcceptNum":AcceptNum,
                "AcceptVal":AcceptVal,
                })


def accepted(msg):
    global AcceptNum, AcceptVal
    print('accepted stage')
    
    bal = msg["BallotNum"]
    with lock:
        if bal[2] < BCHAIN.length:
            send(msg["src_id"], {"type":"BCHAIN", "BCHAIN":BCHAIN})
        elif bal >= BallotNum:
            AcceptNum = bal
            AcceptVal = msg["myVal"]
            time.sleep(5)
            send(msg['src_id'], {
                "type":"accepted",
                "bal":bal,
                "AcceptVal":msg["myVal"],
                })
# ----------------------------------

def paxos():
    global transactions, BallotNum, promises, accept_replies, AcceptNum, AcceptVal, myVal, BCHAIN
    time.sleep(10 + random.random()*5)
    print('PAXOS START')

    promises = []
    accept_replies = 0
    depth = BCHAIN.length

    # get promises
    forward_bal, no_maj = prepare()
    print("propose: ", forward_bal, "\tnoMaj", no_maj)
    
    if no_maj:
        return

    # get accepts
    myVal = transactions[:]
    accept_bal, no_maj, my_trans = accept(forward_bal)
    print("accept: ", accept_bal, "\tnoMaj", no_maj)
    
    if no_maj:
        if my_trans:
            transactions = myVal + transactions
        return

    # calculate nonce + hash
    if BCHAIN.length == 0:
        prev_hash = int(hash(["~"]), 16)
    else:
        prev_block = BCHAIN.tail.block
        prev_hash = int(hash([prev_block[0], prev_block[1], prev_block[2]]), 16)
    
    nonce = 0
    h = 5
    while h % 10 > 4:
        nonce = random.random() * 256
        print(nonce)
        h = int(hash([transactions, nonce, prev_hash]), 16)

    print("Nonce: " + str(nonce) + "\tHash: " + str(h))
    
    # send out decide
    decide(accept_bal, nonce, prev_hash)
    BCHAIN.append(Node([myVal, nonce, prev_hash]))
    
    AcceptNum = [0, "0", 0]
    AcceptVal = None
    BallotNum = [0, "0", 0]
    


process_id = sys.argv[1]
processes, connections, server_sock, state = setup(process_id)

if len(state) > 0:
    MONEY = state["MONEY"]
    BCHAIN = state["BCHAIN"]
    transactions = state["transactions"]
    promises = state["promises"]
    accept_replies = state["accept_replies"]
    BallotNum = state["BallotNum"]
    myVal = state["myVal"]
    AcceptNum = state["AcceptNum"]
    AcceptVal = state["AcceptVal"]

# listener thread for other processes
for p_id in connections:
    hasConnection[p_id] = True
    start_new_thread(listener, (p_id,))
# new connection listener for revived processes
start_new_thread(new_conn_listener, (connections, server_sock))
# paxos queue listener
start_new_thread(paxos_queue, ())

while True:
    print("\n\n----------- Menu -----------")
    option = input("0 Money Transfer\n1 Fail Link\n2 Fix Link\n3 Fail Process\n4 Blockchain\n5 Balance\n6 Queue\n7 Connections\n8 Check State\n")
    if option == "0":
        transaction = [process_id, 0, 0]
        transaction[1] = input("dest_id: ")
        transaction[2] = float(input("balance: "))
        if transaction[2] <= MONEY:
            MONEY -= transaction[2]
            transactions.append(transaction)
        else:
            print("[ERR] Insufficient funds")
    elif option == "1":
        p = input("Process ID: ")
        failLink(connections, hasConnection, p)
    elif option == "2":
        p = input("Process ID: ")
        fixLink(connections, hasConnection, p)
    elif option == "3":
        state = {
            "MONEY":MONEY,
            "BCHAIN":BCHAIN,
            "transactions":transactions,
            "promises":promises,
            "accept_replies":accept_replies,
            "BallotNum":BallotNum,
            "myVal":myVal,
            "AcceptNum":AcceptNum,
            "AcceptVal":AcceptVal
        }
        failProcess(state, process_id, connections, hasConnection)
    elif option == "4":
        print(BCHAIN)
    elif option == "5":
        print("$" + str(float(MONEY)))
    elif option == "6":
       print(transactions)
    elif option == "7":
        print([p_id for p_id in connections if hasConnection[p_id]])
    else:
        checkState()

    

