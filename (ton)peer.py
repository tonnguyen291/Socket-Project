import socket
import threading
import json
import time
import sys
import csv
import math
from collections import deque

# Global Peer State
peer_name = ""
manager_ip = "127.0.0.1"
manager_port = 15000
peer_m_port = 0
peer_p_port = 0

m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # manager comm
p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # peer comm

# DHT structures
registered = False
dht_exists = False
identifier = -1
ring_size = -1
three_tuple_data = []
right_neighbor = None
local_table = []
global_table = []

# Flags
tearing_down = False
leaving = False
joining = False

# Helpers
def is_prime(n):
    if n < 2:
        return False
    if n in (2, 3):
        return True
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

def next_prime_after(n):
    while True:
        n += 1
        if is_prime(n):
            return n

# Listener Thread
def listener():
    global registered, dht_exists, identifier, ring_size
    global right_neighbor, three_tuple_data, local_table, global_table
    global tearing_down, leaving, joining

    while True:
        try:
            data, addr = p_socket.recvfrom(4096)
            message = json.loads(data.decode())

            if message.get("status") == "SUCCESS":
                if message.get("command-type") == "register":
                    registered = True
                    print(f"[Peer-{peer_name}] Registration successful.")
                elif message.get("command-type") == "setup-dht":
                    handle_dht_setup(message)
                elif message.get("command-type") == "teardown-dht":
                    handle_teardown()
                elif message.get("command-type") == "query-dht":
                    print("[Query Result]", message)
            elif message.get("status") == "PEER-MESSAGE":
                if message.get("command-type") == "set-id":
                    handle_set_id(message)
                elif message.get("command-type") == "store":
                    handle_store(message)
                elif message.get("command-type") == "teardown":
                    handle_peer_teardown(message)
                elif message.get("command-type") == "reset-id":
                    handle_reset_id(message)
                elif message.get("command-type") == "rebuild-dht":
                    populate_dht()
            elif message.get("status") == "FAILURE":
                print("[Manager Response] FAILURE:", message.get("message"))
            else:
                print("[Unknown Message]", message)

        except Exception as e:
            print(f"[Listener Error]: {e}")

# Peer Message Handlers
def handle_dht_setup(msg):
    global identifier, ring_size, three_tuple_data, right_neighbor

    print(f"[Peer-{peer_name}] Setting up DHT...")
    members = msg['members']
    ring_size = msg['size']
    three_tuple_data = members

    for idx, member in enumerate(members):
        if member[0] == peer_name:
            identifier = idx
            right_neighbor = members[(idx + 1) % ring_size]
            break

    if identifier == 0:
        populate_dht()
        send_dht_complete()

def handle_set_id(msg):
    global identifier, ring_size, three_tuple_data, right_neighbor

    identifier = msg['identifier']
    ring_size = msg['ring_size']
    three_tuple_data = msg['3-tuple-data']
    right_neighbor = three_tuple_data[(identifier + 1) % ring_size]

def handle_store(msg):
    if msg['id'] == identifier:
        local_table.append(msg['entry'])
    else:
        forward_to_neighbor(msg)

def handle_teardown():
    global local_table, dht_exists

    print(f"[Peer-{peer_name}] Teardown received.")
    local_table.clear()
    dht_exists = False
    send_teardown_complete()

def handle_peer_teardown(msg):
    global tearing_down
    local_table.clear()
    if tearing_down:
        send_teardown_complete()
    else:
        forward_to_neighbor(msg)

def handle_reset_id(msg):
    pass  # Implement later if you do leave/join fully

# Core Functions
def send_manager(payload):
    m_socket.sendto(json.dumps(payload).encode(), (manager_ip, manager_port))

def send_peer(address, payload):
    p_socket.sendto(json.dumps(payload).encode(), address)

def register(name, addr, m_port, p_port):
    payload = {
        "command": "register",
        "peer_name": name,
        "IPv4_address": addr,
        "m_port": m_port,
        "p_port": p_port
    }
    send_manager(payload)

def setup_dht(name, n, year):
    payload = {
        "command": "setup-dht",
        "peer_name": name,
        "n": n,
        "YYYY": year
    }
    send_manager(payload)

def query_dht(name):
    payload = {
        "command": "query-dht",
        "peer_name": name
    }
    send_manager(payload)

def teardown_dht():
    payload = {
        "command": "teardown-dht",
        "peer_name": peer_name
    }
    send_manager(payload)

def send_dht_complete():
    payload = {
        "command": "dht-complete",
        "peer_name": peer_name
    }
    send_manager(payload)

def send_teardown_complete():
    payload = {
        "command": "teardown-complete",
        "peer_name": peer_name
    }
    send_manager(payload)

def populate_dht():
    print(f"[Peer-{peer_name}] Populating DHT...")
    try:
        filename = f"./CSVFiles/details-1950.csv"
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            entries = list(reader)

        prime_size = next_prime_after(2 * len(entries))

        for entry in entries:
            pos = int(entry[0]) % prime_size
            id_target = pos % ring_size
            if id_target == identifier:
                local_table.append(entry)
            else:
                forward_entry(entry, id_target)
        
        print(f"[Peer-{peer_name}] Stored {len(local_table)} entries locally.")

    except Exception as e:
        print(f"[Populate DHT Error]: {e}")

def forward_entry(entry, id_target):
    message = {
        "status": "PEER-MESSAGE",
        "command-type": "store",
        "id": id_target,
        "entry": entry
    }
    addr = (right_neighbor[1], int(right_neighbor[2]))
    send_peer(addr, message)

def forward_to_neighbor(msg):
    addr = (right_neighbor[1], int(right_neighbor[2]))
    send_peer(addr, msg)

# Main
def main():
    global peer_name, peer_m_port, peer_p_port

    if len(sys.argv) != 5:
        print("Usage: python peer.py <peer_name> <m_port> <p_port> <manager_port>")
        sys.exit(1)

    peer_name = sys.argv[1]
    peer_m_port = int(sys.argv[2])
    peer_p_port = int(sys.argv[3])
    global manager_port
    manager_port = int(sys.argv[4])

    local_ip = "127.0.0.1"

    # Bind sockets
    m_socket.bind((local_ip, peer_m_port))
    p_socket.bind((local_ip, peer_p_port))

    # Start listening
    threading.Thread(target=listener, daemon=True).start()

    # Register
    register(peer_name, local_ip, peer_m_port, peer_p_port)

    # User commands
    while True:
        try:
            cmd = input("Command (setup-dht/query/teardown/exit): ").strip()
            if cmd == "exit":
                print("Exiting peer.")
                sys.exit(0)
            elif cmd == "setup-dht":
                n = int(input("Ring Size: "))
                year = int(input("Year (1950-2019): "))
                setup_dht(peer_name, n, year)
            elif cmd == "query":
                query_dht(peer_name)
            elif cmd == "teardown":
                teardown_dht()
            else:
                print("Unknown command.")
        except KeyboardInterrupt:
            print("Keyboard exit.")
            sys.exit(0)

if __name__ == "__main__":
    main()
