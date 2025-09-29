# peer program

# peer can send the following commands to manager:

# register <peer_name> <IPv4-address> <m_port> <p_port>

# setup-dht <peer_name> <n> <YYYY>

# after receiving SUCCESS, leader will complete DHT setup and send dht-complete to manager

# Steps to set up DHT:
# 1. Assign identifiers and neighbours. First, a logical ring among the n processes must be set up. The processes will be assigned identifiers 0, 1, . . . , n − 1. The leader has identifier 0. For i = 1, . . . , n − 1: 
# (a) The leader sends a command set-id to peeri at IP-addri, p_porti. It also sends identifier i, the ring size n, and the 3-tuples from Table 1. 
# (b) On receipt of a set-id, peeri sets its identifier to i and the ring size to n. It stores the 3-tuple of peer(i+1) mod n to use as the address of its right neighbour, i.e., the next process on the cycle. After the assignment of identifiers, all management messages must always travel around the logical ring in one direction, from the process with identifier 0 → 1 → 2 → . . . → n − 1 and back to 0. 
# 2. Construct the local DHTs. Now, the leader populates the DHT with data. The dataset is storm data provided by the U.S. National Weather Service (NWS). It contains a listing by state of weather phenomena. Data is available from 1950 to the present. For each year YYYY, 1950 ≤ YYYY ≤ 2019, the input file is named details-YYYY.csv, and is a comma separated value (CSV) file containing the storm event data. Each line of the file details-YYYY.csv contains details of a storm event described by the following 14 fields, in order. An exception is the first line of the file which contains the field names and should be skipped. 
# (a) event id: An identifier assigned by the NWS for a specific storm event. Example: 383097. 
# (b) state: The state name, spelled in all capital letters, where the event occurred. Example: GEORGIA. 
# (c) year: The four digit year for the event in this record. Example: 2000. 
# (d) month name: Name of the month for the event in this record spelled out. Example: January. 
# (e) event type: The event types permitted are listed in Table 2, spelled out. 
# (f) cz type: Indicates whether the event happened in a county/parish (C), zone (Z), or marine (M). 
# (g) cz name: County/Parish, Zone or Marine name assigned to the county or zone. Example: AIKEN. 
# (h) injuries direct: The number of injuries directly related to the weather event. Examples: 0, 56. 
# (i) injuries indirect: The number of injuries indirectly related to the weather event. Examples: 0, 87. 
# (j) deaths direct: The number of deaths directly related to the weather event. Examples: 0, 23. 
# (k) deaths indirect: The number of deaths indirectly related to the weather event. Examples: 0, 4, 6. 
# (l) damage property: The estimated amount of damage to property incurred by the weather event. Ex- amples: 10.00K, 0.00K, 10.00M. 
# (m) damage crops: The estimated amount of damage to crops incurred by the weather event. 
# (n) tor f scale: Enhanced Fujita Scale describes the strength of the tornado based on the amount and type of damage caused by the tornado. Examples: EF0, EF1, EF2, EF3, EF4, EF5.

# Valid Event Types 
# Event Name | Designator
# Astronomical Low Tide Z 
# Hurricane (Typhoon) Z 
# Avalanche Z 
# Ice Storm Z 
# Blizzard Z 
# Lake-Effect Snow Z 
# Coastal Flood Z 
# Lakeshore Flood Z 
# Cold/Wind Chill Z 
# Lightning C 
# Debris Flow C 
# Marine Hail M 
# Dense Fog Z 
# Marine High Wind M 
# Dense Smoke Z 
# Marine Strong Wind M 
# Drought Z 
# Marine Thunderstorm Wind M 
# Dust Devil C 
# Rip Current Z 
# Dust Storm Z 
# Seiche Z 
# Excessive Heat Z 
# Sleet Z 
# Extreme Cold/Wind Chill Z 
# Storm Surge/Tide Z 
# Flash Flood C 
# Strong Wind Z 
# Flood C 
# Thunderstorm Wind C 
# Frost/Freeze Z 
# Tornado C 
# Funnel Cloud C 
# Tropical Depression Z 
# Freezing Fog Z 
# Tropical Storm Z 
# Hail C 
# Tsunami Z 
# Heat Z 
# Volcanic Ash Z 
# Heavy Rain C 
# Waterspout M 
# Heavy Snow Z 
# Wildfire Z 
# High Surf Z 
# Winter Storm Z 
# High Wind Z 
# Winter Weather Z 
# 
# Compute the number of storm events, `, in the details-YYYY.csv file, i.e., , this is one less than the number of lines in the file. For i = 1, . . . , `, the leader: 
# (a) Reads storm event i from the dataset into a record. Each record will be stored in one local hash table. 
# (b) Determines the identifier of the peer that will store the record. To do so, it computes two hash functions. The first hash function computes a position, pos, as the event id field modulo the hash table size. The hash table size, s, is the first prime number larger than 2 × `. The second hash function is the position modulo the ring size n. 

# pos = event id mod s 
# id = pos mod n 

# Here, 0 ≤ id ≤ n − 1, is the identifier of the node in the ring that is to store the record, whereas pos gives the position in the local hash table of node id at which to store the record. If the id is identifier of the current node, then it stores the record in its local hash table. Otherwise, the leader sends a store command to its right neighbour on the ring, along with the record. The record is forwarded along the ring from the leader to node whose identifier equals id, where it is stored in the local hash table. 
# 
# In a correct implementation, a store command does not return to the leader. Do not have the leader send the store command directly to the node id. In DHTs the topology of the DHT, here a ring, must be used for management functions. 3. Print configuration and signal completion of DHT set up. The leader outputs the number of records stored at each node in the ring, and sends a message for the dht-complete command to the manager.

# dht-complete <peer_name>


#Libraries
import socket as s
import threading
import json
import time
import csv
import math
import random
from collections import deque

m_socket = s.socket(s.AF_INET, s.SOCK_DGRAM)
p_socket = s.socket(s.AF_INET, s.SOCK_DGRAM)
peer_socket = s.socket(s.AF_INET, s.SOCK_DGRAM)

registered = False
name = ""
identifier = -1
ring_size = -1
local_table = []            #hash table that is a part of the DHT
three_tuple_data = []       #stores the member data
right_neighbour_tuple = (0, 0, 0)  #stores the contact info of the right neighbor in the DHT
year_used = 1950            #stores what year was sent to manager/data in table is from

leaving = False
joining = False
tearing_down = False

#used in the dht-setup process only
global_table = []       #stores global table data

def is_prime(n):
    if n < 2: return False
    if n in (2,3): return True
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0: return False
    return True

def next_prime_after(n):
    n = n + 1
    while True:
        if is_prime(n): return n
        else: n = n + 1


def reciever():
    global registered, identifier, ring_size, three_tuple_data, local_table, global_table, right_neighbour_tuple, leaving, joining, tearing_down, year_used
    while True:
        # waiting for a response from the manager. 
        # if the recieved message isn't from  manager, ignore
        raw_data, recv_addr = peer_socket.recvfrom(4096)
        # if(data):
        #     print(f"\n[From {recv_addr[0]}:{recv_addr[1]}] {data.decode()}")
        data = json.loads(raw_data.decode())
        if data.get('status') != 'PEER-MESSAGE':
            print("Status:"+data.get('status'))

        #handle response from the manager
        if data.get('status') == 'SUCCESS':
            if data.get('command-type') == 'register':
                registered = True

            elif data.get('command-type') == 'setup-dht':
                # setting id for all the registered peers
                identifier = 0
                three_tuple_data = data.get('members')
                
                c = three_tuple_data.copy()
                for index in range(0, len(data)-1):
                    c[index].append(index)


                for index in range(1, len(data)-1):
                    #getting address and port for peer index # x
                    _, peerx_add, peerx_port, _ = data.get('members')[index]
                    #encoding data using json and sending to peers_i
                    cmd = {
                        'status': 'PEER-MESSAGE',
                        'command-type': 'set-id', 
                        'identifier': index, 
                        'ring_size': data.get('size'), 
                        '3-tuple-data': (data.get('members'))}
                    print(cmd)
                    cmd_json = json.dumps(cmd).encode()
                    peer_socket.sendto(cmd_json, (peerx_add, int(peerx_port)))
                ring_size = data.get('size')
                right_neighbour_index = (identifier+1) % ring_size 
                right_neighbour_tuple = data.get('members')[right_neighbour_index]
                
                print(three_tuple_data)
                populate_dht()
                # change later, set csv value as 1950
                # reading csv file
                with open("./CSVFiles/details-" + str(year_used) + ".csv", "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # local table has i = 1,..., l entries
                    for row in reader:
                        global_table.append(tuple(row))
                    
                for entry in global_table:
                    position = int(entry[0]) % next_prime_after(2*len(local_table))
                    id = position % ring_size
                    if id == identifier:
                        local_table.append(entry)
                    else:
                        cmd = {
                            'status': 'PEER-MESSAGE',
                            'command-type': 'store',
                            'id': id,
                            'entry': entry
                        }
                        cmd_json = json.dumps(cmd).encode()
                        right_name, right_add, right_port, _ = right_neighbour_tuple
                        peer_socket.sendto(cmd_json, (right_add, right_port))
                
                print(len(local_table))

                cmd = {'command': 'dht-complete', 
                        'peer_name': name}
                cmd_json = json.dumps(cmd).encode()
                peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

            elif data.get('command-type') == 'teardown-dht':
                #confirmed, start teardown
                tearing_down = True
                cmd = {
                    'status': 'PEER-MESSAGE',
                    'command-type': 'teardown'}
                cmd_json = json.dumps(cmd).encode()
                right_name, right_add, right_port, _ = right_neighbour_tuple
                peer_socket.sendto(cmd_json, (right_add, right_port))

            elif data.get('command-type') == 'query-dht':
                print(f"{raw_data.decode()}")
                event_id = 10120412
                cmd = {'status': 'PEER-MESSAGE',
                       'command-type': 'find-event', 
                       'event_id': event_id,
                       'id-seq': []}
                cmd_json = json.dumps(cmd).encode()
                peer_socket.sendto(cmd_json, (data.get("addr"), int(data.get("p-port"))))


            elif data.get('command-type') == 'leave-dht':
                leaving = True
                #initiate step 1
                cmd = {
                    'status': 'PEER-MESSAGE',
                    'command-type': 'teardown',
                    'cause': 'leave'}
                cmd_json = json.dumps(cmd).encode()
                right_name, right_add, right_port = right_neighbour_tuple
                peer_socket.sendto(cmd_json, (right_add, right_port))

            elif data.get('command-type') == 'join-dht':
                leaving = True
                identifier = 0
                right_neighbour_tuple = data.get('leader')
                # initiate step 1
                cmd = {
                    'status': 'PEER-MESSAGE',
                    'command-type': 'reset-id',
                    'identifier': 1,
                    'cause': 'join',
                    'initiator': (name, peer_socket.getsockname()[0], peer_socket.getsockname()[1])}
                cmd_json = json.dumps(cmd).encode()
                right_name, right_add, right_port = right_neighbour_tuple
                peer_socket.sendto(cmd_json, (right_add, right_port))
                
            elif data.get('command-type') == 'dht-complete':
                pass

        elif data.get('status') == "FAILURE":
            print(data.get('message'))
            if data.get('command-type') == 'setup-dht':
                print(data.get('members'))
            pass
        elif data.get('status') == 'PEER-MESSAGE':
            if data.get('command-type')== 'set-id':
                identifier = data.get('identifier')
                print("Identifier: " +str(identifier))
                ring_size = data.get('ring_size')
                print("Ring_size: " +str(ring_size))
                
                # finding and storing the 3-tuple of its right neighbour
                right_neighbour_index = (identifier+1) % ring_size 
                print("right_neighbour_index: " +str(right_neighbour_index))

                right_neighbour_tuple = data.get('3-tuple-data')[right_neighbour_index]
                three_tuple_data = data.get('3-tuple-data')

                print(three_tuple_data)
            elif data.get('command-type')== 'store':
                #add data to local table if it's the intended recipient
                if data.get('id') == identifier:
                    local_table.append(data.get('entry'))
                    year_used = data.get('year')
                #forward to neighbor if not
                else:
                    right_name, right_add, right_port, _ = right_neighbour_tuple
                    peer_socket.sendto(raw_data, (right_add, right_port))
                print(len(local_table))

            elif data.get('command-type')== 'find-event':
                print("finding EVENT\n")
                id_seq = data.get('id-seq')
                I = [i for i in range(0, ring_size)]
                print(id_seq)
                position = data.get('event_id') % next_prime_after(2*len(local_table))
                id = position % ring_size
                
                # not hash, just lookup: fix later
                if id == identifier:
                    for i in local_table: 
                        if int(data.get('event_id')) == int(i[7]):
                            print(f"Storm event found {data.get('event_id')}")
                            print("Id-seq\n")
                            print(id_seq)
                            print("Record\n")
                            print(local_table)
                            return
                
                print("Not found")
                id_seq.append(identifier)
                updateI = [i for i in I if i not in id_seq]
                if (len(updateI) == 0):
                    print(f"Storm event {data.get('event_id')} not found in the DHT.")
                    return
                nextI = random.choice(updateI)
                cmd = {'status': 'PEER-MESSAGE',
                        'command-type': 'find-event', 
                        'event_id': data.get('event_id'),
                        'id-seq': id_seq}
                cmd_json = json.dumps(cmd).encode()
                peer_socket.sendto(cmd_json, (three_tuple_data[nextI][1], int((three_tuple_data[nextI][2]))))

            elif data.get('command-type')== 'teardown':
                #delete own hash table
                local_table.clear()
                if not leaving and not joining and not tearing_down:
                    #forward to neighbor if this peer did not initiate the teardown
                    right_name, right_add, right_port, _ = right_neighbour_tuple
                    peer_socket.sendto(raw_data, (right_add, right_port))
                elif tearing_down:
                    #done with teardown
                    cmd = {'command': 'teardown-complete',
                            'peer_name': name}
                    cmd_json = json.dumps(cmd).encode()
                    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))
                elif leaving:
                    #step 1 of leave-dht is done
                    #send out the reset-id
                    cmd = {'status': 'PEER-MESSAGE',
                            'command-type': 'reset-id',
                            'identifier': 0,
                            'cause': 'leave'}
                    cmd_json = json.dumps(cmd).encode()
                    right_name, right_add, right_port = right_neighbour_tuple
                    peer_socket.sendto(cmd_json, (right_add, right_port))
                elif joining:
                    #step 2 of join-dht is done
                    #begin rebuilding dht
                    populate_dht()
                    #send rebuilt signal to manager
                    cmd = {'command': 'dht-rebuilt',
                            'new-leader': name,
                            'peer_name': name}
                    cmd_json = json.dumps(cmd).encode()
                    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

            elif data.get('command-type') == 'reset-id':
                if leaving:
                    #step 2 of leave-dht is done
                    #send out rebuild-dht
                    cmd = {'status': 'PEER-MESSAGE',
                            'command-type': 'rebuild-dht',
                            'initiator-name': name}
                    cmd_json = json.dumps(cmd).encode()
                    right_name, right_add, right_port = right_neighbour_tuple
                    peer_socket.sendto(cmd_json, (right_add, right_port))
                elif joining:
                    ring_size = data.get('identifier')
                    #step 1 of join-dht is done
                    #initiate step 2
                    cmd = {'status': 'PEER-MESSAGE',
                           'command-type': 'teardown'}
                    cmd_json = json.dumps(cmd).encode()
                    right_name, right_add, right_port = right_neighbour_tuple
                    peer_socket.sendto(cmd_json, (right_add, right_port))
                else:
                    #reset id, rearrange peers, & forward
                    new_id = data.get('identifier')
                    #rearrange peers
                    temp_d = deque(three_tuple_data)
                    temp_d.rotate(identifier-new_id)
                    if data.get('cause') == 'leave':
                        # remove last element if a peer is leaving
                        temp_d.pop()
                        # and change ring size to match
                        ring_size -= 1
                    if data.get('cause') == 'join':
                        ring_size += 1
                        #right neighbor is initiator if you are the last one
                        if identifier == len(temp_d):
                            right_neighbour_tuple = data.get('initiator')
                        #add initiator to member array as first
                        temp_d.appendleft(data.get('initiator'))
                    three_tuple_data = list(temp_d)

                    identifier = new_id
                    cmd = {'status': 'PEER-MESSAGE',
                            'command-type': 'reset-id',
                            'identifier': identifier+1,
                            'cause': data.get('cause')}
                    cmd_json = json.dumps(cmd).encode()
                    right_name, right_add, right_port = right_neighbour_tuple
                    peer_socket.sendto(cmd_json, (right_add, right_port))

            elif data.get('command-type') == 'rebuild-dht':
                populate_dht()
                #send rebuilt signal to manager
                cmd = {'command': 'dht-rebuilt',
                        'new-leader': name,
                        'peer_name': data.get('initiator-name')}
                cmd_json = json.dumps(cmd).encode()
                peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))


        else:
            print("Manager responded with an unrecognized command. Try again.")
            print(data)

def register(name, addr, m_port, p_port):
    time.sleep(1)
    if len(name) > 15 or not(name.isalpha()):
        print("Peer name needs to be alphabetic and less than 15. Try again.")
        return 
    if registered:
        print("peer already registered")
        return
    #encoding data using json and sending to manager
    cmd = {'command': 'register', 
            'peer_name': name, 
            'IPv4_address': addr, 
            'm_port': m_port, 
            'p_port': p_port}
    cmd_json = json.dumps(cmd).encode()
    # m_socket.bind((addr, m_port))
    peer_socket.sendto(cmd_json, (manager_address, int(manager_port)))
    t.start()

def dht_setup(name, size, year):
    global year_used
    #encoding data using json and sending to manager
    year_used = year
    cmd = {'command': 'setup-dht', 
            'peer_name': name, 
            'n': size, 
            'YYYY': year}
    cmd_json = json.dumps(cmd).encode()
    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

def query_dht(peer_name):
    #encoding data using json and sending to manager
    cmd = {'command': 'query-dht', 
            'peer_name': peer_name}
    cmd_json = json.dumps(cmd).encode()
    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

def teardown_dht():
    cmd = {'command': 'teardown-dht',
           'peer_name': name}
    cmd_json = json.dumps(cmd).encode()
    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

def leave_dht():
    cmd = {'command': 'leave-dht',
           'peer_name': name}
    cmd_json = json.dumps(cmd).encode()
    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

def join_dht():
    cmd = {'command': 'join-dht',
           'peer_name': name}
    cmd_json = json.dumps(cmd).encode()
    peer_socket.sendto(cmd_json,(manager_address, int(manager_port)))

def populate_dht():
    global year_used, global_table, local_table, right_neighbour_tuple
    # reading csv file
    with open("./CSVFiles/details-" + str(year_used) + ".csv", "r") as file:
        reader = csv.reader(file)
        next(reader)

        # local table has i = 1,..., l entries
        for row in reader:
            global_table.append(tuple(row))

    for entry in global_table:
        position = int(entry[0]) % next_prime_after(2 * len(local_table))
        id = position % ring_size
        if id == identifier:
            local_table.append(entry)
        else:
            cmd = {
                'status': 'PEER-MESSAGE',
                'command-type': 'store',
                'id': id,
                'entry': entry,
                'year': year_used
            }
            cmd_json = json.dumps(cmd).encode()
            right_name, right_add, right_port = right_neighbour_tuple
            peer_socket.sendto(cmd_json, (right_add, right_port))

def main():
    global name
    # Start a new thread where peer listens to incoming messages. 
    # Main Loop where user inputs commands and parameters
    # Main Loop can be exited by using "exit" or "CTRL+C"
    while True:
        try: 
            time.sleep(1)
            command = input("Command: ")
            if command.lower() == 'exit':
                print("Closing Peer. Exiting...")
                break
            match command:
                case "r":
                    print("\nEnter d1, d2, d3, or d4 to use default settings.")

                    name = input("Peer name: ")
                    # default paramters
                    if name == "d1":
                        name = "apple"
                        addr = "127.0.0.1"
                        m_port = 15001
                        p_port = 15002
                        peer_socket.bind((addr, p_port))

                    elif name == "d2":
                        name = "goat"
                        addr = "127.0.0.1"
                        m_port = 15003
                        p_port = 15004
                        peer_socket.bind((addr, p_port))

                    elif name == "d3":
                        name = "tree"
                        addr = "127.0.0.1"
                        m_port = 15005
                        p_port = 15006
                        peer_socket.bind((addr, p_port))
                    
                    elif name == "d4":
                        name = "sky"
                        addr = "127.0.0.1"
                        m_port = 15007
                        p_port = 15008
                        peer_socket.bind((addr, p_port))

                    elif name == "d5":
                        name = "rock"
                        addr = "127.0.0.1"
                        m_port = 15009
                        p_port = 15010
                        peer_socket.bind((addr, p_port))

                    else:
                        addr = input("IP address: ")
                        m_port = input("Peer-manager port: ")
                        p_port = input("Peer-Peer port: ")
                    register(name, addr, int(m_port), int(p_port))

                case "setup-dht":
                    # n = input("Ring size: ")
                    # year = input("Year: ")
                    n = 3
                    year = 1950
                    dht_setup(name, int(n), int(year))

                case "teardown-dht":
                    teardown_dht()

                case "query-dht":
                    peer_name = input("Peer name: ")
                    query_dht(peer_name)

                case "leave-dht":
                    leave_dht()

                case "join-dht":
                    join_dht()

                case _:
                    print("Command: "+command+ " isn't recognized. Try again.")
                   
        except KeyboardInterrupt:
            print("\nKeyboard interrupt. \nClosing Peer. Exiting...")
            break

if __name__ == "__main__":
    # print("\nEnter d to use default settings.\n")
    # manager_address = input("Enter manager IPv4 address: ")
    # if manager_address == "d":
    manager_address = "127.0.0.1"
    manager_port = 15000
    # else: manager_port = input("Enter manager_port: ")
    # print("\n")
    t = threading.Thread(target=reciever)


    main()
