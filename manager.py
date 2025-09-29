# manager program
import socket
import sys
import random
import json

class PeerState:
    FREE = 'Free'
    LEADER = 'Leader'
    INDHT = 'InDHT'

class PortManager:
# available port numbers for our group: 15000 - 15499
    def __init__(self):
        self.min_port = 15000
        self.max_port = 15499
        self.used_ports = set()
    
    def reserve_port(self, port):
        if port < self.min_port or port > self.max_port:
            return False
        if port in self.used_ports:
            return False
        self.used_ports.add(port)
        return True
    
    def release_port(self, port):
        if port in self.used_ports:
            self.used_ports.remove(port)

    def is_available(self, port):
        return port not in self.used_ports and port >= self.min_port and port <= self.max_port
    
class Manager:
    def __init__(self, host_ip, host_port, port_manager):
    # manager maintains a state information base (SIB) of all registered peers
    # SIB is a dictionary with peer_name as key and a 3-tuple as value
    # 3-tuple is (IPv4_address, m_port, p_port)
    # state of each peer is Free, InDHT, or Leader
        self.addr = (host_ip, host_port)
        self.peers = {} # peer_name: (IPv4_address, m_port, p_port)
        self.peer_states = {} # peer_name: state
        self.port_manager = port_manager

        self.dht_exists = False
        self.dht_ready = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.addr)
        self.port_manager.reserve_port(host_port)

        self.teardown_in_progress = False

        print(f"Manager listening on {host_ip}:{host_port}")

    def listen(self):
        while True:
            data, peer_addr = self.socket.recvfrom(4096)
            try:
                message = json.loads(data.decode())
                response = self.handle_message(message)
            except Exception as e:
                response = {'status': 'FAILURE', 'message': str(e)}

            self.socket.sendto(json.dumps(response).encode(), peer_addr)

    def handle_message(self, message):
        command = message.get('command')

        if self.teardown_in_progress and command != 'teardown-complete':
            return {'status': 'FAILURE', 'message': 'DHT teardown in progress'}
        if self.dht_exists and not self.dht_ready and command != 'dht-complete':
            return {'status': 'FAILURE', 'message': 'DHT setup in progress'}

        if command == 'register':
            return self.register_peer(message)
        elif command == 'setup-dht':
            return self.setup_dht(message)
        elif command == 'dht-complete':
            return self.dht_complete(message)
        elif command == 'deregister':
            return self.deregister_peer(message)
        elif command == 'teardown-dht':
            return self.teardown_dht(message)
        elif command == 'teardown-complete':
            return self.teardown_complete(message)
        elif command == 'query-dht':
            return self.query_dht(message)
        else:
            return {'status': 'FAILURE', 'message': 'Invalid command'}

    
    def register_peer(self, message):
    # register <peer_name> <IPv4_address> <m_port> <p_port> (peer_name is alphabetic string max 15 characters)
    # manager receives a register command from a peer
    # store the peer_name, IPv4_address, n-port, p_port in a state information base
    # m_port for communication with manager, p_port for communication with peers
    # set state of peer to Free
    # if peer_name is unique return SUCCESS, else return FAILURE and do nothing
        peer_name = message.get('peer_name')
        ip = message.get('IPv4_address')
        m_port = message.get('m_port')
        p_port = message.get('p_port')

        if peer_name in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer name already exists'}
        
        if not self.port_manager.is_available(m_port) or not self.port_manager.is_available(p_port):
            return {'status': 'FAILURE', 'message': 'Port number already in use'}
        
        self.peers[peer_name] = {
            'ip': ip,
            'm_port': m_port,
            'p_port': p_port
        }
        self.peer_states[peer_name] = PeerState.FREE
        self.port_manager.reserve_port(m_port)
        self.port_manager.reserve_port(p_port)

        return {'status': 'SUCCESS', 'message': 'Peer registered', 'command-type': 'register'}

    def deregister_peer(self, message):
        peer_name = message.get('peer_name')

        if peer_name not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}

        if self.peer_states.get(peer_name) != PeerState.FREE:
            return {'status': 'FAILURE', 'message': 'Peer not in Free state'}

        # Free ports
        peer_info = self.peers[peer_name]
        self.port_manager.release_port(peer_info['m_port'])
        self.port_manager.release_port(peer_info['p_port'])

        # Remove peer
        del self.peers[peer_name]
        del self.peer_states[peer_name]

        return {'status': 'SUCCESS', 'message': 'Peer deregistered', 'command-type': 'deregister'}

    
    def setup_dht(self, message):
    # setup-dht <peer_name> <n> <YYYY> (DHT of size n from year YYYY)
    # initiate construction of DHT of size n from year YYYY
    # manager receives a setup-dht command from a peer
    # return FAILURE if: peer_name is not registered; n < 3; fewer than n users registered; DHT already exists
    # else: set state of peer to Leader; select at random n-1 Free peers and set states to InDHT; return SUCCESS and list of n peers
    # after SUCCESS, manager waits for dht-complete, returns FAILURE to any other incoming messages
        leader = message.get('peer_name')
        n = message.get('n')
        year = message.get('YYYY')

        if leader not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}
        
        if n < 3:
            return {'status': 'FAILURE', 'message': 'DHT size must be at least 3'}
        
        if len(self.peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough peers registered'}
        
        if self.dht_exists:
            return {'status': 'FAILURE', 'message': 'DHT already exists'}
        
        free_peers = [peer for peer, state in self.peer_states.items() if state == PeerState.FREE]
        if leader not in free_peers:
            return {'status': 'FAILURE', 'message': 'Leader not free'}
        
        if len(free_peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough free peers'}
        
        self.peer_states[leader] = PeerState.LEADER
        free_peers.remove(leader)
        in_dht_peers = random.sample(free_peers, n-1)

        for peer in in_dht_peers:
            self.peer_states[peer] = PeerState.INDHT
        
        self.dht_exists = True
        self.dht_ready = False

        dht_members = [leader] + in_dht_peers
        # DHT structure: 
        # peer is a 3-tuple (peer_name, IPv4_address, p_port)
        member_info = [(name, self.peers[name]["ip"], self.peers[name]["p_port"]) for name in dht_members]

        return {'status': 'SUCCESS', 'members': member_info, 'command-type':'setup-dht', 'size': n}
    
    def dht_complete(self, message):
    # dht-complete <peer_name>
    # indicates leader has completed steps required to setup DHT
    # if peer_name is Leader: return SUCCESS
    # else: return FAILURE
    # manager can now process other commands (except setup-dht)
        peer_name = message.get('peer_name')

        if peer_name not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}
        
        if self.peer_states[peer_name] != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Peer not leader'}
        
        self.dht_ready = True
        print(f"[Manager] DHT setup complete by leader {peer_name}")

        return {'status': 'SUCCESS', 'message': 'DHT setup complete', 'command-type':'setup-dht'}

    def teardown_dht(self, message):
        leader = message.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Peer not the DHT leader'}

        self.teardown_in_progress = True
        print(f"[Manager] Teardown initiated by leader {leader}")
        return {'status': 'SUCCESS', 'message': 'Teardown initiated', 'command-type': 'teardown-dht'}

    def teardown_complete(self, message):
        leader = message.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Peer not the DHT leader'}

        # Reset all DHT peers to Free
        for peer, state in self.peer_states.items():
            if state in [PeerState.INDHT, PeerState.LEADER]:
                self.peer_states[peer] = PeerState.FREE

        self.dht_exists = False
        self.dht_ready = False
        self.teardown_in_progress = False

        print(f"[Manager] DHT teardown completed by leader {leader}")
        return {'status': 'SUCCESS', 'message': 'DHT teardown complete', 'command-type': 'teardown-complete'}

    def query_dht(self, message):
        peer_name = message.get("peer_name")
        if not self.dht_ready:
            return {'status': 'FAILURE', 'message': 'DHT set up has not been completed'}
        if not peer_name in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer is not registered'}
        free_peers = [peer for peer, state in self.peer_states.items() if state == PeerState.FREE]
        if peer_name not in free_peers:
            return {'status': 'FAILURE', 'message': 'Peer is in DHT'}
                
        DHTpeers = [peer for peer, state in self.peer_states.items() if state == PeerState.INDHT]
        peer_name = random.choice(DHTpeers)
        return {'status': 'SUCCESS','peer-name': peer_name, 'addr': self.peers[peer_name]['ip'], 'p-port': self.peers[peer_name]['p_port'], 'command-type':'query-dht'}     

def main():
    host_ip = "127.0.0.1"
    host_port = 15000
    port_manager = PortManager()

    manager = Manager(host_ip, host_port, port_manager)
    manager.listen()

if __name__ == "__main__":
    main()
