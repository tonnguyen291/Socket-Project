import socket
import sys
import random
import json
import threading

class PeerState:
    FREE = 'Free'
    LEADER = 'Leader'
    INDHT = 'InDHT'

class PortManager:
    def __init__(self):
        self.min_port = 15000
        self.max_port = 15499
        self.used_ports = set()

    def reserve_port(self, port):
        if self.min_port <= port <= self.max_port and port not in self.used_ports:
            self.used_ports.add(port)
            return True
        return False

    def release_port(self, port):
        self.used_ports.discard(port)

    def is_available(self, port):
        return self.min_port <= port <= self.max_port and port not in self.used_ports

class Manager:
    def __init__(self, ip, port, port_manager):
        self.addr = (ip, port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.addr)
        self.peers = {}
        self.peer_states = {}
        self.port_manager = port_manager
        self.dht_exists = False
        self.dht_ready = False
        self.teardown_in_progress = False

        print(f"[Manager] Listening on {ip}:{port}")

    def listen(self):
        while True:
            data, client_addr = self.socket.recvfrom(4096)
            try:
                msg = json.loads(data.decode())
                response = self.handle_message(msg)
            except Exception as e:
                response = {'status': 'FAILURE', 'message': str(e)}
            self.socket.sendto(json.dumps(response).encode(), client_addr)

    def handle_message(self, msg):
        command = msg.get('command')

        if self.teardown_in_progress and command != 'teardown-complete':
            return {'status': 'FAILURE', 'message': 'Teardown in progress'}

        if self.dht_exists and not self.dht_ready and command != 'dht-complete':
            return {'status': 'FAILURE', 'message': 'DHT Setup in progress'}

        handler = {
            'register': self.register_peer,
            'setup-dht': self.setup_dht,
            'dht-complete': self.complete_dht,
            'teardown-dht': self.teardown_dht,
            'teardown-complete': self.complete_teardown,
            'query-dht': self.query_dht,
            'deregister': self.deregister_peer
        }.get(command)

        if handler:
            return handler(msg)
        else:
            return {'status': 'FAILURE', 'message': f'Invalid command: {command}'}

    def register_peer(self, msg):
        name = msg.get('peer_name')
        ip = msg.get('IPv4_address')
        m_port = msg.get('m_port')
        p_port = msg.get('p_port')

        if name in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer name already exists'}

        if not self.port_manager.is_available(m_port) or not self.port_manager.is_available(p_port):
            return {'status': 'FAILURE', 'message': 'Ports already in use'}

        self.peers[name] = {'ip': ip, 'm_port': m_port, 'p_port': p_port}
        self.peer_states[name] = PeerState.FREE
        self.port_manager.reserve_port(m_port)
        self.port_manager.reserve_port(p_port)

        return {'status': 'SUCCESS', 'message': f'Peer {name} registered', 'command-type': 'register'}

    def deregister_peer(self, msg):
        name = msg.get('peer_name')

        if name not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}

        if self.peer_states.get(name) != PeerState.FREE:
            return {'status': 'FAILURE', 'message': 'Cannot deregister while in DHT'}

        self.port_manager.release_port(self.peers[name]['m_port'])
        self.port_manager.release_port(self.peers[name]['p_port'])
        del self.peers[name]
        del self.peer_states[name]

        return {'status': 'SUCCESS', 'message': f'Peer {name} deregistered', 'command-type': 'deregister'}

    def setup_dht(self, msg):
        leader = msg.get('peer_name')
        n = msg.get('n')

        if leader not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}
        if n < 3:
            return {'status': 'FAILURE', 'message': 'DHT size must be at least 3'}
        if len(self.peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough peers registered'}
        if self.dht_exists:
            return {'status': 'FAILURE', 'message': 'DHT already exists'}

        free_peers = [p for p, s in self.peer_states.items() if s == PeerState.FREE]
        if leader not in free_peers or len(free_peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough free peers or leader not free'}

        self.peer_states[leader] = PeerState.LEADER
        free_peers.remove(leader)
        selected = random.sample(free_peers, n-1)
        for p in selected:
            self.peer_states[p] = PeerState.INDHT

        members = [leader] + selected
        member_info = [(p, self.peers[p]['ip'], self.peers[p]['p_port']) for p in members]

        self.dht_exists = True
        self.dht_ready = False

        return {'status': 'SUCCESS', 'members': member_info, 'command-type': 'setup-dht', 'size': n}

    def complete_dht(self, msg):
        leader = msg.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Invalid leader'}

        self.dht_ready = True
        return {'status': 'SUCCESS', 'message': 'DHT is now ready', 'command-type': 'setup-dht'}

    def teardown_dht(self, msg):
        leader = msg.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Only DHT leader can request teardown'}

        self.teardown_in_progress = True
        return {'status': 'SUCCESS', 'message': 'Teardown started', 'command-type': 'teardown-dht'}

    def complete_teardown(self, msg):
        self.teardown_in_progress = False
        self.dht_exists = False
        self.dht_ready = False

        for peer in list(self.peer_states):
            self.peer_states[peer] = PeerState.FREE

        return {'status': 'SUCCESS', 'message': 'Teardown complete', 'command-type': 'teardown-complete'}

    def query_dht(self, msg):
        if not self.dht_ready:
            return {'status': 'FAILURE', 'message': 'DHT is not ready'}

        requester = msg.get('peer_name')
        if requester not in self.peers:
            return {'status': 'FAILURE', 'message': 'Requester not registered'}

        dht_peers = [p for p, s in self.peer_states.items() if s == PeerState.INDHT]
        if not dht_peers:
            return {'status': 'FAILURE', 'message': 'No DHT peers available'}

        peer = random.choice(dht_peers)
        return {
            'status': 'SUCCESS',
            'peer-name': peer,
            'addr': self.peers[peer]['ip'],
            'p-port': self.peers[peer]['p_port'],
            'command-type': 'query-dht'
        }

def main():
    ip = "127.0.0.1"
    port = 15000
    port_manager = PortManager()

    mgr = Manager(ip, port, port_manager)
    mgr.listen()

if __name__ == "__main__":
    main()
import socket
import sys
import random
import json
import threading

class PeerState:
    FREE = 'Free'
    LEADER = 'Leader'
    INDHT = 'InDHT'

class PortManager:
    def __init__(self):
        self.min_port = 15000
        self.max_port = 15499
        self.used_ports = set()

    def reserve_port(self, port):
        if self.min_port <= port <= self.max_port and port not in self.used_ports:
            self.used_ports.add(port)
            return True
        return False

    def release_port(self, port):
        self.used_ports.discard(port)

    def is_available(self, port):
        return self.min_port <= port <= self.max_port and port not in self.used_ports

class Manager:
    def __init__(self, ip, port, port_manager):
        self.addr = (ip, port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.addr)
        self.peers = {}
        self.peer_states = {}
        self.port_manager = port_manager
        self.dht_exists = False
        self.dht_ready = False
        self.teardown_in_progress = False

        print(f"[Manager] Listening on {ip}:{port}")

    def listen(self):
        while True:
            data, client_addr = self.socket.recvfrom(4096)
            try:
                msg = json.loads(data.decode())
                response = self.handle_message(msg)
            except Exception as e:
                response = {'status': 'FAILURE', 'message': str(e)}
            self.socket.sendto(json.dumps(response).encode(), client_addr)

    def handle_message(self, msg):
        command = msg.get('command')

        if self.teardown_in_progress and command != 'teardown-complete':
            return {'status': 'FAILURE', 'message': 'Teardown in progress'}

        if self.dht_exists and not self.dht_ready and command != 'dht-complete':
            return {'status': 'FAILURE', 'message': 'DHT Setup in progress'}

        handler = {
            'register': self.register_peer,
            'setup-dht': self.setup_dht,
            'dht-complete': self.complete_dht,
            'teardown-dht': self.teardown_dht,
            'teardown-complete': self.complete_teardown,
            'query-dht': self.query_dht,
            'deregister': self.deregister_peer
        }.get(command)

        if handler:
            return handler(msg)
        else:
            return {'status': 'FAILURE', 'message': f'Invalid command: {command}'}

    def register_peer(self, msg):
        name = msg.get('peer_name')
        ip = msg.get('IPv4_address')
        m_port = msg.get('m_port')
        p_port = msg.get('p_port')

        if name in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer name already exists'}

        if not self.port_manager.is_available(m_port) or not self.port_manager.is_available(p_port):
            return {'status': 'FAILURE', 'message': 'Ports already in use'}

        self.peers[name] = {'ip': ip, 'm_port': m_port, 'p_port': p_port}
        self.peer_states[name] = PeerState.FREE
        self.port_manager.reserve_port(m_port)
        self.port_manager.reserve_port(p_port)

        return {'status': 'SUCCESS', 'message': f'Peer {name} registered', 'command-type': 'register'}

    def deregister_peer(self, msg):
        name = msg.get('peer_name')

        if name not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}

        if self.peer_states.get(name) != PeerState.FREE:
            return {'status': 'FAILURE', 'message': 'Cannot deregister while in DHT'}

        self.port_manager.release_port(self.peers[name]['m_port'])
        self.port_manager.release_port(self.peers[name]['p_port'])
        del self.peers[name]
        del self.peer_states[name]

        return {'status': 'SUCCESS', 'message': f'Peer {name} deregistered', 'command-type': 'deregister'}

    def setup_dht(self, msg):
        leader = msg.get('peer_name')
        n = msg.get('n')

        if leader not in self.peers:
            return {'status': 'FAILURE', 'message': 'Peer not registered'}
        if n < 3:
            return {'status': 'FAILURE', 'message': 'DHT size must be at least 3'}
        if len(self.peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough peers registered'}
        if self.dht_exists:
            return {'status': 'FAILURE', 'message': 'DHT already exists'}

        free_peers = [p for p, s in self.peer_states.items() if s == PeerState.FREE]
        if leader not in free_peers or len(free_peers) < n:
            return {'status': 'FAILURE', 'message': 'Not enough free peers or leader not free'}

        self.peer_states[leader] = PeerState.LEADER
        free_peers.remove(leader)
        selected = random.sample(free_peers, n-1)
        for p in selected:
            self.peer_states[p] = PeerState.INDHT

        members = [leader] + selected
        member_info = [(p, self.peers[p]['ip'], self.peers[p]['p_port']) for p in members]

        self.dht_exists = True
        self.dht_ready = False

        return {'status': 'SUCCESS', 'members': member_info, 'command-type': 'setup-dht', 'size': n}

    def complete_dht(self, msg):
        leader = msg.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Invalid leader'}

        self.dht_ready = True
        return {'status': 'SUCCESS', 'message': 'DHT is now ready', 'command-type': 'setup-dht'}

    def teardown_dht(self, msg):
        leader = msg.get('peer_name')

        if leader not in self.peers or self.peer_states.get(leader) != PeerState.LEADER:
            return {'status': 'FAILURE', 'message': 'Only DHT leader can request teardown'}

        self.teardown_in_progress = True
        return {'status': 'SUCCESS', 'message': 'Teardown started', 'command-type': 'teardown-dht'}

    def complete_teardown(self, msg):
        self.teardown_in_progress = False
        self.dht_exists = False
        self.dht_ready = False

        for peer in list(self.peer_states):
            self.peer_states[peer] = PeerState.FREE

        return {'status': 'SUCCESS', 'message': 'Teardown complete', 'command-type': 'teardown-complete'}

    def query_dht(self, msg):
        if not self.dht_ready:
            return {'status': 'FAILURE', 'message': 'DHT is not ready'}

        requester = msg.get('peer_name')
        if requester not in self.peers:
            return {'status': 'FAILURE', 'message': 'Requester not registered'}

        dht_peers = [p for p, s in self.peer_states.items() if s == PeerState.INDHT]
        if not dht_peers:
            return {'status': 'FAILURE', 'message': 'No DHT peers available'}

        peer = random.choice(dht_peers)
        return {
            'status': 'SUCCESS',
            'peer-name': peer,
            'addr': self.peers[peer]['ip'],
            'p-port': self.peers[peer]['p_port'],
            'command-type': 'query-dht'
        }

def main():
    ip = "127.0.0.1"
    port = 15000
    port_manager = PortManager()

    mgr = Manager(ip, port, port_manager)
    mgr.listen()

if __name__ == "__main__":
    main()
