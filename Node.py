import hashlib
import time
import logging
from cryptography.fernet import Fernet
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='chord_node.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Node:
    def __init__(self, node_id, m):
        """Initialize a node in the Chord network"""
        self.node_id = node_id
        self.m = m
        self.ring_size = 2 ** m  # Calculate ring size per instance
        self.predecessor = self
        self.successor = self
        self.data = dict()
        self.backup_data = dict()
        self.fingers_table = [self for _ in range(m)]  # Create new list for each instance
        self.last_heartbeat = time.time()
        self.cache = {}
        self.cache_timeout = 300  # 5 minutes
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.load = 0
        self.metrics = {
            'lookups': 0,
            'successful_lookups': 0,
            'failed_lookups': 0,
            'data_size': 0,
            'last_updated': time.time()
        }
        logging.info(f"Node {node_id} initialized with m={m}")

    def __str__(self):
        return f'Node {self.node_id}'

    def __lt__(self, other):
        return self.node_id < other.node_id

    def encrypt_data(self, data):
        """Encrypt data before storing"""
        try:
            return self.cipher_suite.encrypt(str(data).encode())
        except Exception as e:
            logging.error(f"Encryption error in node {self.node_id}: {str(e)}")
            raise

    def decrypt_data(self, encrypted_data):
        """Decrypt stored data"""
        try:
            return self.cipher_suite.decrypt(encrypted_data).decode()
        except Exception as e:
            logging.error(f"Decryption error in node {self.node_id}: {str(e)}")
            raise

    def send_heartbeat(self):
        """Send heartbeat signal"""
        try:
            self.last_heartbeat = time.time()
            logging.info(f"Node {self.node_id} heartbeat sent")
            return True
        except Exception as e:
            logging.error(f"Heartbeat failed for node {self.node_id}: {str(e)}")
            return False

    def print_fingers_table(self):
        """Print the finger table entries"""
        try:
            print(f'Node: {self.node_id} has Successor:{self.successor.node_id} and Pred:{self.predecessor.node_id}')
            print('Finger Table:')
            for i in range(self.m):
                print(f'{(self.node_id + 2 ** i) % self.ring_size} : {self.fingers_table[i].node_id}')
        except Exception as e:
            logging.error(f"Error printing finger table for node {self.node_id}: {str(e)}")

    def join(self, node):
        """Join the Chord network"""
        try:
            succ_node = node.find_successor(self.node_id)
            pred_node = succ_node.predecessor
            self.find_node_place(pred_node, succ_node)
            self.fix_fingers()
            self.take_successor_keys()
            self.backup_to_successor()
            logging.info(f"Node {self.node_id} joined successfully")
        except Exception as e:
            logging.error(f"Join failed for node {self.node_id}: {str(e)}")
            raise

    def leave(self):
        """Leave the Chord network"""
        try:
            # Transfer data to successor with encryption
            for key, value in self.data.items():
                if isinstance(value, bytes):
                    self.successor.data[key] = value
                else:
                    self.successor.data[key] = self.encrypt_data(value)

            # Update routing
            self.predecessor.successor = self.successor
            self.predecessor.fingers_table[0] = self.successor
            self.successor.predecessor = self.predecessor

            # Clear node data
            self.data.clear()
            self.cache.clear()
            self.backup_data.clear()

            logging.info(f"Node {self.node_id} left successfully")
        except Exception as e:
            logging.error(f"Leave failed for node {self.node_id}: {str(e)}")
            raise

    def find_node_place(self, pred_node, succ_node):
        """Find the correct position for the node"""
        try:
            pred_node.fingers_table[0] = self
            pred_node.successor = self
            succ_node.predecessor = self
            self.fingers_table[0] = succ_node
            self.successor = succ_node
            self.predecessor = pred_node
            logging.info(f"Node {self.node_id} placed between nodes {pred_node.node_id} and {succ_node.node_id}")
        except Exception as e:
            logging.error(f"Node placement failed for node {self.node_id}: {str(e)}")
            raise

    def take_successor_keys(self):
        """Take appropriate keys from successor"""
        try:
            keys_to_take = {
                key: self.successor.data[key]
                for key in sorted(self.successor.data.keys())
                if key <= self.node_id
            }
            self.data.update(keys_to_take)
            for key in keys_to_take:
                del self.successor.data[key]
            logging.info(f"Node {self.node_id} took {len(keys_to_take)} keys from successor")
        except Exception as e:
            logging.error(f"Key transfer failed for node {self.node_id}: {str(e)}")
            raise

    def fix_fingers(self):
        """Update finger table entries"""
        try:
            for i in range(1, self.m):
                start = (self.node_id + 2 ** i) % self.ring_size
                self.fingers_table[i] = self.find_successor(start)
            logging.info(f"Node {self.node_id} fingers updated")
        except Exception as e:
            logging.error(f"Fix fingers failed for node {self.node_id}: {str(e)}")
            raise

    def closest_preceding_node(self, node, hashed_key):
        """Find closest preceding node for a key"""
        try:
            for i in range(self.m - 1, -1, -1):
                if self.distance(node.fingers_table[i].node_id, hashed_key) < self.distance(node.node_id, hashed_key):
                    return node.fingers_table[i]
            return node
        except Exception as e:
            logging.error(f"Closest preceding node search failed: {str(e)}")
            raise

    def distance(self, n1, n2):
        """Calculate distance between two nodes"""
        return (n2 - n1) % self.ring_size

    def find_successor(self, key):
        """Find the successor node for a key"""
        try:
            # Check cache first
            cached_result = self.cache_lookup(key)
            if cached_result:
                return cached_result

            if self.node_id == key:
                return self

            if self.distance(self.node_id, key) <= self.distance(self.successor.node_id, key):
                self.cache_store(key, self.successor)
                return self.successor
            else:
                next_node = self.closest_preceding_node(self, key)
                result = next_node.find_successor(key)
                self.cache_store(key, result)
                return result
        except Exception as e:
            logging.error(f"Find successor failed for node {self.node_id}: {str(e)}")
            raise

    def cache_lookup(self, key):
        """Look up a key in the cache"""
        if key in self.cache:
            timestamp, value = self.cache[key]
            if time.time() - timestamp < self.cache_timeout:
                return value
            del self.cache[key]
        return None

    def cache_store(self, key, value):
        """Store a key-value pair in the cache"""
        self.cache[key] = (time.time(), value)

    def backup_to_successor(self):
        """Create backup of node data"""
        try:
            backup_data = {
                'data': self.data.copy(),
                'timestamp': time.time(),
                'node_id': self.node_id
            }
            self.successor.backup_data = backup_data
            logging.info(f"Node {self.node_id} backup created")
        except Exception as e:
            logging.error(f"Backup failed for node {self.node_id}: {str(e)}")
            raise

    def recover_from_backup(self):
        """Recover data from backup"""
        try:
            if self.backup_data and time.time() - self.backup_data['timestamp'] < 3600:
                self.data = self.backup_data['data'].copy()
                logging.info(f"Node {self.node_id} recovered from backup")
                return True
            return False
        except Exception as e:
            logging.error(f"Recovery failed for node {self.node_id}: {str(e)}")
            return False

    def check_load(self):
        """Calculate and return node load"""
        self.load = len(self.data) / self.ring_size
        return self.load

    def update_metrics(self, lookup_success=True):
        """Update node performance metrics"""
        self.metrics['lookups'] += 1
        if lookup_success:
            self.metrics['successful_lookups'] += 1
        else:
            self.metrics['failed_lookups'] += 1
        self.metrics['data_size'] = len(self.data)
        self.metrics['last_updated'] = time.time()

    def export_state(self):
        """Export node state for backup"""
        try:
            return {
                'node_id': self.node_id,
                'successor_id': self.successor.node_id,
                'predecessor_id': self.predecessor.node_id,
                'data': {
                    str(k): self.decrypt_data(v) if isinstance(v, bytes) else str(v)
                    for k, v in self.data.items()
                },
                'metrics': self.metrics.copy(),
                'load': self.load,
                'timestamp': time.time()
            }
        except Exception as e:
            logging.error(f"State export failed for node {self.node_id}: {str(e)}")
            raise

    def verify_finger_table(self):
        """Verify finger table integrity"""
        try:
            for i in range(self.m):
                start = (self.node_id + 2 ** i) % self.ring_size
                if self.fingers_table[i] != self.find_successor(start):
                    return False
            return True
        except Exception as e:
            logging.error(f"Finger table verification failed for node {self.node_id}: {str(e)}")
            return False