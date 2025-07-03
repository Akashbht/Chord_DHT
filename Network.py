import hashlib
import os
import sys
from random import choice, sample
import concurrent.futures
import time
import threading
import pydotplus
from PIL import Image
import logging
from datetime import datetime
from cryptography.fernet import Fernet
import json
from prometheus_client import Counter, Gauge, start_http_server
from Node import Node


class NetworkMetrics:
    """Class to handle network-wide metrics"""

    def __init__(self):
        self.total_nodes = Gauge('chord_total_nodes', 'Total nodes in the network')
        self.total_data = Gauge('chord_total_data', 'Total data items stored')
        self.network_operations = Counter('chord_operations', 'Network operations', ['operation_type'])
        self.network_latency = Gauge('chord_latency', 'Network operation latency')
        self.network_load = Gauge('chord_load', 'Network load distribution')
        self.node_health = Gauge('chord_node_health', 'Number of healthy nodes')

    def update_metrics(self, network):
        """Update all metrics"""
        try:
            self.total_nodes.set(len(network.nodes))
            self.total_data.set(sum(len(node.data) for node in network.nodes))
            self.network_load.set(network.calculate_network_load())
            self.node_health.set(sum(1 for node in network.nodes if network.check_node_health(node)))
        except Exception as e:
            logging.error(f"Failed to update metrics: {str(e)}")


class NetworkSecurity:
    """Class to handle network security"""

    def __init__(self):
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.auth_tokens = {}
        self.token_timeout = 3600  # 1 hour

    def generate_auth_token(self, node_id):
        """Generate a new authentication token"""
        try:
            token = Fernet.generate_key()
            self.auth_tokens[node_id] = {
                'token': token,
                'timestamp': time.time()
            }
            return token
        except Exception as e:
            logging.error(f"Token generation failed: {str(e)}")
            raise

    def verify_auth_token(self, node_id, token):
        """Verify an authentication token"""
        try:
            if node_id not in self.auth_tokens:
                return False

            token_data = self.auth_tokens[node_id]
            if time.time() - token_data['timestamp'] > self.token_timeout:
                del self.auth_tokens[node_id]
                return False

            return token_data['token'] == token
        except Exception as e:
            logging.error(f"Token verification failed: {str(e)}")
            return False


class NetworkError(Exception):
    """Custom exception for network operations"""

    def __init__(self, msg='[-]Network Error!', *args, **kwargs):
        super().__init__(msg, *args, **kwargs)
        logging.error(msg)


class Network:
    def __init__(self, m, node_ids):
        """Initialize the Chord network"""
        logging.basicConfig(
            filename='chord_network.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        self.nodes = []
        self.m = m
        self.ring_size = 2 ** m
        self.metrics = NetworkMetrics()
        self.security = NetworkSecurity()
        self.cache = {}
        self.cache_timeout = 300
        self.backup_interval = 3600

        # Thread management
        self.maintenance_thread = None
        self.stop_maintenance = False
        self.lock = threading.Lock()

        try:
            self.insert_first_node(node_ids[0])
            self.first_node = self.nodes[0]
            self.start_maintenance()
            logging.info(f"Network initialized with m={m}, ring_size={self.ring_size}")
        except Exception as e:
            logging.error(f"Network initialization failed: {str(e)}")
            raise

    def __str__(self):
        return (f'Chord network:\n'
                f' |Nodes Alive: {len(self.nodes)} nodes\n'
                f' |Total Capacity: {self.ring_size} nodes\n'
                f' |Parameter m: {self.m}\n'
                f' |First Node: {self.first_node.node_id}\n')

    def start_maintenance(self):
        """Start maintenance thread"""
        self.stop_maintenance = False
        self.maintenance_thread = threading.Thread(target=self._maintenance_loop)
        self.maintenance_thread.daemon = True
        self.maintenance_thread.start()
        logging.info("Maintenance thread started")

    def _maintenance_loop(self):
        """Maintenance loop for periodic tasks"""
        while not self.stop_maintenance:
            try:
                self.fix_network_fingers()
                self.metrics.update_metrics(self)
                self.cleanup_expired_tokens()
                time.sleep(15)
            except Exception as e:
                logging.error(f"Maintenance loop error: {str(e)}")

    def cleanup_expired_tokens(self):
        """Clean up expired authentication tokens"""
        try:
            current_time = time.time()
            expired = [
                node_id for node_id, data in self.security.auth_tokens.items()
                if current_time - data['timestamp'] > self.security.token_timeout
            ]
            for node_id in expired:
                del self.security.auth_tokens[node_id]
        except Exception as e:
            logging.error(f"Token cleanup failed: {str(e)}")

    def shutdown(self):
        """Shutdown the network gracefully"""
        try:
            self.stop_maintenance = True
            if self.maintenance_thread:
                self.maintenance_thread.join()
            self.backup_network_state()
            logging.info("Network shutdown completed")
        except Exception as e:
            logging.error(f"Shutdown failed: {str(e)}")

    def get_network_info(self):
        """Get comprehensive network information"""
        try:
            return {
                'total_nodes': len(self.nodes),
                'ring_size': self.ring_size,
                'm_parameter': self.m,
                'first_node': self.first_node.node_id,
                'total_data': sum(len(node.data) for node in self.nodes),
                'network_load': self.calculate_network_load(),
                'healthy_nodes': sum(1 for node in self.nodes if self.check_node_health(node)),
                'data_distribution': {node.node_id: len(node.data) for node in self.nodes},
                'backup_status': self.verify_backups(),
                'consistency_status': self.verify_data_consistency()
            }
        except Exception as e:
            logging.error(f"Failed to get network info: {str(e)}")
            return {}

    def get_metrics(self):
        """Get network metrics"""
        try:
            total_nodes = len(self.nodes)
            total_data_items = 0
            data_distribution = {}
            successful_operations = 0
            failed_operations = 0
            errors = []
            for node in self.nodes:
                try:
                    total_data_items += len(node.data)
                    data_distribution[node.node_id] = len(node.data)
                    successful_operations += node.metrics.get('successful_lookups', 0)
                    failed_operations += node.metrics.get('failed_lookups', 0)
                except Exception as e:
                    logging.error(f"Error accessing metrics for node {getattr(node, 'node_id', '?')}: {str(e)}")
                    errors.append(f"Node {getattr(node, 'node_id', '?')}: {str(e)}")
            average_load = 0
            try:
                if total_nodes > 0:
                    average_load = sum(node.check_load() for node in self.nodes) / total_nodes
            except Exception as e:
                logging.error(f"Error calculating average load: {str(e)}")
                average_load = 0
                errors.append(f"Average load: {str(e)}")
            return {
                'total_nodes': total_nodes,
                'total_data_items': total_data_items,
                'average_load': average_load,
                'network_latency': 0.0,  # Simplified metric
                'data_distribution': data_distribution,
                'successful_operations': successful_operations,
                'failed_operations': failed_operations,
                'errors': errors if errors else 'None'
            }
        except Exception as e:
            logging.error(f"Failed to get metrics: {str(e)}")
            return {'error': str(e)}

    def calculate_network_load(self):
        """Calculate average network load"""
        try:
            if not self.nodes:
                return 0
            return sum(node.check_load() for node in self.nodes) / len(self.nodes)
        except Exception as e:
            logging.error(f"Failed to calculate network load: {str(e)}")
            return 0

    def check_node_health(self, node):
        """Check health status of a node"""
        try:
            with self.lock:
                return (node.send_heartbeat() and
                       node.verify_finger_table() and
                       hasattr(node.successor, 'backup_data'))
        except Exception:
            return False

    def insert_first_node(self, node_id):
        """Insert the first node into the network"""
        try:
            print(f'[!]Initializing network, inserting first node {node_id}\n')
            node = Node(node_id, self.m)
            self.nodes.append(node)
            logging.info(f"First node {node_id} inserted")
        except Exception as e:
            logging.error(f"First node insertion failed: {str(e)}")
            raise

    def create_node(self, node_id):
        """Create a new node"""
        try:
            return Node(node_id, self.m)
        except Exception as e:
            logging.error(f"Node creation failed: {str(e)}")
            raise

    def insert_node(self, node_id):
        """Insert a new node into the network"""
        start_time = time.time()

        try:
            with self.lock:
                if node_id > self.ring_size:
                    raise NetworkError('Node id should be smaller or equal to the networks size.')

                node = self.create_node(node_id)
                auth_token = self.security.generate_auth_token(node_id)
                node.auth_token = auth_token

                self.nodes.append(node)
                node.join(self.first_node)

                self.metrics.network_operations.labels(operation_type='node_insertion').inc()
                self.metrics.network_latency.set(time.time() - start_time)

                logging.info(f"Node {node_id} inserted successfully")
                return node

        except Exception as e:
            logging.error(f"Node insertion failed: {str(e)}")
            raise

    def delete_node(self, node_id):
        """Delete a node from the network"""
        try:
            with self.lock:
                node = next((n for n in self.nodes if n.node_id == node_id), None)
                if not node:
                    raise NetworkError(f"Node {node_id} not found")

                # Verify authentication before deletion
                if not self.security.verify_auth_token(node_id, getattr(node, 'auth_token', None)):
                    raise NetworkError("Authentication failed for node deletion")

                node.leave()
                self.nodes.remove(node)
                self.fix_network_fingers()

                # Clean up security tokens
                if node_id in self.security.auth_tokens:
                    del self.security.auth_tokens[node_id]

                self.metrics.network_operations.labels(operation_type='node_deletion').inc()
                logging.info(f"Node {node_id} deleted successfully")

        except Exception as e:
            logging.error(f"Node deletion failed: {str(e)}")
            raise

    def hash_function(self, key):
        """Hash a key to get its position in the ring"""
        try:
            num_bits = self.m
            bt = hashlib.sha1(str.encode(key)).digest()
            req_bytes = (num_bits + 7) // 8
            hashed_id = int.from_bytes(bt[:req_bytes], 'big')

            if num_bits % 8:
                hashed_id >>= 8 - num_bits % 8

            return hashed_id % self.ring_size  # Ensure the hash is within ring size
        except Exception as e:
            logging.error(f"Hashing failed: {str(e)}")
            raise

    def find_data(self, data):
        """Find data in the network"""
        start_time = time.time()

        try:
            hashed_key = self.hash_function(data)
            logging.info(f"Searching for data '{data}' with key {hashed_key}")

            # Check network cache
            if data in self.cache:
                cache_time, cache_value = self.cache[data]
                if time.time() - cache_time < self.cache_timeout:
                    return cache_value

            with self.lock:
                node = self.first_node.find_successor(hashed_key)
                encrypted_result = node.data.get(hashed_key)

                if encrypted_result:
                    # Decrypt the result before returning
                    try:
                        result = node.decrypt_data(encrypted_result)
                        self.cache[data] = (time.time(), result)
                        node.update_metrics(lookup_success=True)

                        self.metrics.network_operations.labels(operation_type='data_lookup').inc()
                        self.metrics.network_latency.set(time.time() - start_time)

                        return result
                    except Exception as e:
                        logging.error(f"Data decryption failed: {str(e)}")
                        node.update_metrics(lookup_success=False)
                        return None

            return None

        except Exception as e:
            logging.error(f"Data lookup failed: {str(e)}")
            raise

    def insert_data(self, key):
        """Insert data into the network"""
        try:
            hashed_key = self.hash_function(key)

            with self.lock:
                target_node = self.first_node.find_successor(hashed_key)

                # Encrypt data before storing
                encrypted_data = target_node.encrypt_data(key)
                target_node.data[hashed_key] = encrypted_data

                # Create backup
                target_node.backup_to_successor()

                self.metrics.network_operations.labels(operation_type='data_insertion').inc()
                logging.info(f"Data inserted: {key} -> Node {target_node.node_id}")

                # Verify data insertion
                self.verify_data_insertion(hashed_key, target_node)

                return True
        except Exception as e:
            logging.error(f"Data insertion failed: {str(e)}")
            raise

    def verify_data_insertion(self, key, node):
        """Verify that data was properly inserted"""
        try:
            correct_node = self.first_node.find_successor(key)
            if correct_node.node_id != node.node_id:
                logging.warning(f"Data inconsistency detected for key {key}")
                return False
            return True
        except Exception as e:
            logging.error(f"Data verification failed: {str(e)}")
            return False

    def print_network(self):
        """Visualize the full network structure with all successor and finger pointers, always as SVG."""
        try:
            node_count = len(self.nodes)
            if node_count > 300:
                print("[WARNING] Rendering a very large network. SVG generation may be slow and the result may be visually dense. Use a vector editor for best results.")

            # Arrange nodes in successor order
            if not self.nodes:
                print("[INFO] No nodes to visualize.")
                return

            # Find the start node (lowest node_id for determinism)
            start_node = min(self.nodes, key=lambda n: n.node_id)
            ordered_nodes = []
            visited = set()
            current = start_node
            while current.node_id not in visited:
                ordered_nodes.append(current)
                visited.add(current.node_id)
                current = current.successor
                if current is None:
                    break

            with open('graph.dot', 'w+') as f:
                f.write('digraph ChordNetwork {\n')
                f.write('rankdir=LR;\n')
                f.write('node [shape=circle, fontsize=14, penwidth=2];\n')
                f.write('edge [fontsize=10, penwidth=1.5];\n')
                f.write('graph [dpi=120];\n')

                # Add all nodes and all connections in successor order
                for node in ordered_nodes:
                    # Successor pointer (highlighted)
                    f.write(f'{node.node_id} -> {node.successor.node_id} [label="succ", color="blue", penwidth=2.5];\n')
                    # All finger table connections
                    for i, finger_node in enumerate(node.fingers_table):
                        if finger_node.node_id != node.successor.node_id:
                            f.write(f'{node.node_id} -> {finger_node.node_id} [label="f{i}", color="gray"];\n')
                    # Data count
                    data_count = len(node.data)
                    if data_count > 0:
                        f.write(f'data_{node.node_id} [label="{data_count}", shape=box, fontsize=10];\n')
                        f.write(f'{node.node_id} -> data_{node.node_id} [style=dotted, color="black"];\n')
                f.write('}\n')

            import pydotplus
            graph = pydotplus.graph_from_dot_file('graph.dot')
            graph.write_svg('network_graph.svg', prog='circo')
            print("[INFO] Full network visualized as SVG: network_graph.svg. Open this file in a browser or vector image viewer. For very large networks, use a vector editor for best experience.")
            logging.info("Full network SVG visualization created successfully")
        except Exception as e:
            logging.error(f"Network visualization failed: {str(e)}")
            print(f"Visualization error: {str(e)}")

    def fix_network_fingers(self):
        """Update finger tables across the network"""
        if len(self.nodes) > 0:
            try:
                with self.lock:
                    for node in self.nodes:
                        node.fix_fingers()
                    logging.info("Network finger tables updated")
            except Exception as e:
                logging.error(f"Network finger update failed: {str(e)}")
                raise

    def backup_network_state(self):
        """Create backup of current network state"""
        try:
            backup_data = {
                'timestamp': datetime.now().isoformat(),
                'network_info': {
                    'm': self.m,
                    'ring_size': self.ring_size,
                    'total_nodes': len(self.nodes)
                },
                'nodes': []
            }

            with self.lock:
                for node in self.nodes:
                    try:
                        node_state = node.export_state()
                        backup_data['nodes'].append(node_state)
                    except Exception as e:
                        logging.warning(f"Failed to backup node {node.node_id}: {str(e)}")
                        continue

            # Save to file with timestamp
            filename = f'network_backup_{int(time.time())}.json'
            with open(filename, 'w') as f:
                json.dump(backup_data, f, indent=2)

            print(f"Network backup saved to {filename}")
            logging.info(f"Network backup successful: {filename}")
            return True

        except Exception as e:
            logging.error(f"Network backup failed: {str(e)}")
            return False

    def restore_from_backup(self, filename):
        """Restore network state from backup"""
        try:
            with open(filename, 'r') as f:
                backup_data = json.load(f)

            with self.lock:
                # Verify backup data
                if not self.verify_backup_data(backup_data):
                    raise NetworkError("Invalid backup data")

                # Clear current network state
                self.nodes.clear()

                # Restore network parameters
                network_info = backup_data['network_info']
                self.m = network_info['m']
                self.ring_size = network_info['ring_size']

                # Restore nodes
                for node_data in backup_data['nodes']:
                    node = self.create_node(node_data['node_id'])
                    self.restore_node_state(node, node_data)
                    self.nodes.append(node)

                # Restore connections
                self.restore_network_connections(backup_data['nodes'])

            logging.info(f"Network restored from backup: {filename}")
            return True

        except Exception as e:
            logging.error(f"Network restoration failed: {str(e)}")
            return False

    def verify_backup_data(self, backup_data):
        """Verify backup data integrity"""
        try:
            required_keys = {'timestamp', 'network_info', 'nodes'}
            if not all(key in backup_data for key in required_keys):
                return False

            if not backup_data['nodes']:
                return False

            for node_data in backup_data['nodes']:
                if not all(key in node_data for key in {'node_id', 'data', 'metrics'}):
                    return False

            return True
        except Exception as e:
            logging.error(f"Backup verification failed: {str(e)}")
            return False

    def restore_node_state(self, node, node_data):
        """Restore individual node state"""
        try:
            # Restore node data with encryption
            for key, value in node_data['data'].items():
                encrypted_value = node.encrypt_data(value)
                node.data[int(key)] = encrypted_value

            # Restore metrics
            node.metrics = node_data['metrics']
            node.load = node_data.get('load', 0)
        except Exception as e:
            logging.error(f"Node state restoration failed: {str(e)}")
            raise

    def restore_network_connections(self, nodes_data):
        """Restore network connections from backup"""
        try:
            for node_data in nodes_data:
                node = next(n for n in self.nodes if n.node_id == node_data['node_id'])
                successor = next(n for n in self.nodes if n.node_id == node_data['successor_id'])
                predecessor = next(n for n in self.nodes if n.node_id == node_data['predecessor_id'])

                node.successor = successor
                node.predecessor = predecessor
                node.fingers_table[0] = successor
        except Exception as e:
            logging.error(f"Network connections restoration failed: {str(e)}")
            raise

    def check_network_health(self):
        """Perform comprehensive network health check with detailed logging, global timeout, and per-check skip on timeout."""
        import time
        import threading
        health_status = {
            'node_health': True,
            'data_consistency': True,
            'load_balance': True,
            'backup_status': True,
            'finger_tables': True
        }
        health_details = {
            'node_health': [],
            'data_consistency': [],
            'load_balance': [],
            'backup_status': [],
            'finger_tables': []
        }
        global_timeout = 30  # seconds
        start_global = time.time()
        try:
            with self.lock:
                # Node health
                logging.info("[HealthCheck] Starting node health check...")
                try:
                    for node in self.nodes:
                        if time.time() - start_global > global_timeout:
                            raise TimeoutError("Global health check timeout during node health.")
                        try:
                            if not self.check_node_health(node):
                                health_status['node_health'] = False
                                health_details['node_health'].append(f"Node {node.node_id} failed health check.")
                        except Exception as e:
                            health_status['node_health'] = False
                            health_details['node_health'].append(f"Node {node.node_id} health check error: {str(e)}")
                    if not health_details['node_health']:
                        health_details['node_health'].append("OK")
                except Exception as e:
                    health_status['node_health'] = False
                    health_details['node_health'].append(f"Node health check skipped: {str(e)}")
                logging.info("[HealthCheck] Finished node health check.")
                # Data consistency
                logging.info("[HealthCheck] Starting data consistency check...")
                start = time.time()
                try:
                    for node in self.nodes:
                        for key in node.data:
                            if time.time() - start > 10 or time.time() - start_global > global_timeout:
                                raise TimeoutError("Data consistency check timed out.")
                            correct_node = self.first_node.find_successor(key, depth=0)
                            if correct_node.node_id != node.node_id:
                                health_status['data_consistency'] = False
                                health_details['data_consistency'].append(
                                    f"Key {key} is on node {node.node_id} but should be on {correct_node.node_id}.")
                    if not health_details['data_consistency']:
                        health_details['data_consistency'].append("OK")
                except Exception as e:
                    health_status['data_consistency'] = False
                    health_details['data_consistency'].append(f"Data consistency check skipped: {str(e)}")
                logging.info("[HealthCheck] Finished data consistency check.")
                # Load balance
                logging.info("[HealthCheck] Starting load balance check...")
                try:
                    if time.time() - start_global > global_timeout:
                        raise TimeoutError("Global health check timeout during load balance.")
                    if not self.check_load_balance():
                        health_status['load_balance'] = False
                        health_details['load_balance'].append("Load is not balanced across nodes.")
                    else:
                        health_details['load_balance'].append("OK")
                except Exception as e:
                    health_status['load_balance'] = False
                    health_details['load_balance'].append(f"Load balance check skipped: {str(e)}")
                logging.info("[HealthCheck] Finished load balance check.")
                # Backup status
                logging.info("[HealthCheck] Starting backup status check...")
                try:
                    if time.time() - start_global > global_timeout:
                        raise TimeoutError("Global health check timeout during backup status.")
                    if not self.verify_backups():
                        health_status['backup_status'] = False
                        health_details['backup_status'].append("Backup verification failed.")
                    else:
                        health_details['backup_status'].append("OK")
                except Exception as e:
                    health_status['backup_status'] = False
                    health_details['backup_status'].append(f"Backup status check skipped: {str(e)}")
                logging.info("[HealthCheck] Finished backup status check.")
                # Finger tables
                logging.info("[HealthCheck] Starting finger table check...")
                start = time.time()
                try:
                    for node in self.nodes:
                        if time.time() - start > 10 or time.time() - start_global > global_timeout:
                            raise TimeoutError("Finger table check timed out.")
                        if not node.verify_finger_table():
                            health_status['finger_tables'] = False
                            health_details['finger_tables'].append(f"Node {node.node_id} finger table incorrect.")
                    if not health_details['finger_tables']:
                        health_details['finger_tables'].append("OK")
                except Exception as e:
                    health_status['finger_tables'] = False
                    health_details['finger_tables'].append(f"Finger table check skipped: {str(e)}")
                logging.info("[HealthCheck] Finished finger table check.")
            # Log details
            for k, v in health_details.items():
                for msg in v:
                    logging.info(f"Health check [{k}]: {msg}")
            return {'status': health_status, 'details': health_details}
        except Exception as e:
            logging.error(f"Health check failed: {str(e)}")
            return {'status': {k: False for k in health_status}, 'details': {'error': str(e)}}

    def verify_data_consistency(self):
        """Verify data consistency across the network"""
        try:
            with self.lock:
                for node in self.nodes:
                    for key in node.data:
                        correct_node = self.first_node.find_successor(key)
                        if correct_node.node_id != node.node_id:
                            return False
            return True
        except Exception as e:
            logging.error(f"Data consistency check failed: {str(e)}")
            return False

    def verify_finger_tables(self):
        """Verify finger table integrity across the network"""
        try:
            with self.lock:
                return all(node.verify_finger_table() for node in self.nodes)
        except Exception as e:
            logging.error(f"Finger table verification failed: {str(e)}")
            return False

    def check_load_balance(self):
        """Check if load is balanced across nodes"""
        try:
            if not self.nodes:
                return True

            loads = [node.check_load() for node in self.nodes]
            avg_load = sum(loads) / len(loads)
            max_deviation = 0.2  # 20% threshold

            return all(abs(load - avg_load) <= (avg_load * max_deviation) for load in loads)
        except Exception as e:
            logging.error(f"Load balance check failed: {str(e)}")
            return False

    def verify_backups(self):
        """Verify backup integrity across the network"""
        try:
            with self.lock:
                return all(
                    hasattr(node.successor, 'backup_data') and
                    node.successor.backup_data.get('node_id') == node.node_id
                    for node in self.nodes
                )
        except Exception as e:
            logging.error(f"Backup verification failed: {str(e)}")
            return False

    def balance_network_load(self):
        """Balance load across network nodes (improved: move to underloaded nodes, not just successor, with verbose logging and infinite loop protection)"""
        if len(self.nodes) < 2:
            logging.info("Not enough nodes to balance load.")
            return

        try:
            with self.lock:
                loads = [(node, node.check_load()) for node in self.nodes]
                avg_load = sum(load for _, load in loads) / len(loads)
                max_deviation = 0.2  # 20% threshold

                # Sort nodes by load
                overloaded = [n for n, l in loads if l > avg_load * (1 + max_deviation)]
                underloaded = [n for n, l in loads if l < avg_load * (1 - max_deviation)]

                if not overloaded or not underloaded:
                    logging.info("No overloaded or underloaded nodes. Load is balanced.")
                    return

                # Sort underloaded by how underloaded they are (most underloaded first)
                underloaded = sorted(underloaded, key=lambda n: n.check_load())

                for o_node in overloaded:
                    o_keys = list(o_node.data.keys())
                    o_load = o_node.check_load()
                    logging.info(f"Overloaded node {o_node.node_id} initial load: {o_load:.4f}")
                    while o_load > avg_load * (1 + max_deviation) and underloaded:
                        u_node = underloaded[0]
                        u_load = u_node.check_load()
                        move_count = int(min(
                            (o_load - avg_load) * o_node.ring_size,
                            (avg_load - u_load) * u_node.ring_size
                        ))
                        if move_count <= 0 or not o_keys:
                            logging.info(f"No keys to move from node {o_node.node_id} to node {u_node.node_id}. Breaking loop.")
                            break
                        keys_to_move = []
                        for key in o_keys:
                            correct_node = self.first_node.find_successor(key)
                            if correct_node.node_id == u_node.node_id:
                                keys_to_move.append(key)
                                if len(keys_to_move) >= move_count:
                                    break
                        if not keys_to_move:
                            logging.info(f"No eligible keys to move from node {o_node.node_id} to node {u_node.node_id}. Breaking loop.")
                            break
                        for key in keys_to_move:
                            u_node.data[key] = o_node.data[key]
                            del o_node.data[key]
                        logging.info(f"Moved {len(keys_to_move)} keys from node {o_node.node_id} (load {o_load:.4f}) to node {u_node.node_id} (load {u_load:.4f})")
                        # Update loads and keys
                        o_keys = list(o_node.data.keys())
                        o_load = o_node.check_load()
                        u_load = u_node.check_load()
                        logging.info(f"After move: node {o_node.node_id} load: {o_load:.4f}, node {u_node.node_id} load: {u_load:.4f}")
                        # If u_node is now within threshold, remove from underloaded
                        if u_load >= avg_load * (1 - max_deviation):
                            logging.info(f"Node {u_node.node_id} is now within threshold. Removing from underloaded list.")
                            underloaded.pop(0)
                    # Update backups after redistribution
                    o_node.backup_to_successor()
                    for u_node in underloaded:
                        u_node.backup_to_successor()

                logging.info("Improved load balancing completed")
        except Exception as e:
            logging.error(f"Load balancing failed: {str(e)}")
            raise

    def get_all_network_data(self):
        """Get all data stored in the network"""
        network_data = {}
        try:
            with self.lock:
                for node in self.nodes:
                    node_data = {}
                    for key, value in node.data.items():
                        try:
                            if isinstance(value, bytes):
                                decrypted_value = node.decrypt_data(value)
                            else:
                                decrypted_value = value
                            node_data[key] = decrypted_value
                        except Exception as e:
                            node_data[key] = f"[Encrypted: {value}]"
                            logging.warning(f"Failed to decrypt data in node {node.node_id}: {str(e)}")

                    network_data[node.node_id] = node_data

            return network_data
        except Exception as e:
            logging.error(f"Failed to get network data: {str(e)}")
            return {}

    def cleanup(self):
        """Cleanup network resources"""
        try:
            # Stop maintenance thread
            self.stop_maintenance = True
            if self.maintenance_thread:
                self.maintenance_thread.join()

            # Create final backup
            self.backup_network_state()

            # Clear sensitive data
            for node in self.nodes:
                node.data.clear()
                node.backup_data.clear()
                node.cache.clear()

            # Clear network cache
            self.cache.clear()

            logging.info("Network cleanup completed")
        except Exception as e:
            logging.error(f"Network cleanup failed: {str(e)}")
            raise

# Patch Node.find_successor to add recursion depth limit
import types
old_find_successor = None
if not hasattr(Node, '_patched_find_successor'):
    old_find_successor = Node.find_successor
    def find_successor_with_depth(self, key, depth=0, max_depth=32):
        if depth > max_depth:
            raise RecursionError(f"find_successor exceeded max recursion depth at node {self.node_id}")
        try:
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
                result = next_node.find_successor(key) if not hasattr(next_node, 'find_successor_with_depth') else next_node.find_successor_with_depth(key, depth+1, max_depth)
                self.cache_store(key, result)
                return result
        except Exception as e:
            logging.error(f"find_successor failed at node {self.node_id}: {str(e)}")
            raise
    Node.find_successor_with_depth = find_successor_with_depth
    Node._patched_find_successor = True
