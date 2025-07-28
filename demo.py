#!/usr/bin/env python3
"""
Demo script for Chord DHT functionality
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Network import Network
from Node import Node
import time

def demo_chord_dht():
    """Demonstrate Chord DHT functionality"""
    print("🌟 Chord DHT Demo")
    print("=" * 50)
    
    # Create network
    print("1️⃣ Creating Chord network (m=4, ring size=16)...")
    network = Network(4, [1, 5, 9, 13])
    
    print(f"   ✓ Network created with {len(network.nodes)} initial nodes")
    print(f"   ✓ Ring size: {network.ring_size}")
    print(f"   ✓ Node IDs: {[n.node_id for n in network.nodes]}")
    
    # Add more nodes
    print("\n2️⃣ Adding more nodes to the network...")
    new_nodes = [3, 7, 11]
    for node_id in new_nodes:
        network.insert_node(node_id)
        print(f"   ✓ Added node {node_id}")
    
    print(f"   ✓ Total nodes now: {len(network.nodes)}")
    
    # Insert data
    print("\n3️⃣ Inserting data into the network...")
    test_data = ["apple", "banana", "cherry", "date", "elderberry"]
    
    for data in test_data:
        network.insert_data(data)
        print(f"   ✓ Inserted: {data}")
    
    # Search data
    print("\n4️⃣ Searching for data...")
    for data in ["apple", "cherry", "unknown"]:
        result = network.find_data(data)
        status = "FOUND" if result else "NOT FOUND"
        print(f"   🔍 Search '{data}': {status}")
    
    # Show network info
    print("\n5️⃣ Network Information:")
    info = network.get_network_info()
    for key, value in info.items():
        print(f"   📊 {key}: {value}")
    
    # Show data distribution
    print("\n6️⃣ Data Distribution:")
    data_dist = network.get_all_network_data()
    for node_id, data in sorted(data_dist.items()):
        print(f"   📦 Node {node_id}: {len(data)} items {list(data.keys())}")
    
    # Cleanup
    print("\n7️⃣ Cleaning up...")
    network.cleanup()
    print("   ✓ Network cleaned up successfully")
    
    print("\n🎉 Demo completed successfully!")
    print("\nTo run the interactive CLI: python Main.py")
    print("To run the web interface: python app.py")

if __name__ == "__main__":
    try:
        demo_chord_dht()
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        sys.exit(1)