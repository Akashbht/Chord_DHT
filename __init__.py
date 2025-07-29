"""
Chord DHT (Distributed Hash Table) Implementation

A comprehensive implementation of the Chord protocol for distributed hash tables
with both command-line and web interfaces.

Author: Akash Bhat
Team: Team Glitch
"""

from .Node import Node
from .Network import Network
from .Main import ChordInterface

__version__ = "1.0.0"
__author__ = "Akash Bhat"
__email__ = ""

__all__ = [
    "Node", 
    "Network", 
    "ChordInterface"
]