# Chord DHT (Distributed Hash Table)

A comprehensive implementation of the Chord protocol for distributed hash tables with both command-line and web interfaces. This project provides a complete DHT system with features like data encryption, network visualization, metrics monitoring, and fault tolerance.

## 🌟 Features

### Core Chord DHT Features
- **Distributed Hash Table**: Complete implementation of the Chord protocol
- **Scalable Architecture**: Configurable ring size with parameter `m` (supports up to 2^32 nodes)
- **Consistent Hashing**: Efficient data distribution and lookup using SHA-1 hashing
- **Finger Tables**: Optimized routing with logarithmic lookup complexity O(log N)
- **Fault Tolerance**: Automatic node failure detection and recovery
- **Data Replication**: Backup data storage for reliability

### Advanced Features
- **Data Encryption**: Built-in data encryption using Fernet symmetric encryption
- **Metrics & Monitoring**: Prometheus metrics integration for performance monitoring
- **Network Visualization**: Generate network topology graphs (DOT, PNG, SVG formats)
- **Load Balancing**: Automatic load distribution across nodes
- **Caching System**: Intelligent caching for improved lookup performance
- **Authentication**: Token-based security for network operations

### User Interfaces
- **Rich CLI Interface**: Interactive command-line interface with colorful output
- **Web Dashboard**: Browser-based interface for network management
- **Real-time Updates**: Live monitoring of network status and metrics

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Akashbht/Chord_DHT.git
   cd Chord_DHT
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **For development:**
   ```bash
   pip install -r requirements-dev.txt
   ```

### Basic Usage

#### Command Line Interface

1. **Start the interactive CLI:**
   ```bash
   python Main.py
   ```

2. **Or with command line arguments:**
   ```bash
   python Main.py --m 5 --nodes 8 --data 100
   ```

#### Web Interface

1. **Start the web server:**
   ```bash
   python app.py
   ```
   Or using Gunicorn for production:
   ```bash
   gunicorn app:app
   ```

2. **Open your browser and go to:**
   ```
   http://localhost:5000
   ```

## 📖 Detailed Usage

### Network Configuration

- **Parameter m**: Determines the ring size (2^m). Choose based on expected network size
- **Number of Nodes**: Initial nodes to create in the network
- **Test Data**: Amount of sample data to insert for testing

### CLI Commands

The interactive CLI provides the following options:

1. **Insert Data**: Add key-value pairs to the DHT
2. **Search Data**: Find data by key with lookup statistics
3. **Delete Data**: Remove data from the network
4. **Add Node**: Insert a new node into the network
5. **Remove Node**: Remove a node from the network
6. **Network Info**: Display current network status
7. **Show Finger Tables**: View routing tables for all nodes
8. **Generate Graph**: Create network topology visualization
9. **Load Test**: Run performance tests with bulk operations
10. **Backup Network**: Save current network state

### Web Interface Features

- **Dashboard**: Real-time network overview
- **Node Management**: Add/remove nodes through web interface
- **Data Operations**: Insert, search, and manage data
- **Network Visualization**: Interactive network topology
- **Metrics Dashboard**: Performance and health monitoring

## 🏗️ Architecture

### Core Components

1. **Node.py**: Individual node implementation
   - Finger table management
   - Data storage and encryption
   - Successor/predecessor maintenance
   - Metrics collection

2. **Network.py**: Network management
   - Node coordination
   - Network maintenance
   - Fault tolerance
   - Load balancing
   - Security management

3. **Main.py**: Command-line interface
   - Interactive CLI with Rich library
   - Network setup and configuration
   - Testing and benchmarking tools

4. **app.py**: Web interface
   - Flask-based web server
   - RESTful API endpoints
   - State persistence

### Network Topology

```
Node Structure:
┌─────────────┐
│    Node     │
│  ID: X      │
│             │
│ Predecessor │ ←── Previous node in ring
│ Successor   │ ←── Next node in ring
│ Finger[0]   │ ←── Node at (n + 2^0) mod 2^m
│ Finger[1]   │ ←── Node at (n + 2^1) mod 2^m
│ ...         │
│ Finger[m-1] │ ←── Node at (n + 2^(m-1)) mod 2^m
│             │
│ Data Store  │ ←── Local key-value storage
│ Cache       │ ←── Performance optimization
└─────────────┘
```

## 🛠️ Configuration

### Environment Variables

- `SECRET_KEY`: Flask secret key for web interface
- `PORT`: Web server port (default: 5000)

### Network Parameters

- **m**: Ring size parameter (1-32)
  - m=5: Ring size 32, suitable for small networks
  - m=10: Ring size 1024, suitable for medium networks
  - m=20: Ring size ~1M, suitable for large networks

### Performance Tuning

- **Cache Timeout**: Adjust `cache_timeout` in Node class
- **Backup Interval**: Modify `backup_interval` in Network class
- **Maintenance Frequency**: Change sleep interval in maintenance loop

## 📊 Monitoring & Metrics

### Prometheus Metrics

Access metrics at `http://localhost:8000/metrics`:

- `chord_total_nodes`: Total nodes in network
- `chord_total_data`: Total data items stored
- `chord_operations`: Network operations counter
- `chord_latency`: Operation latency
- `chord_load`: Load distribution
- `chord_node_health`: Healthy nodes count

### Network Visualization

Generate network graphs:
```bash
# In CLI, select option 8 to generate graph
# Files created: graph.dot, network_graph.png, network_graph.svg
```

## 🧪 Testing

### Load Testing

The CLI includes built-in load testing:
1. Start the network
2. Select "Load Test" option
3. Specify number of operations
4. Monitor performance metrics

### Development Testing

```bash
# Run with development dependencies
pytest tests/
coverage run -m pytest
coverage report
```

## 🚀 Deployment

### Local Development
```bash
python app.py
```

### Production with Gunicorn
```bash
gunicorn --bind 0.0.0.0:8000 app:app
```

### Heroku Deployment
The project includes a `Procfile` for Heroku deployment:
```bash
git push heroku main
```

### Render.com Deployment
Configuration included in `render.yaml`:
```bash
# Deploy through Render.com dashboard
```

## 🔧 Development

### Project Structure
```
Chord_DHT/
├── Main.py              # CLI interface
├── Node.py              # Node implementation
├── Network.py           # Network management
├── app.py               # Web interface
├── requirements.txt     # Dependencies
├── requirements-dev.txt # Development dependencies
├── setup.py            # Package configuration
├── templates/          # HTML templates
│   ├── dashboard.html
│   ├── setup.html
│   └── network_data.html
├── Procfile           # Heroku deployment
├── render.yaml        # Render.com deployment
└── README.md          # This file
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Add docstrings for functions and classes
- Format code with black: `black .`

## 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

## 👥 Authors

- **Akash Bhat** - Initial development
- **Team Glitch** - Project contributors

## 🙏 Acknowledgments

- Chord protocol paper by Stoica et al.
- Python community for excellent libraries
- Contributors and testers

## 🐛 Known Issues

- Large networks (m > 20) may require increased system resources
- Web interface state persistence requires proper file permissions
- Network visualization may be slow for very large networks

## 🔮 Future Enhancements

- [ ] Docker containerization
- [ ] Kubernetes deployment manifests
- [ ] Enhanced security with TLS
- [ ] REST API documentation
- [ ] Real-time web interface updates
- [ ] Performance benchmarking suite
- [ ] Network simulation tools

## 📞 Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review the code comments for implementation details

---

**Note**: This is an educational implementation of the Chord protocol. For production use, consider additional security, performance, and reliability measures.