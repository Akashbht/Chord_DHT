import os
import json
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from Network import Network, NetworkError
from Node import Node

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'chordsecret')
STATE_FILE = 'network_state.json'

# Global network instance
network = None
node_ids = []

# Helper to load/save state

def save_state():
    if network:
        with open(STATE_FILE, 'w') as f:
            json.dump({
                'm': network.m,
                'node_ids': [n.node_id for n in network.nodes],
                'data': {n.node_id: {str(k): v for k, v in n.data.items()} for n in network.nodes}
            }, f)

def load_state():
    global network, node_ids
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
            m = state['m']
            node_ids = state['node_ids']
            network = Network(m, node_ids)
            # Restore data
            for n in network.nodes:
                ndata = state['data'].get(str(n.node_id), {})
                n.data = {int(k): v for k, v in ndata.items()}
    else:
        network = None
        node_ids = []

@app.route('/')
def dashboard():
    if not network:
        return render_template('setup.html')
    info = network.get_network_info()
    metrics = network.get_metrics()
    return render_template('dashboard.html', info=info, metrics=metrics, node_ids=[n.node_id for n in network.nodes])

@app.route('/setup', methods=['POST'])
def setup_network():
    global network, node_ids
    m = int(request.form['m'])
    n_nodes = int(request.form['nodes'])
    node_ids = list(range(n_nodes))
    network = Network(m, node_ids)
    save_state()
    flash('Network initialized!', 'success')
    return redirect(url_for('dashboard'))

# More endpoints for each action will be added here

if __name__ == '__main__':
    load_state()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 10000))) 