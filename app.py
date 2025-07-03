import os
import json
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
from Network import Network, NetworkError
from Node import Node

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'chordsecret')
STATE_FILE = 'network_state.json'

# Global network instance
network = None
node_ids = []

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
            for n in network.nodes:
                ndata = state['data'].get(str(n.node_id), {})
                n.data = {int(k): v for k, v in ndata.items()}
    else:
        network = None
        node_ids = []

@app.before_first_request
def before_first_request():
    load_state()

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

@app.route('/insert_node', methods=['POST'])
def insert_node():
    try:
        node_id = int(request.form['node_id'])
        network.insert_node(node_id)
        save_state()
        flash(f'Node {node_id} inserted successfully.', 'success')
    except Exception as e:
        flash(f'Insert node failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        data = request.form['data']
        network.insert_data(data)
        save_state()
        flash('Data inserted successfully.', 'success')
    except Exception as e:
        flash(f'Insert data failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/find_data', methods=['POST'])
def find_data():
    try:
        query = request.form['query']
        result = network.find_data(query)
        if result:
            flash(f'Found: {result}', 'success')
        else:
            flash('Data not found.', 'warning')
    except Exception as e:
        flash(f'Find data failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/delete_node', methods=['POST'])
def delete_node():
    try:
        node_id = int(request.form['delete_node_id'])
        network.delete_node(node_id)
        save_state()
        flash(f'Node {node_id} deleted successfully.', 'success')
    except Exception as e:
        flash(f'Delete node failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/backup_network', methods=['POST'])
def backup_network():
    try:
        success = network.backup_network_state()
        if success:
            flash('Backup created successfully.', 'success')
        else:
            flash('Backup failed.', 'danger')
    except Exception as e:
        flash(f'Backup failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/load_balancing', methods=['POST'])
def load_balancing():
    try:
        network.balance_network_load()
        save_state()
        flash('Load balancing completed.', 'success')
    except Exception as e:
        flash(f'Load balancing failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/network_health', methods=['POST'])
def network_health():
    try:
        health_info = network.check_network_health()
        details = '\n'.join([f"{k}: {v}" for k, v in health_info['details'].items()])
        flash(f'Network Health: {health_info["status"]}\n{details}', 'info')
    except Exception as e:
        flash(f'Network health check failed: {str(e)}', 'danger')
    return redirect(url_for('dashboard'))

@app.route('/show_network_data', methods=['GET'])
def show_network_data():
    try:
        network_data = network.get_all_network_data()
        return render_template('network_data.html', network_data=network_data)
    except Exception as e:
        flash(f'Failed to display network data: {str(e)}', 'danger')
        return redirect(url_for('dashboard'))

@app.route('/show_network_graph', methods=['GET'])
def show_network_graph():
    try:
        network.print_network()
        if os.path.exists('network_graph.svg'):
            return send_file('network_graph.svg', mimetype='image/svg+xml')
        else:
            flash('Network graph not available.', 'warning')
            return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f'Failed to generate network graph: {str(e)}', 'danger')
        return redirect(url_for('dashboard'))

# REST API endpoints
@app.route('/api/info')
def api_info():
    return jsonify(network.get_network_info())

@app.route('/api/metrics')
def api_metrics():
    return jsonify(network.get_metrics())

@app.route('/api/health')
def api_health():
    return jsonify(network.check_network_health())

@app.route('/api/data')
def api_data():
    return jsonify(network.get_all_network_data())

@app.route('/api/insert_node', methods=['POST'])
def api_insert_node():
    try:
        node_id = int(request.json['node_id'])
        network.insert_node(node_id)
        save_state()
        return jsonify({'status': 'success', 'message': f'Node {node_id} inserted.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/insert_data', methods=['POST'])
def api_insert_data():
    try:
        data = request.json['data']
        network.insert_data(data)
        save_state()
        return jsonify({'status': 'success', 'message': 'Data inserted.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/find_data', methods=['POST'])
def api_find_data():
    try:
        query = request.json['query']
        result = network.find_data(query)
        if result:
            return jsonify({'status': 'success', 'result': result})
        else:
            return jsonify({'status': 'not_found', 'message': 'Data not found.'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/delete_node', methods=['POST'])
def api_delete_node():
    try:
        node_id = int(request.json['node_id'])
        network.delete_node(node_id)
        save_state()
        return jsonify({'status': 'success', 'message': f'Node {node_id} deleted.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/backup_network', methods=['POST'])
def api_backup_network():
    try:
        success = network.backup_network_state()
        if success:
            return jsonify({'status': 'success', 'message': 'Backup created.'})
        else:
            return jsonify({'status': 'error', 'message': 'Backup failed.'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/load_balancing', methods=['POST'])
def api_load_balancing():
    try:
        network.balance_network_load()
        save_state()
        return jsonify({'status': 'success', 'message': 'Load balancing completed.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/network_graph')
def api_network_graph():
    try:
        network.print_network()
        if os.path.exists('network_graph.svg'):
            return send_file('network_graph.svg', mimetype='image/svg+xml')
        else:
            return jsonify({'status': 'error', 'message': 'Network graph not available.'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

if __name__ == '__main__':
    load_state()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 10000))) 