import hashlib
import os
import sys
from random import choice, sample
import concurrent.futures
import time
import threading
import pydotplus
from PIL import Image
import pyfiglet
import logging
import argparse
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from prometheus_client import start_http_server
import signal
import json
from datetime import datetime

from Node import Node
from Network import Network, NetworkError


class ChordInterface:
    """Main interface for the Chord DHT network"""

    def __init__(self):
        # Initialize console and styling
        self.console = Console()
        self.network = None
        self.node_ids = []
        self.layout = Layout()

        # Configure logging
        logging.basicConfig(
            filename='chord_main.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        # Initialize status flags
        self.is_running = True
        self.need_refresh = False

    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown on signal"""
        self.console.print("\n[yellow]Initiating graceful shutdown...[/yellow]")
        self.is_running = False
        if self.network:
            try:
                self.network.cleanup()
                self.backup_and_exit()
            except Exception as e:
                logging.error(f"Shutdown error: {str(e)}")
        sys.exit(0)

    def backup_and_exit(self):
        """Backup network state and exit"""
        try:
            self.console.print("[yellow]Backing up network state...[/yellow]")
            success = self.network.backup_network_state()
            if success:
                self.console.print("[green]Backup completed successfully[/green]")
            else:
                self.console.print("[red]Backup failed[/red]")
        except Exception as e:
            self.console.print(f"[red]Backup failed: {str(e)}[/red]")
        finally:
            self.console.print("[green]Goodbye![/green]")

    def show_banner(self):
        """Display application banner"""
        try:
            ascii_banner = pyfiglet.figlet_format('CHORD DHT')
            self.console.print(Panel(ascii_banner, style="bold blue"))
            self.console.print(Panel("Developed by: Team Glitch", style="italic"))
        except Exception as e:
            logging.error(f"Banner display error: {str(e)}")
            self.console.print("[red]Failed to display banner[/red]")

    def create_network(self):
        """Initialize Chord network with enhanced setup"""
        try:
            sys.setrecursionlimit(10000000)
            self.show_banner()

            network_params = self.get_network_parameters()

            with Progress() as progress:
                task = progress.add_task("[cyan]Initializing network...", total=100)
                self.setup_network(network_params, progress, task)

            self.console.print("[green]Network initialization complete![/green]")

            try:
                start_http_server(8000)
                self.console.print("[green]Metrics server started on port 8000[/green]")
            except Exception as e:
                logging.error(f"Metrics server failed to start: {str(e)}")
                self.console.print("[yellow]Warning: Metrics server failed to start[/yellow]")

            self.show_menu()

        except Exception as e:
            logging.critical(f"Network creation failed: {str(e)}")
            self.console.print(f"[red]Critical error: {str(e)}[/red]")
            sys.exit(1)

    def get_network_parameters(self):
        """Get network parameters from arguments or user input"""
        parser = argparse.ArgumentParser(description='Chord DHT Network')
        parser.add_argument('--m', type=int, help='Network parameter m')
        parser.add_argument('--nodes', type=int, help='Number of initial nodes')
        parser.add_argument('--data', type=int, help='Amount of test data')

        args = parser.parse_args()

        if all([args.m, args.nodes, args.data]):
            self.validate_parameters(args)
            return args

        params = {}
        params['m'] = args.m or self.get_valid_input(
            'Enter m parameter: ',
            lambda x: 0 < int(x) <= 32,
            'Parameter m must be between 1 and 32'
        )

        params['nodes'] = args.nodes or self.get_valid_input(
            'Enter number of nodes: ',
            lambda x: 0 < int(x) <= 2 ** params['m'],
            'Invalid number of nodes'
        )

        params['data'] = args.data or self.get_valid_input(
            'Enter amount of test data: ',
            lambda x: int(x) >= 0,
            'Amount must be non-negative'
        )

        return argparse.Namespace(**params)

    def validate_parameters(self, params):
        """Validate network parameters"""
        if not 0 < params.m <= 32:
            raise ValueError("Parameter m must be between 1 and 32")
        if not 0 < params.nodes <= 2 ** params.m:
            raise ValueError(f"Number of nodes must be between 1 and {2 ** params.m}")
        if params.data < 0:
            raise ValueError("Amount of test data must be non-negative")

    def get_valid_input(self, prompt, validator, error_message):
        """Get and validate user input"""
        while True:
            try:
                value = input(prompt)
                if validator(int(value)):
                    return int(value)
                self.console.print(f"[red]{error_message}[/red]")
            except ValueError:
                self.console.print("[red]Please enter a valid number[/red]")

    def setup_network(self, params, progress, task):
        """Setup network with given parameters"""
        try:
            Node.m = params.m
            Node.ring_size = 2 ** params.m

            progress.update(task, advance=20)

            self.node_ids = sample(range(Node.ring_size), params.nodes)
            self.network = Network(params.m, self.node_ids)

            progress.update(task, advance=30)

            # Initialize and join nodes
            for node_id in self.node_ids[1:]:
                node = self.network.create_node(node_id)
                node.join(self.network.first_node)
                self.network.nodes.append(node)
                progress.update(task, advance=30 / len(self.node_ids[1:]))

            # Generate test data
            if params.data > 0:
                self.generate_test_data(params.data, progress, task)

            progress.update(task, completed=100)

        except Exception as e:
            logging.error(f"Setup network failed: {str(e)}")
            raise

    def generate_test_data(self, amount, progress, task):
        """Generate and insert test data"""
        try:
            extensions = ['.txt', '.png', '.doc', '.mov', '.jpg', '.py']
            progress.update(task, description="[cyan]Generating test data...")

            for i in range(amount):
                filename = f'file_{i}{choice(extensions)}'
                self.network.insert_data(filename)
                progress.update(task, advance=20 / amount)

        except Exception as e:
            logging.error(f"Test data generation failed: {str(e)}")
            raise

    def show_menu(self):
        """Display interactive menu"""
        while self.is_running:
            table = Table(title="Chord DHT Operations")
            table.add_column("Option", style="cyan")
            table.add_column("Description", style="green")

            menu_items = [
                ("1", "Insert new node"),
                ("2", "Find data"),
                ("3", "Insert data"),
                ("4", "Print network graph"),
                ("5", "Print network info"),
                ("6", "Delete node"),
                ("7", "Show metrics"),
                ("8", "Backup network"),
                ("9", "Load balancing"),
                ("10", "Network health check"),
                ("11", "Show all network data"),
                ("12", "Exit")
            ]

            for option, description in menu_items:
                table.add_row(option, description)

            self.console.print(table)

            try:
                choice = input('Select an operation: ')
                self.handle_menu_choice(choice)
            except Exception as e:
                logging.error(f"Menu operation failed: {str(e)}")
                self.console.print(f"[red]Operation failed: {str(e)}[/red]")

    def handle_menu_choice(self, choice):
        """Handle menu selection"""
        handlers = {
            '1': self.handle_node_insertion,
            '2': self.handle_data_search,
            '3': self.handle_data_insertion,
            '4': self.handle_network_visualization,
            '5': self.handle_network_info,
            '6': self.handle_node_deletion,
            '7': self.handle_metrics_display,
            '8': self.handle_network_backup,
            '9': self.handle_load_balancing,
            '10': self.handle_health_check,
            '11': self.handle_show_network_data,
            '12': self.handle_exit
        }

        handler = handlers.get(choice)
        if handler:
            try:
                handler()
            except Exception as e:
                logging.error(f"Handler failed: {str(e)}")
                self.console.print(f"[red]Operation failed: {str(e)}[/red]")
        else:
            self.console.print("[red]Invalid choice[/red]")

    def handle_node_insertion(self):
        """Handle node insertion"""
        try:
            node_id = self.get_valid_input(
                '[->]Enter node id: ',
                lambda x: x not in self.node_ids and x < Node.ring_size,
                'Invalid node ID or node already exists'
            )

            with self.console.status("[bold green]Inserting node..."):
                self.network.insert_node(node_id)
                self.node_ids.append(node_id)

            self.console.print(f"[green]Node {node_id} inserted successfully[/green]")

        except Exception as e:
            raise NetworkError(f"Node insertion failed: {str(e)}")

    def handle_data_search(self):
        """Handle data search"""
        query = input('[->]Search data: ')
        with self.console.status("[bold green]Searching..."):
            result = self.network.find_data(query)

        if result:
            self.console.print(f"[green]Found: {result}[/green]")
        else:
            self.console.print("[yellow]Data not found[/yellow]")

    def handle_data_insertion(self):
        """Handle data insertion"""
        data = input('[->]Enter data: ')
        with self.console.status("[bold green]Inserting data..."):
            self.network.insert_data(data)
        self.console.print("[green]Data inserted successfully[/green]")

    def handle_network_visualization(self):
        """Handle network visualization"""
        if len(self.network.nodes) > 0:
            with self.console.status("[bold green]Generating network graph..."):
                self.network.print_network()
        else:
            self.console.print("[yellow]Network is empty[/yellow]")

    def handle_network_info(self):
        """Display network information"""
        info = self.network.get_network_info()
        self.console.print(Panel(str(info), title="Network Information"))

    def handle_node_deletion(self):
        """Handle node deletion"""
        try:
            node_id = self.get_valid_input(
                '[->]Enter node to delete: ',
                lambda x: x in self.node_ids,
                'Node not found'
            )

            with self.console.status("[bold green]Deleting node..."):
                self.network.delete_node(node_id)
                self.node_ids.remove(node_id)

            self.console.print(f"[green]Node {node_id} deleted successfully[/green]")

        except Exception as e:
            raise NetworkError(f"Node deletion failed: {str(e)}")

    def handle_metrics_display(self):
        """Display network metrics"""
        try:
            metrics = self.network.get_metrics()
            table = Table(title="Network Metrics")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")

            for metric, value in metrics.items():
                table.add_row(str(metric), str(value))

            self.console.print(table)

        except Exception as e:
            raise NetworkError(f"Failed to display metrics: {str(e)}")

    def handle_network_backup(self):
        """Handle network backup"""
        with self.console.status("[bold green]Creating backup..."):
            success = self.network.backup_network_state()

        if success:
            self.console.print("[green]Backup created successfully[/green]")
        else:
            self.console.print("[red]Backup failed[/red]")

    def handle_load_balancing(self):
        """Handle load balancing"""
        with self.console.status("[bold green]Balancing network load..."):
            self.network.balance_network_load()
        self.console.print("[green]Load balancing completed[/green]")

    def handle_health_check(self):
        """Handle network health check"""
        with self.console.status("[bold green]Checking network health..."):
            health_info = self.network.check_network_health()

        table = Table(title="Network Health")
        table.add_column("Check", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="yellow")

        status_details = {
            'node_health': "All nodes responding",
            'data_consistency': "Data properly distributed",
            'load_balance': "Load evenly distributed",
            'backup_status': "Backups maintained",
            'finger_tables': "Routing tables correct"
        }

        for check in status_details.keys():
            status = health_info['status'].get(check, False)
            details_list = health_info['details'].get(check, [])
            details = "\n".join(details_list) if isinstance(details_list, list) else str(details_list)
            table.add_row(
                check,
                "✓" if status else "✗",
                details if details else (status_details[check] if status else "Issue detected")
            )

        self.console.print(table)

    def handle_show_network_data(self):
        """Display all network data"""
        try:
            network_data = self.network.get_all_network_data()

            table = Table(title="Network Data Distribution")
            table.add_column("Node ID", style="cyan")
            table.add_column("Data Count", style="green")
            table.add_column("Data Items", style="yellow")

            for node_id, data in sorted(network_data.items()):
                data_items = "\n".join([f"{k}: {v}" for k, v in data.items()])
                table.add_row(
                    str(node_id),
                    str(len(data)),
                    data_items or "No data"
                )

            self.console.print(table)
        except Exception as e:
            raise NetworkError(f"Failed to display network data: {str(e)}")

    def handle_exit(self):
        """Handle application exit"""
        self.is_running = False
        self.backup_and_exit()
        sys.exit(0)


def main():
    """Main entry point"""
    try:
        interface = ChordInterface()
        interface.create_network()
    except Exception as e:
        logging.critical(f"Application failed: {str(e)}")
        Console().print(f"[red]Critical error: {str(e)}[/red]")
        sys.exit(1)


if __name__ == '__main__':
    main()