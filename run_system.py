#!/usr/bin/env python3
"""
Main system runner script.
Orchestrates the entire sentiment analysis streaming system.
"""

import os
import sys
import logging
import argparse
import subprocess
import time
import signal
from typing import Dict, List, Any
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemRunner:
    """Main system orchestrator."""

    def __init__(self):
        """Initialize system runner."""
        self.processes = {}
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load system configuration."""
        try:
            with open('config/system_config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.info("System config not found, using defaults")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default system configuration."""
        return {
            'services': {
                'infrastructure': {
                    'command': 'docker-compose up -d',
                    'description': 'Start infrastructure services'
                },
                'initialization': {
                    'command': 'python scripts/init_system.py',
                    'description': 'Initialize system tables and topics'
                },
                'model_server': {
                    'command': 'python models/model_server.py',
                    'description': 'Start ML model server'
                },
                'webhook_collector': {
                    'command': 'python collectors/run_webhook_collector.py',
                    'description': 'Start webhook collector'
                },
                'reddit_collector': {
                    'command': 'python collectors/run_reddit_collector.py',
                    'description': 'Start Reddit collector'
                },
                'twitter_collector': {
                    'command': 'python collectors/run_twitter_collector.py',
                    'description': 'Start Twitter collector'
                },
                'preprocess_job': {
                    'command': 'spark-submit --master local[*] streaming/preprocess_job.py',
                    'description': 'Start preprocess streaming job'
                },
                'sentiment_job': {
                    'command': 'spark-submit --master local[*] streaming/sentiment_job.py',
                    'description': 'Start sentiment analysis job'
                }
            }
        }

    def start_infrastructure(self):
        """Start Docker infrastructure."""
        logger.info("Starting infrastructure services...")

        try:
            result = subprocess.run(
                ['docker-compose', 'up', '-d'],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                logger.info("Infrastructure started successfully")
                logger.info(result.stdout)
            else:
                logger.error(f"Failed to start infrastructure: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Error starting infrastructure: {e}")
            return False

        return True

    def stop_infrastructure(self):
        """Stop Docker infrastructure."""
        logger.info("Stopping infrastructure services...")

        try:
            result = subprocess.run(
                ['docker-compose', 'down'],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                logger.info("Infrastructure stopped successfully")
            else:
                logger.error(f"Error stopping infrastructure: {result.stderr}")

        except Exception as e:
            logger.error(f"Error stopping infrastructure: {e}")

    def initialize_system(self):
        """Initialize system tables and topics."""
        logger.info("Initializing system...")

        try:
            result = subprocess.run(
                ['python', 'scripts/init_system.py'],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                logger.info("System initialized successfully")
                return True
            else:
                logger.error(f"System initialization failed: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Error initializing system: {e}")
            return False

    def start_service(self, service_name: str) -> bool:
        """Start a specific service."""
        if service_name not in self.config['services']:
            logger.error(f"Unknown service: {service_name}")
            return False

        service_config = self.config['services'][service_name]
        logger.info(f"Starting {service_name}: {service_config['description']}")

        try:
            # Split command into list
            command_parts = service_config['command'].split()

            process = subprocess.Popen(
                command_parts,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            self.processes[service_name] = process
            logger.info(f"Started {service_name} with PID {process.pid}")
            return True

        except Exception as e:
            logger.error(f"Error starting {service_name}: {e}")
            return False

    def stop_service(self, service_name: str):
        """Stop a specific service."""
        if service_name not in self.processes:
            logger.warning(f"Service {service_name} not running")
            return

        process = self.processes[service_name]
        logger.info(f"Stopping {service_name} (PID: {process.pid})")

        try:
            # Send SIGTERM first
            process.terminate()

            # Wait for graceful shutdown
            try:
                process.wait(timeout=10)
                logger.info(f"Service {service_name} stopped gracefully")
            except subprocess.TimeoutExpired:
                # Force kill if not stopped
                process.kill()
                process.wait()
                logger.info(f"Service {service_name} force killed")

            del self.processes[service_name]

        except Exception as e:
            logger.error(f"Error stopping {service_name}: {e}")

    def stop_all_services(self):
        """Stop all running services."""
        logger.info("Stopping all services...")

        for service_name in list(self.processes.keys()):
            self.stop_service(service_name)

    def start_all_services(self, services: List[str] = None):
        """Start all or specified services."""
        if services is None:
            services = list(self.config['services'].keys())

        # Skip infrastructure if already running
        if 'infrastructure' in services:
            if not self.start_infrastructure():
                logger.error("Failed to start infrastructure, aborting")
                return
            services.remove('infrastructure')

            # Wait for services to be ready
            logger.info("Waiting for infrastructure to be ready...")
            time.sleep(30)

        # Initialize system
        if 'initialization' in services:
            if not self.initialize_system():
                logger.error("System initialization failed, aborting")
                return
            services.remove('initialization')

        # Start other services
        for service_name in services:
            if not self.start_service(service_name):
                logger.error(f"Failed to start {service_name}")

    def show_status(self):
        """Show status of all services."""
        print("\n=== System Status ===")

        # Check infrastructure
        try:
            result = subprocess.run(
                ['docker-compose', 'ps'],
                capture_output=True,
                text=True
            )
            print("\nInfrastructure Services:")
            print(result.stdout)
        except:
            print("\nInfrastructure: Unknown")

        print("\nApplication Services:")
        for service_name, process in self.processes.items():
            if process.poll() is None:
                print(f"✓ {service_name}: Running (PID: {process.pid})")
            else:
                print(f"✗ {service_name}: Stopped (exit code: {process.returncode})")

    def monitor_services(self):
        """Monitor running services."""
        logger.info("Monitoring services...")

        try:
            while True:
                time.sleep(30)

                for service_name, process in list(self.processes.items()):
                    if process.poll() is not None:
                        logger.warning(f"Service {service_name} stopped unexpectedly")
                        logger.info(f"Restarting {service_name}...")
                        self.start_service(service_name)

        except KeyboardInterrupt:
            logger.info("Stopping monitor...")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop_all_services()
        sys.exit(0)

    def run_interactive(self):
        """Run interactive mode."""
        print("\n=== Sentiment Analysis System ===")
        print("Available commands:")
        print("  start <service>   - Start a service")
        print("  stop <service>    - Stop a service")
        print("  status            - Show system status")
        print("  monitor           - Monitor services")
        print("  quit              - Exit")
        print("\nAvailable services:", ', '.join(self.config['services'].keys()))

        while True:
            try:
                command = input("\n> ").strip().split()
                if not command:
                    continue

                cmd = command[0].lower()

                if cmd == 'quit':
                    break
                elif cmd == 'start':
                    if len(command) > 1:
                        self.start_service(command[1])
                    else:
                        print("Usage: start <service>")
                elif cmd == 'stop':
                    if len(command) > 1:
                        self.stop_service(command[1])
                    else:
                        print("Usage: stop <service>")
                elif cmd == 'status':
                    self.show_status()
                elif cmd == 'monitor':
                    self.monitor_services()
                else:
                    print(f"Unknown command: {cmd}")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")

        print("\nShutting down...")
        self.stop_all_services()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Sentiment Analysis System Runner')
    parser.add_argument('command', choices=[
        'start', 'stop', 'restart', 'status', 'interactive', 'monitor'
    ], help='Command to execute')
    parser.add_argument('--services', nargs='+', help='Services to manage')
    parser.add_argument('--config', type=str, help='Configuration file path')

    args = parser.parse_args()

    runner = SystemRunner()

    # Setup signal handlers
    signal.signal(signal.SIGINT, runner.signal_handler)
    signal.signal(signal.SIGTERM, runner.signal_handler)

    try:
        if args.command == 'start':
            if args.services:
                runner.start_all_services(args.services)
            else:
                # Start all services
                runner.start_all_services()
                runner.monitor_services()

        elif args.command == 'stop':
            if args.services:
                for service in args.services:
                    runner.stop_service(service)
            else:
                runner.stop_all_services()
                runner.stop_infrastructure()

        elif args.command == 'restart':
            runner.stop_all_services()
            time.sleep(5)
            runner.start_all_services(args.services)

        elif args.command == 'status':
            runner.show_status()

        elif args.command == 'interactive':
            runner.run_interactive()

        elif args.command == 'monitor':
            runner.monitor_services()

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()