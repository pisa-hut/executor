"""
Apptainer service management for simulation components.

This module provides utilities to start and stop Apptainer instances
for different simulation components (simulators, AVs, etc.).
"""

import json
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional


def find_free_port(start_port: int = 8000, max_attempts: int = 100) -> Optional[int]:
    """
    Find a free port on the system.

    Args:
        start_port: Port number to start searching from
        max_attempts: Maximum number of ports to try

    Returns:
        A free port number, or None if no free port found
    """
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", port))
                return port
        except OSError:
            continue
    return None


class ApptainerServiceConfig:
    """Configuration for an Apptainer service."""

    def __init__(
        self,
        sif_path: str,
        startup_wait: float = 2.0,
        preferred_port: Optional[int] = None,
        extra_ports: Optional[dict[str, int]] = None,
        extra_args: Optional[list[str]] = None,
    ):
        """
        Initialize service configuration.

        Args:
            sif_path: Path to the .sif container image
            startup_wait: Seconds to wait after starting the service
            preferred_port: Preferred port to start searching from (actual port will be dynamically allocated)
            extra_ports: Dict mapping env var names to preferred port starts (e.g., {"CARLA_PORT": 2000})
            extra_args: Additional arguments to pass to apptainer run
        """
        self.sif_path = sif_path
        self.startup_wait = startup_wait
        self.preferred_port = preferred_port or 8000
        self.extra_ports = extra_ports or {}
        self.extra_args = extra_args or []

    def get_start_command(self, ports: dict[str, int]) -> list[str]:
        """
        Get the command to start this service.

        Args:
            ports: Dict mapping env var names to allocated ports (includes "PORT" and any extra ports)

        Returns:
            Command list for starting the service
        """
        cmd = ["apptainer", "run"]

        cmd.extend(["--containall"])

        # Pass all allocated ports via environment variables
        for env_var, port in ports.items():
            cmd.extend(["--env", f"{env_var}={port}"])

        # Add any extra arguments
        cmd.extend(self.extra_args)

        # Add the SIF file
        cmd.append(self.sif_path)

        return cmd


class ApptainerServiceManager:
    """Manager for starting and stopping Apptainer services."""

    # Predefined configurations for different simulators
    SIMULATOR_CONFIGS = {
        "esmini": ApptainerServiceConfig(
            sif_path="./worker.sif",
            preferred_port=8080,
        ),
        "carla": ApptainerServiceConfig(
            sif_path="./carla-wrapper.sif",
            preferred_port=3000,
            extra_ports={"CARLA_PORT": 2000},
        ),
        # Add more simulator configurations as needed
        # "autoware": ApptainerServiceConfig(
        #     sif_path="./autoware.sif",
        #     preferred_port=8080,
        # ),
    }

    # Predefined configurations for different AVs
    AV_CONFIGS = {
        # Example AV configurations - uncomment and modify as needed
        # "autoware": ApptainerServiceConfig(
        #     sif_path="./autoware-av.sif",
        #     preferred_port=9090,
        # ),
        # "apollo": ApptainerServiceConfig(
        #     sif_path="./apollo.sif",
        #     preferred_port=8888,
        # ),
    }

    # Runner configuration
    RUNNER_CONFIG = ApptainerServiceConfig(
        sif_path="./runner.sif",
        preferred_port=None,  # Runner doesn't need a port
    )

    def __init__(self):
        """Initialize the service manager."""
        self.active_processes: dict[str, tuple[subprocess.Popen, dict[str, int]]] = (
            {}
        )  # Maps service_name -> (process, ports_dict)
        self.component_to_instance: dict[str, str] = (
            {}
        )  # Maps component_type:component_name -> service_name

    def start_simulator_service(self, simulator_name: str) -> Optional[dict]:
        """
        Start an Apptainer service for a simulator.

        Args:
            simulator_name: Name of the simulator

        Returns:
            Dict with 'url' and 'port' if service started successfully, None otherwise
        """
        return self._start_service(
            component_type="simulator",
            component_name=simulator_name,
            configs=self.SIMULATOR_CONFIGS,
        )

    def start_av_service(self, av_name: str) -> Optional[dict]:
        """
        Start an Apptainer service for an AV.

        Args:
            av_name: Name of the AV

        Returns:
            Dict with 'url' and 'port' if service started successfully, None otherwise
        """
        return self._start_service(
            component_type="av", component_name=av_name, configs=self.AV_CONFIGS
        )

    def _start_service(
        self,
        component_type: str,
        component_name: str,
        configs: dict[str, ApptainerServiceConfig],
    ) -> Optional[dict]:
        """
        Internal method to start an Apptainer service.

        Args:
            component_type: Type of component (e.g., "simulator", "av")
            component_name: Name of the specific component
            configs: Dictionary of available configurations

        Returns:
            Dict with 'url', 'port', and 'service_name' if service started successfully, None otherwise
        """
        print(f"Starting Apptainer service for {component_type}: {component_name}")

        config = configs.get(component_name.lower())
        if config is None:
            print(
                f"No Apptainer service configuration found for {component_type}: {component_name}"
            )
            return None

        # Dynamically allocate the main port
        allocated_port = find_free_port(start_port=config.preferred_port)
        if allocated_port is None:
            print(f"Failed to find a free port for {component_type}: {component_name}")
            return None

        print(f"Allocated main port: {allocated_port}")

        # Allocate extra ports if needed
        allocated_ports = {"PORT": allocated_port}
        for env_var, preferred_start in config.extra_ports.items():
            extra_port = find_free_port(start_port=preferred_start)
            if extra_port is None:
                print(f"Failed to find a free port for {env_var}")
                return None
            allocated_ports[env_var] = extra_port
            print(f"Allocated {env_var}: {extra_port}")

        service_name = f"{component_name}-{allocated_port}"

        try:
            command = config.get_start_command(allocated_ports)
            print(f"Running command: {' '.join(command)}")

            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )

            # Wait for service to start
            time.sleep(config.startup_wait)

            # Check if service started successfully
            if proc.poll() is not None:
                _, stderr = proc.communicate()
                print(f"Warning: Apptainer service may have failed to start: {stderr}")
                return None
            else:
                service_url = f"http://localhost:{allocated_port}"
                print(f"Apptainer service '{service_name}' started successfully")
                print(f"Service URL: {service_url}")
                print(f"Service Port: {allocated_port}")
                for env_var, port in allocated_ports.items():
                    if env_var != "PORT":
                        print(f"  {env_var}: {port}")

                self.active_processes[service_name] = (proc, allocated_ports)
                self.component_to_instance[f"{component_type}:{component_name}"] = (
                    service_name
                )

                return {
                    "url": service_url,
                    "port": allocated_port,
                    "ports": allocated_ports,
                    "service_name": service_name,
                }
        except Exception as e:
            print(f"Failed to start Apptainer service: {e}")
            return None

    def stop_simulator_service(self, simulator_name: str):
        """
        Stop the Apptainer service for a simulator.

        Args:
            simulator_name: Name of the simulator
        """
        self._stop_service(
            component_type="simulator",
            component_name=simulator_name,
            configs=self.SIMULATOR_CONFIGS,
        )

    def stop_av_service(self, av_name: str):
        """
        Stop the Apptainer service for an AV.

        Args:
            av_name: Name of the AV
        """
        self._stop_service(
            component_type="av", component_name=av_name, configs=self.AV_CONFIGS
        )

    def _stop_service(
        self,
        component_type: str,
        component_name: str,
        configs: dict[str, ApptainerServiceConfig],
    ):
        """
        Internal method to stop an Apptainer service.

        Args:
            component_type: Type of component (e.g., "simulator", "av")
            component_name: Name of the specific component
            configs: Dictionary of available configurations
        """
        config = configs.get(component_name.lower())
        if config is None:
            return

        # Find the service name for this component
        component_key = f"{component_type}:{component_name}"
        service_name = self.component_to_instance.get(component_key)

        if service_name is None:
            print(f"No active service found for {component_type}: {component_name}")
            return

        try:
            print(f"Stopping Apptainer process: {service_name}")
            proc_info = self.active_processes.get(service_name)
            if proc_info:
                proc, ports = proc_info
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                main_port = ports.get("PORT", "unknown")
                print(f"Apptainer process '{service_name}' (port {main_port}) stopped")

            # Remove from active processes
            self.active_processes.pop(service_name, None)
            self.component_to_instance.pop(component_key, None)
        except Exception as e:
            print(f"Failed to stop Apptainer service: {e}")

    def stop_all_services(self):
        """Stop all active Apptainer services."""
        for service_name in list(self.active_processes.keys()):
            try:
                print(f"Stopping Apptainer process: {service_name}")
                proc, ports = self.active_processes[service_name]
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                main_port = ports.get("PORT", "unknown")
                print(f"Apptainer process '{service_name}' (port {main_port}) stopped")
            except Exception as e:
                print(f"Failed to stop Apptainer process {service_name}: {e}")

        self.active_processes.clear()
        self.component_to_instance.clear()

    def run_runner(self, spec: dict, task_id: int = 0, worker_id: int = 0) -> int:
        """
        Run the scenario runner in an Apptainer container.
        This is a blocking operation that waits for the runner to complete.

        Args:
            spec: The full specification dictionary to pass to the runner
            task_id: Task ID for unique spec filename (optional)
            worker_id: Worker ID for unique spec filename (optional)

        Returns:
            Exit code of the runner process (0 for success)
        """
        print("Starting runner in Apptainer container...")

        # Create a unique spec file for this task/worker to avoid conflicts in shared directories
        if task_id != 0:
            spec_filename = f".runner_spec_task_{task_id}.json"
        elif worker_id != 0:
            spec_filename = f".runner_spec_worker_{worker_id}.json"
        else:
            # Fallback: use job_id from spec if available
            job_id = spec.get("task", {}).get("job_id", "unknown")
            spec_filename = f".runner_spec_job_{job_id}.json"

        spec_file = Path(spec_filename)
        try:
            with open(spec_file, "w") as f:
                json.dump(spec, f, indent=2)
            print(f"Wrote spec to {spec_file}")

            # Build the apptainer run command
            cmd = ["apptainer", "run", "--containall"]

            # Bind mount the spec file and output directory
            cmd.extend(["--bind", f"{spec_file.absolute()}:/spec.json:ro"])

            # Bind mount the output directory
            output_dir = spec.get("task", {}).get("output_dir", "./output_dir")
            output_path = Path(output_dir).absolute()
            output_path.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--bind", f"{output_path}:/output"])

            # Add the SIF file
            cmd.append(self.RUNNER_CONFIG.sif_path)

            print(f"Running command: {' '.join(cmd)}")

            # Run the container and wait for it to complete
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Stream output in real-time
            print("--- Runner Output ---")
            while True:
                if proc.stdout:
                    line = proc.stdout.readline()
                    if line:
                        print(line.rstrip())

                # Check if process has finished
                if proc.poll() is not None:
                    break

            # Get any remaining output
            if proc.stdout:
                remaining = proc.stdout.read()
                if remaining:
                    print(remaining.rstrip())

            exit_code = proc.returncode
            print("--- End Runner Output ---")
            print(f"Runner exited with code: {exit_code}")

            return exit_code
        except Exception as e:
            print(f"Failed to run runner in Apptainer container: {e}")
            return -1

        finally:
            # Clean up the spec file
            if spec_file.exists():
                spec_file.unlink()
                print(f"Cleaned up {spec_file}")
