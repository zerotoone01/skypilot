"""Utils for provisioning"""
import collections
import functools
import pathlib
import subprocess
import time
from typing import Dict, Optional

import socket

from sky import sky_logging
from sky.provision import common as provision_comm
from sky.utils import ux_utils

SKY_CLUSTER_PATH = pathlib.Path.home() / '.sky' / 'clusters'
logger = sky_logging.init_logger(__name__)


def _wait_ssh_connection_direct(ip: str) -> bool:
    try:
        with socket.create_connection((ip, 22), timeout=1) as s:
            if s.recv(100).startswith(b'SSH'):
                return True
    except socket.timeout:  # this is the most expected exception
        pass
    except Exception:  # pylint: disable=broad-except
        pass
    return False


def _wait_ssh_connection_indirect(
        ip: str,
        ssh_user: str,
        ssh_private_key: str,
        ssh_control_name: Optional[str] = None,
        ssh_proxy_command: Optional[str] = None) -> bool:
    del ssh_control_name
    # We test ssh with 'echo', because it is of the most common
    # commandline programs on both Unix-like and Windows platforms.
    # NOTE: Ray uses 'uptime' command and 10s timeout.
    command = [
        'ssh', '-T', '-i', ssh_private_key, f'{ssh_user}@{ip}', '-o',
        'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=20s', '-o',
        f'ProxyCommand={ssh_proxy_command}', 'echo'
    ]
    proc = subprocess.run(command,
                          shell=False,
                          check=False,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL)
    return proc.returncode == 0


def wait_for_ssh(cluster_metadata: provision_comm.ClusterMetadata,
                 ssh_credentials: Dict[str, str]):
    """Wait until SSH is ready."""
    ips = cluster_metadata.get_feasible_ips()
    if cluster_metadata.has_public_ips():
        # If we can access public IPs, then it is more efficient to test SSH
        # connection with raw sockets.
        waiter = _wait_ssh_connection_direct
    else:
        # See https://github.com/skypilot-org/skypilot/pull/1512
        waiter = functools.partial(_wait_ssh_connection_indirect,
                                   **ssh_credentials)

    timeout = 60 * 10  # 10-min maximum timeout
    start = time.time()
    # use a queue for SSH querying
    ips = collections.deque(ips)
    while ips:
        ip = ips.popleft()
        if not waiter(ip):
            ips.append(ip)
            if time.time() - start > timeout:
                with ux_utils.print_exception_no_traceback():
                    raise TimeoutError(
                        f'Wait SSH timeout ({timeout}s) exceeded.')
            time.sleep(1)
