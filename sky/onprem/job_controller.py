import ast
import argparse
import socket
import threading

import ray

import sky
from sky import sky_logging

ADMIN_CONTROLLER_IP = socket.gethostname()
ADMIN_CONTROLLER_PORT = 65432

logger = sky_logging.init_logger(__name__)
ray.init('auto')


def _validate_resources(resources: str) -> dict:
    if not resources:
        return False
    try:
        ast.literal_eval(resources)
        return True
    except ValueError:
        logger.error(
            'Passed an invalid resources dict to job controller. Try again.')
        return False


class Policy(object):

    def __init__(self):
        self.cluster_resources = ray.cluster_resources()

    def determine_spillover(self) -> bool:
        raise NotImplementedError


class BaselinePolicy(Policy):

    def determine_spillover(self, resources):
        available_resources = ray.available_resources()
        for key in resources.keys():
            if key not in available_resources or available_resources[
                    key] < resources[key]:
                return True
        return False


# Runs on the admin account of the head node of the on-premise cluster
# and assigns whether user submitted jobs stay on the on-premise cluster
# or go to cloud.
class Controller(object):

    def __init__(self, policy='base'):
        assert policy in ['base']
        if policy == 'base':
            self.policy = BaselinePolicy()
        else:
            self.policy = None

    def schedule_job(self, resources: str) -> bool:
        return self.policy.determine_spillover(resources)

    def _process_job(self, conn, addr):
        conn.settimeout(5.0)
        while True:
            recv_str = conn.recv(1024).decode("utf-8")
            # If empty string is encountered, do not process string.
            if not recv_str:
                break
            if not _validate_resources(recv_str):
                # '-1' indicates error encountered processing resources.
                conn.sendall(b'-1')
                continue
            resources = ast.literal_eval(recv_str)
            if self.schedule_job(resources):
                # '1' indicates job should be launched on the cloud.
                conn.sendall(b'1')
            else:
                # '0' indicates job should be launched on the on-prem cluster.
                conn.sendall(b'0')
        conn.close()

    def poll_for_jobs(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Connect to localhost
        s.bind((ADMIN_CONTROLLER_IP, ADMIN_CONTROLLER_PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(
                target=self._process_job,
                args=(conn, addr),
            ).start()
        s.close()
        return


if __name__ == '__main__':
    admin_job_controller = Controller(policy='base')
    admin_job_controller.poll_for_jobs()
