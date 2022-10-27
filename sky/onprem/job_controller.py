import ast
import argparse
import shlex
import socket
import threading
import uuid

import ray

import sky
from sky import sky_logging
from sky.backends import onprem_utils
from sky.skylet import job_lib

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


def _run_on_user(user, cmd):
    remote_run_file = f'/tmp/local-{uuid.uuid4().hex[:10]}'
    with open(remote_run_file, 'w+') as fp:
        fp.write(cmd)
        fp.flush()
    os.system(
        f'chmod a+rwx {remote_run_file}; sudo -H su --login {user} -c {remote_run_file}'
    )
    os.system(f'rm -rf {remote_run_file}')


class MixedJob(object):

    def __init__(self, user: str, job_id: int):
        # 'local', 'cloud'
        self.state = None

        self.user = user
        self.job_id = job_id

    def _launch_local(self):
        code = [
            'import os',
            'from sky.backends import onprem_utils',
            f'onprem_utils.run_local_job({self.job_id!r},{self.user!r})',
        ]
        code = ';'.join(code)
        python_cmd = f'python3 -u -c {shlex.quote(code)}'
        _run_on_user(self.user, python_cmd)
        self.state = 'local'

    def _kill_local(self):
        kill_cmd = f'sky cancel local {self.job_id}'
        _run_on_user(self.user, kill_cmd)

    def _launch_cloud(self):
        code = [
            'import os',
            'from sky.backends import onprem_utils',
            f'onprem_utils.run_cloud_job({self.job_id!r},{self.user!r})',
        ]
        code = ';'.join(code)
        python_cmd = f'python3 -u -c {shlex.quote(code)}'
        _run_on_user(self.user, python_cmd)
        self.state = 'cloud'

    def _kill_cloud(self):
        kill_cmd = f'sky cancel local {self.job_id}; sky down -y -p {self.user}-{self.job_id}'
        _run_on_user(self.user, kill_cmd)

    def migrate_cloud(self):
        assert self.state != 'cloud'
        if self.state:
            self._kill_local()
        self._launch_cloud()
        self.state = 'local'

    def migrate_local(self):
        assert self.state != 'local'
        if self.state:
            self._kill_cloud()
        self._launch_local()
        self.state = 'cloud'


# Runs on the admin account of the head node of the on-premise cluster
# and assigns whether user submitted jobs stay on the on-premise cluster
# or go to cloud.
class JobController(object):

    def __init__(self, policy='base'):
        assert policy in ['base']
        if policy == 'base':
            self.policy = BaselinePolicy()
        else:
            self.policy = None

        # Nested dict
        self.jobs = {}

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
    admin_job_controller = JobController(policy='base')
    # Assumes there are NO active jobs on the cluster at the current moment.
    admin_job_controller.poll_for_jobs()
