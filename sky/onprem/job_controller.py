import argparse
import socket

import ray

import sky

CONTROLLER_PORT = 65432

logger = sky_logging.init_logger(__name__)
ray.init('auto')


class Policy(object):

    def __init__(self):
        self.cluster_resources = ray.cluster_resources()

    def determine_spillover(self) -> bool:
        raise NotImplementedError


class BaselinePolicy(Policy):

    def determine_spillover(self, resources):
        available_resources = ray.available_resources()
        for key in resources.keys():
            if key not in available_resources:
                return True
            if available_resources[key] < resources[key]:
                return True
        return False


class Controller(object):

    def __init__(self):
        return

    def schedule_job(self):
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Connect to localhost
                s.bind(('127.0.0.1', CONTROLLER_PORT))
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        conn.sendall(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
