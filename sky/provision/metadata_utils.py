"""Utils for managing metadata for provisioning."""
from typing import Optional

import contextlib
import functools
import json
import os
import pathlib
import shutil

from sky import sky_logging
from sky.provision import common

SKY_CLUSTER_PATH = pathlib.Path.home() / '.sky' / 'clusters'
SKY_REMOTE_REFLECTION_METADATA_PATH = '~/.sky/reflection.json'
logger = sky_logging.init_logger(__name__)


def cache_func(cluster_name: str, instance_id: str, stage_name: str,
               hash_str: str):
    """A helper function for caching function execution."""

    def decorator(function):

        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            with check_cache_hash_or_update(cluster_name, instance_id,
                                            stage_name,
                                            hash_str) as need_update:
                if need_update:
                    return function(*args, **kwargs)
                return None

        return wrapper

    return decorator


@contextlib.contextmanager
def check_cache_hash_or_update(cluster_name: str, instance_id: str,
                               stage_name: str, hash_str: str):
    """A decorator for 'cache_func'."""
    path = get_cache_dir(cluster_name, instance_id) / stage_name
    if path.exists():
        with open(path) as f:
            need_update = f.read() != hash_str
    else:
        need_update = True
    logger.debug(f'Need to update {cluster_name}/{instance_id}/{stage_name}: '
                 f'{str(need_update)}')
    errored = False
    try:
        yield need_update
    except Exception as e:
        errored = True
        raise e
    finally:
        if not errored and (not path.exists() or need_update):
            with open(path, 'w') as f:
                f.write(hash_str)


def get_cache_dir(cluster_name: str, instance_id: str) -> pathlib.Path:
    """This function returns a pathlib.Path object representing the cache
    directory for the specified cluster and instance. If the directory
    does not exist, it is created."""
    dirname = (SKY_CLUSTER_PATH / cluster_name / 'instances' / instance_id /
               'cache')
    dirname.mkdir(parents=True, exist_ok=True)
    return dirname.resolve()


def get_log_dir(cluster_name: str, instance_id: str) -> pathlib.Path:
    """This function returns a pathlib.Path object representing the
    log directory for the specified cluster and instance. If the
    directory does not exist, it is created."""
    dirname = (SKY_CLUSTER_PATH / cluster_name / 'instances' / instance_id /
               'logs')
    dirname.mkdir(exist_ok=True, parents=True)
    return dirname.resolve()


def generate_reflection_metadata(
        provision_metadata: common.ProvisionMetadata) -> pathlib.Path:
    """This function generates metadata for instances to 'reflect' its own
    configuration, including its cloud, region, head instance id etc.
    The metadata is a dictionary containing these information and then
    mount to instances."""
    name = provision_metadata.cluster_name
    metadata = provision_metadata.json(indent=2)
    (SKY_CLUSTER_PATH / name).mkdir(exist_ok=True, parents=True)
    path = SKY_CLUSTER_PATH / name / 'reflection.json'
    with open(path, 'w') as f:
        json.dump(metadata, f)
    return path


def get_reflection_metadata() -> Optional[common.ProvisionMetadata]:
    """This function attempts to open the metadata file located at
    ~/.sky/metadata.json and returns its contents as a dictionary.
    If the metadata file does not exist, it returns None."""
    try:
        with open(os.path.expanduser(SKY_REMOTE_REFLECTION_METADATA_PATH)) as f:
            content = json.load(f)
        return common.ProvisionMetadata.parse_obj(content)
    except FileNotFoundError:
        return None


def remove_cluster_profile(cluster_name: str) -> None:
    """Remove profiles of a cluster. This is called when terminating
    the cluster."""
    dirname = SKY_CLUSTER_PATH / cluster_name
    logger.debug(f'Remove profile of cluster {cluster_name}.')
    shutil.rmtree(dirname, ignore_errors=True)
