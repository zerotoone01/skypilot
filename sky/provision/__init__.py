"""This module provides a standard low-level interface that all
providers supported by Skypilot need to follow."""

import functools
import importlib
import inspect

from sky.provision import common


def _router(func):

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        # check the signature to fail early
        inspect.signature(func).bind(*args, **kwargs)
        if args:
            provider_name = args[0]
            args = args[1:]
        else:
            provider_name = kwargs.pop('provider_name')

        module_name = provider_name
        module = importlib.import_module(f'sky.provision.{module_name.lower()}')

        impl = getattr(module, func.__name__)
        return impl(*args, **kwargs)

    return _wrapper


# pylint: disable=unused-argument


@_router
def bootstrap(provider_name: str, region: str, cluster_name: str,
              config: common.InstanceConfig) -> common.InstanceConfig:
    """This function sets up ancillary resources for an instance
    in the specified cluster with the provided configuration,
    and returns an InstanceConfig object with updated configuration.

    These ancillary resources could include security policies, network
    configurations etc. These resources tend to be free or very cheap,
    but it takes time to set them up from scratch. So we generally
    caching or reusing them when possible.
    """
    raise NotImplementedError


@_router
def start_instances(provider_name: str, region: str, cluster_name: str,
                    config: common.InstanceConfig) -> common.ProvisionMetadata:
    """Start instances with bootstrapped configuration."""
    raise NotImplementedError


@_router
def stop_instances(provider_name: str, region: str, cluster_name: str) -> None:
    """Stop running instances."""
    raise NotImplementedError


@_router
def terminate_instances(provider_name: str, region: str,
                        cluster_name: str) -> None:
    """Terminate running or stopped instances."""
    raise NotImplementedError


@_router
def stop_instances_with_self(provider_name: str) -> None:
    """A helper function to stop instances within the targeted instance."""
    raise NotImplementedError


@_router
def terminate_instances_with_self(provider_name: str) -> None:
    """A helper function to terminate instances within the
    targeted instance."""
    raise NotImplementedError


@_router
def wait_instances(provider_name: str, region: str, cluster_name: str,
                   state: str) -> None:
    """Wait instances until they ends up in the given state."""
    raise NotImplementedError


@_router
def get_cluster_metadata(provider_name: str, region: str,
                         cluster_name: str) -> common.ClusterMetadata:
    """Get the metadata of instances in a cluster."""
    raise NotImplementedError
