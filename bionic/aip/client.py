import logging

from bionic.deps.optdep import import_optional_dependency

logger = logging.getLogger(__name__)

_cached_aip_client = None


def get_aip_client(cache_value=True):
    if cache_value:
        global _cached_aip_client
        if _cached_aip_client is None:
            _cached_aip_client = get_aip_client(cache_value=False)
        return _cached_aip_client

    discovery = import_optional_dependency(
        "googleapiclient.discovery", raise_on_missing=True
    )
    logger.info("Initializing AIP client ...")
    return discovery.build("ml", "v1", cache_discovery=False)
