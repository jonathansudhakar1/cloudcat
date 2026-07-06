"""Cloud storage client modules.

Re-exports are lazy (PEP 562 module __getattr__): initializing this package
must not import the cloud SDKs, because importing any submodule (e.g. the
lightweight storage.base used by the CLI) initializes the package first —
and shell tab-completion re-imports the CLI on every keypress.
"""

_EXPORTS = {
    # base (light)
    'parse_cloud_path': 'base',
    'get_stream': 'base',
    'list_directory': 'base',
    'get_file_size': 'base',
    # gcs
    'get_gcs_client': 'gcs',
    'get_gcs_stream': 'gcs',
    'list_gcs_directory': 'gcs',
    'HAS_GCS': 'gcs',
    # s3
    'get_s3_client': 's3',
    'get_s3_stream': 's3',
    'list_s3_directory': 's3',
    'HAS_S3': 's3',
    # azure
    'get_azure_datalake_service_client': 'azure',
    'get_azure_blob_service_client': 'azure',  # legacy alias
    'get_azure_stream': 'azure',
    'list_azure_directory': 'azure',
    'HAS_AZURE': 'azure',
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    module = importlib.import_module(f'.{module_name}', __name__)
    value = getattr(module, name)
    globals()[name] = value  # cache for subsequent lookups
    return value
