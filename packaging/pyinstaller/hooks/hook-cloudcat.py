"""PyInstaller hook for cloudcat package.

This hook ensures all cloudcat submodules are properly collected,
including the storage backends and data format readers.
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

# Collect all cloudcat submodules
hiddenimports = collect_submodules('cloudcat')

# Explicitly add storage backends (dynamically imported based on URL scheme)
hiddenimports += [
    'cloudcat.storage.gcs',
    'cloudcat.storage.s3',
    'cloudcat.storage.azure',
    'cloudcat.storage.base',
]

# Explicitly add data format readers (dynamically imported based on file extension)
hiddenimports += [
    'cloudcat.readers.csv',
    'cloudcat.readers.json',
    'cloudcat.readers.parquet',
    'cloudcat.readers.avro',
    'cloudcat.readers.orc',
    'cloudcat.readers.text',
]

# Collect any data files (none currently, but future-proofing)
datas = collect_data_files('cloudcat')
