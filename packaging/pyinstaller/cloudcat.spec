# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec file for cloudcat standalone build.

This spec file bundles cloudcat with all dependencies including:
- Python interpreter
- pandas, pyarrow (for data handling)
- google-cloud-storage, boto3, azure-storage-blob (cloud providers)
- fastavro, zstandard, lz4, snappy (optional formats/compression)
"""

import sys
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_data_files

block_cipher = None

# Collect all submodules and data for complex packages
hiddenimports = []
datas = []
binaries = []

# Packages that need full collection (have native extensions or complex imports)
packages_to_collect = [
    'pandas',
    'numpy',
    'pyarrow',
    'google.cloud.storage',
    'google.cloud',
    'google.auth',
    'google.oauth2',
    'google.api_core',
    'google.protobuf',
    'googleapis_common_protos',
    'grpc',
    'boto3',
    'botocore',
    's3transfer',
    'azure.storage.blob',
    'azure.identity',
    'azure.core',
    'click',
    'tabulate',
    'colorama',
    'fastavro',
    'zstandard',
    'lz4',
    'snappy',
]

for pkg in packages_to_collect:
    try:
        pkg_datas, pkg_binaries, pkg_hiddenimports = collect_all(pkg)
        datas += pkg_datas
        binaries += pkg_binaries
        hiddenimports += pkg_hiddenimports
    except Exception as e:
        print(f"Warning: Could not collect {pkg}: {e}")

# Explicitly include cloudcat modules
hiddenimports += collect_submodules('cloudcat')

# Additional hidden imports for dynamic imports in dependencies
hiddenimports += [
    # Google Cloud
    'grpc._cython',
    'grpc._cython.cygrpc',
    'google.auth.transport.requests',
    'google.auth.transport.grpc',
    'google.auth.credentials',
    'google.auth.compute_engine',
    'google.auth.compute_engine.credentials',
    'google.auth.iam',
    'google.auth.impersonated_credentials',
    'google.oauth2.credentials',
    'google.oauth2.service_account',
    'google.resumable_media',
    'google.resumable_media.requests',

    # AWS
    'botocore.regions',
    'botocore.httpsession',
    'botocore.parsers',
    'botocore.retryhandler',
    'botocore.translate',
    'botocore.utils',

    # Azure
    'azure.core.pipeline',
    'azure.core.pipeline.transport',
    'azure.core.pipeline.policies',
    'azure.identity._credentials',
    'azure.identity._internal',

    # Data formats
    'pyarrow.parquet',
    'pyarrow.feather',
    'pyarrow.orc',
    'pyarrow.csv',
    'pyarrow.json',
    'pyarrow.compute',

    # Pandas internals
    'pandas._libs',
    'pandas._libs.tslibs',
    'pandas.io.formats.style',

    # Compression
    'zstandard',
    'lz4.frame',
    'lz4.block',

    # Standard library that may be dynamically imported
    'encodings',
    'encodings.utf_8',
    'encodings.ascii',
    'encodings.latin_1',
]

# Cloudcat-specific modules
hiddenimports += [
    'cloudcat',
    'cloudcat.cli',
    'cloudcat.config',
    'cloudcat.compression',
    'cloudcat.filtering',
    'cloudcat.formatters',
    'cloudcat.storage',
    'cloudcat.storage.base',
    'cloudcat.storage.gcs',
    'cloudcat.storage.s3',
    'cloudcat.storage.azure',
    'cloudcat.readers',
    'cloudcat.readers.csv',
    'cloudcat.readers.json',
    'cloudcat.readers.parquet',
    'cloudcat.readers.avro',
    'cloudcat.readers.orc',
    'cloudcat.readers.text',
]

a = Analysis(
    ['../../cloudcat/cli.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=['hooks'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unused heavy packages to reduce size
        'tkinter',
        'matplotlib',
        'PIL',
        'scipy',
        'IPython',
        'jupyter',
        'notebook',
        'sphinx',
        'pytest',
        'setuptools',
        'pip',
        'wheel',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='cloudcat',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # UPX can cause issues on macOS
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,  # Use native architecture
    codesign_identity=None,
    entitlements_file=None,
)
