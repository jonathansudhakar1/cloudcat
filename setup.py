from setuptools import setup, find_packages
from pathlib import Path

# Read the README file for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Read version from package __init__.py
def get_version():
    version_file = this_directory / "cloudcat" / "__init__.py"
    for line in version_file.read_text().splitlines():
        if line.startswith("__version__"):
            # Handle both single and double quotes
            if '"' in line:
                return line.split('"')[1]
            elif "'" in line:
                return line.split("'")[1]
    return "0.0.0"

setup(
    name="cloudcat",
    version=get_version(),
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "pandas>=1.3.0",
        "tabulate>=0.8.9",
        "colorama>=0.4.4",
        # Cloud providers (included by default)
        "google-cloud-storage>=2.0.0",
        "boto3>=1.18.0",
        "azure-storage-file-datalake>=12.0.0",
        "azure-identity>=1.0.0",
    ],
    tests_require=[
        "pytest>=6.0.0",
        "pytest-mock>=3.6.0",
    ],
    extras_require={
        "parquet": ["pyarrow>=5.0.0"],
        "avro": ["fastavro>=1.4.0"],
        "orc": ["pyarrow>=5.0.0"],
        "zstd": ["zstandard>=0.15.0"],
        "lz4": ["lz4>=3.0.0"],
        "snappy": ["python-snappy>=0.6.0"],
        "compression": ["zstandard>=0.15.0", "lz4>=3.0.0", "python-snappy>=0.6.0"],
        "all": [
            "pyarrow>=5.0.0",
            "fastavro>=1.4.0",
            "zstandard>=0.15.0",
            "lz4>=3.0.0",
            "python-snappy>=0.6.0"
        ],
    },
    entry_points={
        "console_scripts": [
            "cloudcat=cloudcat.cli:main",
        ],
    },
    author="Jonathan Sudhakar",
    author_email="jonathan@example.com",
    description="Preview and analyze data files in Google Cloud Storage, AWS S3, and Azure Data Lake Storage Gen2 from your terminal",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="cloud, gcs, s3, azure, cli, storage, data, parquet, csv, json, avro, orc, google-cloud, aws, adls, datalake, data-engineering, etl, spark, bigquery, databricks, kafka, hive",
    project_urls={
        "Homepage": "https://github.com/jonathansudhakar1/cloudcat",
        "Documentation": "https://cloudcatcli.com",
        "Bug Reports": "https://github.com/jonathansudhakar1/cloudcat/issues",
        "Source": "https://github.com/jonathansudhakar1/cloudcat",
    },
    url="https://github.com/jonathansudhakar1/cloudcat",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: System :: Systems Administration",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.7",
)