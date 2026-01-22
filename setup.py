from setuptools import setup, find_packages
from pathlib import Path

# Read the README file for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="cloudcat",
    version="0.1.4",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "pandas>=1.3.0",
        "tabulate>=0.8.9",
        "colorama>=0.4.4",
    ],
    tests_require=[
        "pytest>=6.0.0",
        "pytest-mock>=3.6.0",
    ],
    extras_require={
        "gcs": ["google-cloud-storage>=2.0.0"],
        "s3": ["boto3>=1.18.0"],
        "parquet": ["pyarrow>=5.0.0"],
        "all": [
            "google-cloud-storage>=2.0.0", 
            "boto3>=1.18.0", 
            "pyarrow>=5.0.0"
        ],
    },
    entry_points={
        "console_scripts": [
            "cloudcat=cloudcat.cli:main",
        ],
    },
    author="Jonathan Sudhakar",
    author_email="jonathan@example.com",
    description="Preview and analyze data files in Google Cloud Storage and AWS S3 from your terminal",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="cloud, gcs, s3, cli, storage, data, parquet, csv, json, google-cloud, aws, data-engineering, etl, spark, bigquery",
    project_urls={
        "Bug Reports": "https://github.com/jonathansudhakar1/cloudcat/issues",
        "Source": "https://github.com/jonathansudhakar1/cloudcat",
        "Documentation": "https://github.com/jonathansudhakar1/cloudcat#readme",
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
    ],
    python_requires=">=3.7",
)