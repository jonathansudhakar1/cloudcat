from setuptools import setup, find_packages

setup(
    name="cloudcat",
    version="0.1.3",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "pandas>=1.3.0",
        "tabulate>=0.8.9",
        "colorama>=0.4.4",
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
    author="Your Name",
    author_email="your.email@example.com",
    description="A CLI utility to read and display files from cloud storage",
    long_description="""
    # CloudCat

    A command-line utility to read and display files from Google Cloud Storage and AWS S3 buckets.
    
    ## Features
    
    - Read files from GCS (gcs://) or S3 (s3://)
    - Stream data (avoid downloading entire files when possible)
    - Support for CSV, JSON, and Parquet formats
    - Column selection and row limiting
    - Schema display
    - Record counting
    """,
    long_description_content_type="text/markdown",
    keywords="cloud, gcs, s3, cli, storage, data",
    url="https://github.com/yourusername/cloudcat",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Utilities",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
)