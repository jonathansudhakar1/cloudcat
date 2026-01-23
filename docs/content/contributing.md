## Contributing

Contributions are welcome! Here's how you can help improve CloudCat.

### Ways to Contribute

1. **Report Bugs** - Open an issue with reproduction steps
2. **Suggest Features** - Open an issue describing the use case
3. **Submit PRs** - Fork, create a branch, and submit a pull request
4. **Improve Docs** - Help make the documentation better
5. **Share CloudCat** - Star the repo and spread the word

### Development Setup

Clone and set up the development environment:

```bash
# Clone the repository
git clone https://github.com/jonathansudhakar1/cloudcat.git
cd cloudcat

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in development mode with all dependencies
pip install -e ".[all]"

# Run tests
pytest
```

### Project Structure

```
cloudcat/
├── cloudcat/
│   ├── __init__.py         # Version info
│   ├── cli.py              # Main CLI entry point
│   ├── config.py           # Configuration management
│   ├── compression.py      # Compression handling
│   ├── filtering.py        # WHERE clause parsing
│   ├── formatters.py       # Output formatting
│   ├── readers/            # Format readers
│   │   ├── csv.py
│   │   ├── json.py
│   │   ├── parquet.py
│   │   ├── avro.py
│   │   ├── orc.py
│   │   └── text.py
│   └── storage/            # Cloud storage clients
│       ├── base.py
│       ├── gcs.py
│       ├── s3.py
│       └── azure.py
├── tests/
├── docs/
├── setup.py
└── README.md
```

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Add tests if applicable
5. Run tests: `pytest`
6. Commit your changes: `git commit -m "Add my feature"`
7. Push to your fork: `git push origin feature/my-feature`
8. Open a pull request

### Code Style

- Follow PEP 8 guidelines
- Use meaningful variable names
- Add docstrings to functions
- Keep functions focused and small

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=cloudcat

# Run specific test file
pytest tests/test_cli.py
```

### Adding a New File Format

To add support for a new file format:

1. Create a new reader in `cloudcat/readers/`
2. Export a `read_*_data()` function that returns `(DataFrame, schema)`
3. Update `cli.py` to handle the new format
4. Add format detection in `cli.py`
5. Add tests
6. Update documentation

### Adding a New Cloud Provider

To add support for a new cloud provider:

1. Create a new client in `cloudcat/storage/`
2. Implement `get_stream()` and `list_directory()` functions
3. Update `cloudcat/storage/base.py` to route to the new provider
4. Add authentication handling
5. Add tests
6. Update documentation

### Reporting Issues

When reporting bugs, please include:

- CloudCat version (`pip show cloudcat`)
- Python version (`python --version`)
- Operating system
- Full error message/traceback
- Minimal reproduction steps
- Sample data (if possible and not sensitive)

### Feature Requests

When suggesting features:

- Describe the use case
- Explain how you'd like it to work
- Consider if it fits CloudCat's scope
- Be open to discussion on implementation

### License

By contributing, you agree that your contributions will be licensed under the MIT License.
