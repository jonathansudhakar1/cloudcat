"""User configuration file support.

Reads persistent defaults and named profiles from a TOML file so frequently
used flags (credentials, output preferences) don't have to be retyped:

    # ~/.config/cloudcat/config.toml
    num-rows = 20
    output-format = "table"

    [profiles.prod]
    profile = "production"        # AWS profile
    project = "my-gcp-project"

Precedence (highest wins): explicit CLI flag > selected profile section >
top-level defaults > built-in defaults. The file location can be overridden
with the CLOUDCAT_CONFIG environment variable.
"""

import os
import sys
from typing import Optional

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover - exercised only on 3.9/3.10
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

# CLI option names that may be set from the config file (dashes or
# underscores both accepted). Path is deliberately excluded.
CONFIGURABLE_KEYS = {
    'output_format', 'output_file', 'input_format', 'columns', 'num_rows',
    'offset', 'where', 'schema', 'count', 'multi_file_mode', 'max_size_mb',
    'delimiter', 'no_color', 'profile', 'project', 'credentials',
    'az_access_key', 'yes',
}


def default_config_path() -> str:
    """Return the config file path (CLOUDCAT_CONFIG overrides the default)."""
    override = os.environ.get('CLOUDCAT_CONFIG')
    if override:
        return override
    xdg = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
    return os.path.join(xdg, 'cloudcat', 'config.toml')


def _normalize(section: dict) -> dict:
    """Map config keys (dash or underscore) onto CLI parameter names.

    Unknown keys are reported to stderr rather than silently ignored, so
    typos in the config file are visible.
    """
    normalized = {}
    for key, value in section.items():
        param = key.replace('-', '_')
        if param in CONFIGURABLE_KEYS:
            normalized[param] = value
        elif param != 'profiles':
            sys.stderr.write(f"cloudcat: ignoring unknown config key '{key}'\n")
    return normalized


def load_user_config(profile_name: Optional[str] = None) -> dict:
    """Load defaults from the config file, merging a named profile if given.

    Returns a dict mapping CLI parameter names to default values (empty when
    no config file exists).

    Raises:
        ValueError: If a requested profile doesn't exist, or TOML support is
            unavailable / the file is malformed.
    """
    path = default_config_path()
    if not os.path.exists(path):
        if profile_name:
            raise ValueError(
                f"--config-profile '{profile_name}' requested but no config file at {path}"
            )
        return {}

    if tomllib is None:
        raise ValueError(
            "Reading the config file requires Python 3.11+ or the 'tomli' package "
            "(pip install tomli)."
        )

    try:
        with open(path, 'rb') as f:
            data = tomllib.load(f)
    except Exception as e:
        raise ValueError(f"Could not parse config file {path}: {e}")

    profiles = data.get('profiles', {})
    defaults = _normalize(data)

    if profile_name:
        if profile_name not in profiles:
            available = ', '.join(sorted(profiles)) or '(none defined)'
            raise ValueError(
                f"Profile '{profile_name}' not found in {path}. Available: {available}"
            )
        defaults.update(_normalize(profiles[profile_name]))

    return defaults
