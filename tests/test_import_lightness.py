"""Guard: importing cloudcat.cli must stay light.

Shell tab-completion re-imports the CLI on every keypress, so heavy
dependencies (pandas, pyarrow, cloud SDKs, the readers) must only load
inside the functions that use them. If this test fails, a module-level
import crept back in — make it lazy instead.
"""

import subprocess
import sys

HEAVY_MODULES = (
    "pandas",
    "pyarrow",
    "numpy",
    "boto3",
    "google.cloud.storage",
    "azure.storage.filedatalake",
    "azure.storage.blob",
    "fastavro",
    "tabulate",
    "deltalake",
    "pyiceberg",
)

CHECK = (
    "import sys; import cloudcat.cli; "
    "loaded = [m for m in {mods!r} if m in sys.modules]; "
    "print(','.join(loaded))"
).format(mods=HEAVY_MODULES)


def test_cli_import_pulls_no_heavy_modules():
    result = subprocess.run(
        [sys.executable, "-c", CHECK], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    loaded = [m for m in result.stdout.strip().split(",") if m]
    assert loaded == [], (
        f"cloudcat.cli import now loads heavy modules: {loaded}. "
        "Move the import inside the function that needs it."
    )


def test_completion_protocol_pulls_no_heavy_modules():
    """The completion callback path must stay light end to end."""
    import os
    env = dict(os.environ,
               _CLOUDCAT_COMPLETE="zsh_complete",
               COMP_WORDS="cloudcat ''", COMP_CWORD="1")
    probe = (
        "import sys, cloudcat.cli as c; "
        "import click; "
        "from click.shell_completion import ShellComplete; "
        "sc = ShellComplete(c.main, dict(), 'cloudcat', '_CLOUDCAT_COMPLETE'); "
        "sc.get_completions([], ''); "
        f"loaded = [m for m in {HEAVY_MODULES!r} if m in sys.modules]; "
        "print(','.join(loaded))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    loaded = [m for m in result.stdout.strip().split(",") if m]
    assert loaded == [], f"completion path loads heavy modules: {loaded}"
