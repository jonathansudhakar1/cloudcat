"""Shell tab-completion for PATH: local files, buckets, and cloud prefixes.

Wired into the PATH argument via Click's shell_complete. The generated
completion script re-invokes cloudcat on every TAB, so this module must be
cheap to import (SDKs load lazily, guarded by tests/test_import_lightness)
and must never hang or error the user's shell: cloud calls run with short
timeouts, no retries, capped result counts — and any failure completes to
nothing rather than raising.

Behavior mirrors `ls` completion, scheme-aware:
    <TAB>                  -> native file completion
    s3://<TAB>             -> buckets (via --profile if typed earlier)
    s3://bucket/dir/<TAB>  -> immediate children under the prefix
"""

from typing import List, Optional, Tuple

import os

from click.shell_completion import CompletionItem, ZshComplete, add_completion_class


def _debug(message: str) -> None:
    """Optionally record why a completion produced nothing.

    Completion swallows every error so it can never break the shell — which
    makes "TAB does nothing" undiagnosable. Set CLOUDCAT_COMPLETE_DEBUG=1 and
    failures are appended to ~/.cache/cloudcat/completion.log instead.
    """
    if not os.environ.get('CLOUDCAT_COMPLETE_DEBUG'):
        return
    try:
        import datetime
        cache_dir = os.path.join(
            os.environ.get('XDG_CACHE_HOME', os.path.expanduser('~/.cache')), 'cloudcat')
        os.makedirs(cache_dir, exist_ok=True)
        with open(os.path.join(cache_dir, 'completion.log'), 'a', encoding='utf-8') as f:
            f.write(f"{datetime.datetime.now().isoformat()} {message}\n")
    except Exception:
        pass


class CloudcatZshComplete(ZshComplete):
    """Click's zsh completer, hardened for real-world .zshrc files.

    The stock script calls compdef unconditionally; if the user evals it
    before compinit has run, registration fails silently and TAB "does
    nothing". This variant bootstraps compinit when needed.
    """

    name = 'zsh'
    source_template = ZshComplete.source_template.replace(
        "    compdef %(complete_func)s %(prog_name)s",
        """    if ! typeset -f compdef >/dev/null 2>&1; then
        autoload -Uz compinit && compinit -u
    fi
    compdef %(complete_func)s %(prog_name)s""",
    )


# Replace the registered zsh completer with the hardened variant.
add_completion_class(CloudcatZshComplete)

# Never return more candidates than a shell menu can usefully show.
LIMIT = 100
# Cloud calls must never hang a keypress.
CONNECT_TIMEOUT = 1.0
READ_TIMEOUT = 2.0

_SCHEMES = ('s3://', 'r2://', 'gs://', 'gcs://', 'abfss://', 'file://')


# --------------------------------------------------------------------------
# per-provider listing (small, patchable seams; SDK imports stay inside)
# --------------------------------------------------------------------------

def _s3_client(profile: Optional[str], endpoint: Optional[str] = None):
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=CONNECT_TIMEOUT, read_timeout=READ_TIMEOUT,
                    retries={'max_attempts': 0})
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    kwargs = {'config': config}
    if endpoint:
        kwargs['endpoint_url'] = endpoint
    # Without a configured region some boto3 versions raise NoRegionError at
    # client creation; bucket listing is region-agnostic, so default it.
    kwargs['region_name'] = session.region_name or ('auto' if endpoint else 'us-east-1')
    return session.client('s3', **kwargs)


def _list_s3_buckets(profile: Optional[str], endpoint: Optional[str] = None) -> List[str]:
    response = _s3_client(profile, endpoint).list_buckets()
    return [b['Name'] for b in response.get('Buckets', [])]


def _shallow_list_s3(bucket: str, prefix: str, profile: Optional[str],
                     endpoint: Optional[str] = None) -> Tuple[List[str], List[str]]:
    """Immediate children under a prefix: (dirs, files) as full keys."""
    response = _s3_client(profile, endpoint).list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter='/', MaxKeys=LIMIT)
    dirs = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
    files = [o['Key'] for o in response.get('Contents', []) if o['Key'] != prefix]
    return dirs, files


def _gcs_client(project: Optional[str], credentials: Optional[str]):
    from google.cloud import storage
    kwargs = {}
    if project:
        kwargs['project'] = project
    if credentials:
        from google.oauth2 import service_account
        kwargs['credentials'] = service_account.Credentials.from_service_account_file(credentials)
    return storage.Client(**kwargs)


def _list_gcs_buckets(project: Optional[str], credentials: Optional[str]) -> List[str]:
    client = _gcs_client(project, credentials)
    return [b.name for b in client.list_buckets(max_results=LIMIT, timeout=READ_TIMEOUT)]


def _shallow_list_gcs(bucket: str, prefix: str, project: Optional[str],
                      credentials: Optional[str]) -> Tuple[List[str], List[str]]:
    client = _gcs_client(project, credentials)
    iterator = client.list_blobs(bucket, prefix=prefix, delimiter='/',
                                 max_results=LIMIT, timeout=READ_TIMEOUT)
    files = [b.name for b in iterator if b.name != prefix]
    dirs = sorted(iterator.prefixes)  # populated after iteration
    return dirs, files


def _abfss_service(account: str, access_key: Optional[str]):
    import os
    from azure.storage.filedatalake import DataLakeServiceClient
    url = f"https://{account}.dfs.core.windows.net"
    key = access_key or os.environ.get('AZURE_STORAGE_ACCESS_KEY')
    if key:
        return DataLakeServiceClient(account_url=url, credential=key,
                                     connection_timeout=CONNECT_TIMEOUT, read_timeout=READ_TIMEOUT)
    from azure.identity import DefaultAzureCredential
    return DataLakeServiceClient(account_url=url, credential=DefaultAzureCredential(),
                                 connection_timeout=CONNECT_TIMEOUT, read_timeout=READ_TIMEOUT)


def _list_abfss_containers(account: str, access_key: Optional[str]) -> List[str]:
    from itertools import islice
    service = _abfss_service(account, access_key)
    return [fs.name for fs in islice(service.list_file_systems(timeout=READ_TIMEOUT), LIMIT)]


def _shallow_list_abfss(container: str, account: str, prefix: str,
                        access_key: Optional[str]) -> Tuple[List[str], List[str]]:
    from itertools import islice
    service = _abfss_service(account, access_key)
    fs = service.get_file_system_client(container)
    dirs, files = [], []
    parent = prefix.rsplit('/', 1)[0] if '/' in prefix else ''
    paths = fs.get_paths(path=parent or None, recursive=False, timeout=READ_TIMEOUT)
    for p in islice(paths, LIMIT * 2):
        if not p.name.startswith(prefix):
            continue
        (dirs if p.is_directory else files).append(p.name + ('/' if p.is_directory else ''))
    return dirs[:LIMIT], files[:LIMIT]


# --------------------------------------------------------------------------
# the completer
# --------------------------------------------------------------------------

def _param(ctx, name: str) -> Optional[str]:
    """A parsed option from the partial command line, if the shell saw it."""
    try:
        value = ctx.params.get(name)
        return value if isinstance(value, str) and value else None
    except Exception:
        return None


def _cloud_candidates(ctx, incomplete: str) -> List[str]:
    """Full-word candidates for an incomplete cloud URL."""
    scheme, rest = incomplete.split('://', 1)
    scheme = scheme.lower()
    profile = _param(ctx, 'profile')
    project = _param(ctx, 'project')
    credentials = _param(ctx, 'credentials')
    az_key = _param(ctx, 'az_access_key')
    endpoint = _param(ctx, 'endpoint_url') or os.environ.get('AWS_ENDPOINT_URL_S3') \
        or os.environ.get('AWS_ENDPOINT_URL')

    if scheme == 'file':
        return []  # handled by the file-completion fallback in complete_path

    if scheme == 'r2' and not endpoint:
        _debug("r2:// completion needs --endpoint-url (or AWS_ENDPOINT_URL_S3)")
        return []
    s3_endpoint = endpoint if scheme in ('s3', 'r2') else None

    if '/' not in rest:
        # Completing the bucket/container part.
        if scheme in ('s3', 'r2'):
            names = _list_s3_buckets(profile, s3_endpoint)
        elif scheme in ('gs', 'gcs'):
            names = _list_gcs_buckets(project, credentials)
        elif scheme == 'abfss':
            if '@' not in rest:
                return []  # abfss needs container@account; accounts aren't listable
            container_part, host = rest.split('@', 1)
            if '.' not in host:
                return []
            account = host.split('.')[0]
            return [f"{scheme}://{name}@{host}/"
                    for name in _list_abfss_containers(account, az_key)
                    if name.startswith(container_part)][:LIMIT]
        else:
            return []
        return [f"{scheme}://{name}/" for name in sorted(names)
                if name.startswith(rest)][:LIMIT]

    # Completing a key/prefix inside a bucket.
    bucket, prefix = rest.split('/', 1)
    if scheme in ('s3', 'r2'):
        dirs, files = _shallow_list_s3(bucket, prefix, profile, s3_endpoint)
    elif scheme in ('gs', 'gcs'):
        dirs, files = _shallow_list_gcs(bucket, prefix, project, credentials)
    elif scheme == 'abfss' and '@' in bucket:
        container, host = bucket.split('@', 1)
        account = host.split('.')[0]
        dirs, files = _shallow_list_abfss(container, account, prefix, az_key)
    else:
        return []

    base = f"{scheme}://{bucket}/"
    candidates = [base + d for d in dirs] + [base + f for f in files]
    return sorted(candidates)[:LIMIT]


def complete_path(ctx, param, incomplete: str) -> List[CompletionItem]:
    """shell_complete callback for the PATH argument."""
    try:
        if '://' in incomplete:
            return [CompletionItem(c) for c in _cloud_candidates(ctx, incomplete)]

        items: List[CompletionItem] = []
        # Offer scheme prefixes while a scheme could still be being typed
        # (e.g. "s3" or "gc"), alongside normal file completion.
        if incomplete and '/' not in incomplete:
            items.extend(
                CompletionItem(scheme)
                for scheme in _SCHEMES
                if scheme.startswith(incomplete)
            )
        items.append(CompletionItem(incomplete, type='file'))
        return items
    except Exception:
        # Completion must never break the shell: no candidates beats a stack.
        import traceback
        _debug(f"complete_path({incomplete!r}) failed:\n{traceback.format_exc()}")
        return []
