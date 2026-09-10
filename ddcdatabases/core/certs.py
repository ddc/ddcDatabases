"""
Construction of the client SSL contexts used by the async drivers.
"""

import os
import ssl
from collections.abc import Iterable


class SSLCertificateError(OSError):
    """A configured certificate path could not be loaded"""


def build_client_ssl_context(
    ca_cert_path: str,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    minimum_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_3,
) -> ssl.SSLContext:
    """Build a client SSL context, naming the file when one cannot be loaded"""

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = minimum_version

    try:
        context.load_verify_locations(cafile=ca_cert_path)
    except OSError as err:
        raise SSLCertificateError(f"CA certificate unusable: {ca_cert_path} | {err}") from err

    if client_cert_path and client_key_path:
        try:
            context.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
        except OSError as err:
            raise SSLCertificateError(
                f"client certificate/key unusable: {client_cert_path}, {client_key_path} | {err}"
            ) from err

    return context


def verify_cert_paths(entries: Iterable[tuple[str | None, str]]) -> None:
    """Raise SSLCertificateError naming every configured path that is not usable"""

    problems: list[str] = []
    for path, label in entries:
        if not path:
            continue
        if not os.path.exists(path):
            problems.append(f"{label} missing: {path}")
            continue
        # a directory (an Oracle wallet) must also be traversable, not merely readable
        required = os.R_OK | os.X_OK if os.path.isdir(path) else os.R_OK
        if not os.access(path, required):
            problems.append(f"{label} not readable: {path}")
    if problems:
        raise SSLCertificateError("; ".join(problems))
