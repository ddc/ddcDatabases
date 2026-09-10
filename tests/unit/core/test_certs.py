"""The SSL context builder"""

import os
import pytest
import ssl
from ddcdatabases.core.certs import SSLCertificateError, build_client_ssl_context, verify_cert_paths
from unittest.mock import patch


@pytest.fixture
def certs_dir(tmp_path):
    for name in ("ca.pem", "client.crt", "client.key"):
        (tmp_path / name).write_text("x")
    return str(tmp_path)


class TestBuildClientSslContext:
    def test_missing_ca_names_the_file(self, tmp_path):
        """The exact production failure, now attributable."""
        absent = str(tmp_path / "postgres.ca.pem")
        with pytest.raises(SSLCertificateError) as err:
            build_client_ssl_context(absent)
        assert absent in str(err.value)
        assert "CA certificate" in str(err.value)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
    def test_unreadable_ca_names_the_file(self, certs_dir):
        ca = os.path.join(certs_dir, "ca.pem")
        os.chmod(ca, 0o000)
        try:
            with pytest.raises(SSLCertificateError) as err:
                build_client_ssl_context(ca)
            assert ca in str(err.value)
        finally:
            os.chmod(ca, 0o600)

    def test_malformed_pem_names_the_file(self, certs_dir):
        """What a stat-based pre-check cannot catch: present, readable, still unloadable."""
        ca = os.path.join(certs_dir, "ca.pem")
        with pytest.raises(SSLCertificateError) as err:
            build_client_ssl_context(ca)
        assert ca in str(err.value)

    def test_missing_client_pair_names_both_paths(self, certs_dir, tmp_path):
        crt, key = str(tmp_path / "absent.crt"), str(tmp_path / "absent.key")
        with (
            patch.object(ssl.SSLContext, "load_verify_locations"),
            pytest.raises(SSLCertificateError) as err,
        ):
            build_client_ssl_context(os.path.join(certs_dir, "ca.pem"), crt, key)
        assert crt in str(err.value)
        assert key in str(err.value)

    def test_error_is_an_oserror(self):
        """Existing broad `except OSError` handlers in consumers must keep catching."""
        assert issubclass(SSLCertificateError, OSError)

    def test_original_error_is_chained(self, tmp_path):
        with pytest.raises(SSLCertificateError) as err:
            build_client_ssl_context(str(tmp_path / "absent.pem"))
        assert isinstance(err.value.__cause__, FileNotFoundError)

    def test_pins_tls_13_and_requires_verification(self, certs_dir):
        with patch.object(ssl.SSLContext, "load_verify_locations"):
            context = build_client_ssl_context(os.path.join(certs_dir, "ca.pem"))
        assert context.minimum_version == ssl.TLSVersion.TLSv1_3
        assert context.verify_mode == ssl.CERT_REQUIRED

    def test_minimum_version_is_overridable(self, certs_dir):
        with patch.object(ssl.SSLContext, "load_verify_locations"):
            context = build_client_ssl_context(
                os.path.join(certs_dir, "ca.pem"),
                minimum_version=ssl.TLSVersion.TLSv1_2,
            )
        assert context.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_client_pair_is_optional(self, certs_dir):
        with (
            patch.object(ssl.SSLContext, "load_verify_locations"),
            patch.object(ssl.SSLContext, "load_cert_chain") as load_chain,
        ):
            build_client_ssl_context(os.path.join(certs_dir, "ca.pem"))
        load_chain.assert_not_called()

    def test_client_pair_is_loaded_when_given(self, certs_dir):
        crt = os.path.join(certs_dir, "client.crt")
        key = os.path.join(certs_dir, "client.key")
        with (
            patch.object(ssl.SSLContext, "load_verify_locations"),
            patch.object(ssl.SSLContext, "load_cert_chain") as load_chain,
        ):
            build_client_ssl_context(os.path.join(certs_dir, "ca.pem"), crt, key)
        load_chain.assert_called_once_with(certfile=crt, keyfile=key)


class TestVerifyCertPaths:
    """For drivers that open the certificate themselves, this is the only place to name it."""

    def test_passes_when_every_path_exists(self, certs_dir):
        verify_cert_paths(
            (
                (os.path.join(certs_dir, "ca.pem"), "CA certificate"),
                (os.path.join(certs_dir, "client.crt"), "client certificate"),
                (os.path.join(certs_dir, "client.key"), "client key"),
            )
        )

    def test_unset_paths_are_not_an_error(self):
        """Not configuring mTLS is a valid configuration, not a failure."""
        verify_cert_paths(((None, "CA certificate"), ("", "client key")))

    def test_names_the_absent_file(self, tmp_path):
        absent = str(tmp_path / "ca.pem")
        with pytest.raises(SSLCertificateError) as err:
            verify_cert_paths(((absent, "CA certificate"),))
        assert str(err.value) == f"CA certificate missing: {absent}"

    def test_reports_every_problem_at_once(self, tmp_path):
        """The production case: the mount is present but its contents are gone."""
        paths = [str(tmp_path / n) for n in ("ca.pem", "client.crt", "client.key")]
        labels = ("CA certificate", "client certificate", "client key")
        with pytest.raises(SSLCertificateError) as err:
            verify_cert_paths(zip(paths, labels, strict=True))
        for path in paths:
            assert path in str(err.value)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
    def test_unreadable_is_distinguished_from_absent(self, certs_dir):
        ca = os.path.join(certs_dir, "ca.pem")
        os.chmod(ca, 0o000)
        try:
            with pytest.raises(SSLCertificateError) as err:
                verify_cert_paths(((ca, "CA certificate"),))
            assert "not readable" in str(err.value)
        finally:
            os.chmod(ca, 0o600)

    def test_directory_is_accepted(self, certs_dir):
        """An Oracle wallet is a directory, not a file."""
        verify_cert_paths(((certs_dir, "wallet directory"),))

    @pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
    def test_untraversable_directory_is_rejected(self, tmp_path):
        """A readable-but-not-executable directory cannot be opened by the driver."""
        wallet = tmp_path / "wallet"
        wallet.mkdir()
        os.chmod(wallet, 0o600)
        try:
            with pytest.raises(SSLCertificateError) as err:
                verify_cert_paths(((str(wallet), "wallet directory"),))
            assert "not readable" in str(err.value)
        finally:
            os.chmod(wallet, 0o700)
