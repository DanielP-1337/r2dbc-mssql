# 1.1.0-danielp.2 - Named Instances, Kerberos and TLS

Regular GitHub release of the DanielP-1337 fork of r2dbc-mssql.
This release updates documentation and the project version. Production driver
source is unchanged from `1.1.0-danielp.1`.

## Features

- SQL Server named instance discovery through SQL Server Browser (SSRP).
- Windows Integrated Security through Windows SSPI using Negotiate.
- Combined named instance discovery and integrated authentication using the
  resolved TCP port when constructing the SQL Server SPN.
- TLS with certificate validation and support for a private CA truststore.

## Installation

Add `https://jitpack.io` to your Maven or Gradle repositories and use:

`com.github.DanielP-1337:r2dbc-mssql:1.1.0-danielp.2`

Windows Integrated Security additionally requires:

`net.java.dev.jna:jna-platform:5.17.0`

Replace the upstream `io.r2dbc:r2dbc-mssql` dependency, including transitive
occurrences, to avoid loading both driver variants.

## Validation

Manual Windows lab validation on September 11, 2026 used the previous release
checkout, with a test-only extension for named-instance configuration:

- Explicit-port connection: expected Windows identity and KERBEROS verified.
- Named instance with dynamic TCP discovery: expected identity and KERBEROS verified.
- Named instance + Kerberos + TLS: passed with a trusted private CA and
  `trustServerCertificate=false` (1 test, 0 failures/errors/skips).
- Negative TLS trust test: correctly failed with SSLHandshakeException and
  PKIX path building failed when the private CA was absent from Java trust.
- The preceding release's unit suite passed 948 tests. The new version is rebuilt
  and its JitPack dependency is resolved before this release is published.

The production driver code is unchanged. The complete database integration suite
and a dedicated wrong-hostname negative test were not run. Live Windows tests
were separate from the release builds, which exclude database integration tests.

## Kerberos and TLS setup

Kerberos requires the SPN matching the FQDN and actual TCP port, registered on
the appropriate SQL service identity. SQL Server attempts automatic registration;
missing AD permissions (0x2098) can require an administrator to fix permissions
or register the SPN. Missing or outdated entries can cause NTLM fallback;
incorrect ownership can cause authentication errors. Dynamic port changes require
matching SPNs.

TLS separately requires a valid server certificate matching the hostname and a
trusted chain in the Java truststore. Windows root trust alone is not sufficient
for every JVM configuration. Keep `trustServerCertificate=false` for validation.

See the [README shipped with this tag](https://github.com/DanielP-1337/r2dbc-mssql/blob/1.1.0-danielp.2/README.md).

The existing `1.1.0-danielp.1` tag and artifacts remain unchanged. This is a regular
GitHub release; the `-danielp.2` suffix remains a prerelease identifier under strict
Semantic Versioning. This fork release is not an upstream project endorsement.
