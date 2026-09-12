# Reactive Relational Database Connectivity Microsoft SQL Server Implementation [![Java CI with Maven](https://github.com/r2dbc/r2dbc-mssql/actions/workflows/ci.yml/badge.svg)](https://github.com/r2dbc/r2dbc-mssql/actions/workflows/ci.yml) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-mssql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-mssql)
 

This project contains the [Microsoft SQL Server][m] implementation of the [R2DBC SPI][r]. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to

[m]: https://microsoft.com/sqlserver
[r]: https://github.com/r2dbc/r2dbc-spi

This driver provides the following features:

* Complies with R2DBC 1.0
* Login with username/password with temporary SSL encryption
* Windows Integrated Security using the credentials of the current Windows process
* Full SSL encryption support (for e.g. Azure usage).
* SQL Server named instance discovery through SQL Server Browser (SSRP)
* Transaction Control
* Simple execution of SQL batches (direct and cursored execution)
* Execution of parametrized statements (direct and cursored execution)
* Extensive type support (including `TEXT`, `VARCHAR(MAX)`, `IMAGE`, `VARBINARY(MAX)` and national variants, see below for exceptions)
* Execution of stored procedures

Next steps:

* Add support for TVP and UDTs

## Code of Conduct

This project is governed by the [R2DBC Code of Conduct](https://github.com/r2dbc/.github/blob/main/CODE_OF_CONDUCT.adoc). By participating, you are expected to uphold this code of conduct. Please
report unacceptable behavior to [info@r2dbc.io](mailto:info@r2dbc.io).

## DanielP-1337 fork release

This fork adds SQL Server named instance discovery and Windows Integrated Security.
The current fork version is **1.1.0-danielp.2**, distributed through JitPack.
This is an independently maintained fork release; it is not an upstream release.

Use the following Maven configuration for this fork:

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.github.DanielP-1337</groupId>
    <artifactId>r2dbc-mssql</artifactId>
    <version>1.1.0-danielp.2</version>
  </dependency>
  <!-- Required when using Windows Integrated Security -->
  <dependency>
    <groupId>net.java.dev.jna</groupId>
    <artifactId>jna-platform</artifactId>
    <version>5.17.0</version>
  </dependency>
</dependencies>
```

Replace any direct upstream `io.r2dbc:r2dbc-mssql` dependency and exclude it
where it is brought in transitively, so only one driver variant is loaded.

For Gradle, add `maven { url = uri("https://jitpack.io") }` to your dependency
repositories and use:

```groovy
implementation 'com.github.DanielP-1337:r2dbc-mssql:1.1.0-danielp.2'
implementation 'net.java.dev.jna:jna-platform:5.17.0' // Windows Integrated Security
```

The GitHub release label and the version string are separate: this is published
as a regular GitHub release, while the `-danielp.2` suffix is a prerelease
identifier under strict Semantic Versioning. Pin the complete version string.

## Getting Started

Here is a quick teaser of how to use R2DBC MSSQL in Java:

**URL Connection Factory Discovery**

```java
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:mssql://<host>:1433/<database>");

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
```

To discover the TCP port of a named SQL Server instance, specify `instanceName`
and omit the port:
`r2dbc:mssql://<host>/<database>?instanceName=SQLEXPRESS`.

When connecting through a tunnel or proxy, `serverName` can specify the logical SQL Server name independently from the physical connection endpoint:

```java
ConnectionFactory connectionFactory = ConnectionFactories.get(
    "r2dbc:mssql://localhost:15433/<database>?serverName=server.database.windows.net");
```

**Programmatic Connection Factory Discovery**

```java
ConnectionFactoryOptions options = builder()
    .option(DRIVER, "sqlserver")
    .option(HOST, "…")
    .option(PORT, …)  // optional, defaults to 1433
    .option(MssqlConnectionFactoryProvider.INSTANCE_NAME, "SQLEXPRESS") // optional
    .option(USER, "…")
    .option(PASSWORD, "…")
    .option(DATABASE, "…") // optional
    .option(SSL, true) // optional, defaults to false
    .option(Option.valueOf("serverName"), "server.example.com") // optional, defaults to host
    .option(Option.valueOf("applicationName"), "…") // optional
    .option(Option.valueOf("preferCursoredExecution"), true/false) // optional
    .option(Option.valueOf("connectionId"), new UUID(…)) // optional
    .build();

ConnectionFactory connectionFactory = ConnectionFactories.get(options);

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

**Supported ConnectionFactory Discovery Options**

| Option                          | Description                                                                                                                                                                                                                                                               
|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| `ssl`                           | Whether to use transport-level encryption for the entire SQL server traffic.                                                                                                                                                                                              
| `driver`                        | Must be `sqlserver`.                                                                                                                                                                                                                                                      
| `host`                          | Server hostname to connect to.                                                                                                                                                                                                                                            
| `serverName`                    | Logical SQL Server name used for LOGIN7 and TLS/SNI when it differs from the physical connection endpoint. Defaults to `host`. _(Optional)_
| `port`                          | Server port to connect to. Defaults to `1433` when no named instance is configured. If both `port` and `instanceName` are specified, the explicit port takes precedence. _(Optional)_
| `instanceName`                  | SQL Server named instance. If no explicit port is configured, the driver resolves the TCP port through SQL Server Browser using SSRP over UDP port `1434`. _(Optional)_
| `integratedSecurity`            | Use Windows Integrated Security with the credentials of the current Windows process. When enabled, `username` and `password` are not required. Windows only. Defaults to `false`. _(Optional)_
| `username`                      | Login username.                                                                                                                                                                                                                                                           
| `password`                      | Login password.                                                                                                                                                                                                                                                           
| `database`                      | Initial database to select. Defaults to SQL Server user profile settings. _(Optional)_                                                                                                                                                                                    
| `applicationName`               | Name of the application. Defaults to driver name and version. _(Optional)_                                                                                                                                                                                                
| `connectionId`                  | Connection Id for tracing purposes. Defaults to a random Id. _(Optional)_                                                                                                                                                                                                 
| `connectionProvider`            | Set the `reactor.netty.resources.ConnectionProvider` to be used when creating the connection. Defaults to `ConnectionProvider.newConnection()`. _(Optional)_                                                                                                              
| `connectTimeout`                | Connection Id for tracing purposes. Defaults to 30 seconds. _(Optional)_                                                                                                                                                                                                  
| `hostNameInCertificate`         | Expected hostname in SSL certificate. Supports wildcards (e.g. `*.database.windows.net`). _(Optional)_                                                                                                                                                                    
| `lockWaitTimeout`               | Lock wait timeout using `SET LOCK_TIMEOUT …`. _(Optional)_                                                                                                                                                                                                                
| `preferCursoredExecution`       | Whether to prefer cursors  or direct execution for queries. Uses by default direct. Cursors require more round-trips but are more backpressure-friendly. Defaults to direct execution. Can be `boolean` or a `Predicate<String>` accepting the SQL query. _(Optional)_    
| `sendStringParametersAsUnicode` | Configure whether to send character data as unicode (NVARCHAR, NCHAR, NTEXT) or whether to use the database encoding, defaults to `true`. If disabled, `CharSequence` data is sent using the database-specific collation such as ASCII/MBCS instead of Unicode.           
| `sslTunnel`                     | Enables SSL tunnel usage when using a SSL tunnel or SSL terminator in front of SQL Server. Accepts `Function<SslContextBuilder, SslContextBuilder>` to customize the SSL tunnel settings. SSL tunneling is not related to SQL Server's built-in SSL support. _(Optional)_ 
| `sslContextBuilderCustomizer`   | SSL Context customizer to configure SQL Server's built-in SSL support (`Function<SslContextBuilder, SslContextBuilder>`) _(Optional)_                                                                                                                                     
| `tcpKeepAlive`                  | Enable/disable TCP KeepAlive. Disabled by default. _(Optional)_                                                                                                                                                                                                           
| `tcpNoDelay`                    | Enable/disable TCP NoDelay. Enabled by default. _(Optional)_                                                                                                                                                                                                              
| `trustServerCertificate`        | Fully trust the server certificate bypassing X.509 certificate validation. Disabled by default. _(Optional)_                                                                                                                                                              
| `trustStoreType`                | Type of the TrustStore. Defaults to `KeyStore.getDefaultType()`. _(Optional)_                                                                                                                                                                                             
| `trustStore`                    | Path to the certificate TrustStore file. _(Optional)_                                                                                                                                                                                                                     
| `trustStorePassword`            | Password used to check the integrity of the TrustStore data. _(Optional)_                                                                                                                                                                                                 


**Programmatic Configuration**

```java
MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
    .host("…")
    .serverName("server.example.com") // optional, defaults to host
    .instanceName("SQLEXPRESS") // optional
    .username("…")
    .password("…")
    .database("…")
    .preferCursoredExecution(…)
    .build();

MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

Mono<MssqlConnection> connectionMono = factory.create();
```

### Windows Integrated Security

Windows Integrated Security uses the credentials of the current Windows process through
Windows SSPI with the `Negotiate` security package. SQL Server authentication with
username/password remains unchanged.

URL-based configuration:

```java
ConnectionFactory connectionFactory = ConnectionFactories.get(
    "r2dbc:mssql://sql.example.com:1433/database?integratedSecurity=true");
```

Programmatic discovery:

```java
ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
    .option(DRIVER, "sqlserver")
    .option(HOST, "sql.example.com")
    .option(PORT, 1433)
    .option(DATABASE, "database")
    .option(Option.valueOf("integratedSecurity"), true)
    .build();

ConnectionFactory connectionFactory = ConnectionFactories.get(options);
```

Direct configuration:

```java
MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
    .host("sql.example.com")
    .port(1433)
    .database("database")
    .integratedSecurity()
    .build();

MssqlConnectionFactory connectionFactory = new MssqlConnectionFactory(configuration);
```

Integrated Security is currently supported on Windows only. It uses the identity of the
process running the JVM; explicit Windows username/password credentials are not part of
this authentication mode. The SQL Server instance must permit Windows authentication and
the current process identity must have a SQL Server login or otherwise be granted access.

The driver constructs the SQL Server service principal name (SPN) from the connection
endpoint as `MSSQLSvc/<host>:<port>`. Use the SQL Server DNS/FQDN as the host when Kerberos
authentication is required. Windows `Negotiate` determines whether Kerberos or NTLM is
used according to the Windows environment and security policy.

Windows Integrated Security uses JNA to access Windows SSPI. JNA is an optional driver
dependency, so applications using Integrated Security must include `jna-platform`:

```xml
<dependency>
  <groupId>net.java.dev.jna</groupId>
  <artifactId>jna-platform</artifactId>
  <version>5.17.0</version>
</dependency>
```

The Windows Integrated Security integration test is intentionally not backed by the
regular SQL Server Testcontainers setup because it requires a real Windows identity and
an SQL Server configured to accept that identity. On Windows, configure at least
`R2DBC_MSSQL_INTEGRATED_HOST`; the other variables are optional:

```powershell
$env:R2DBC_MSSQL_INTEGRATED_HOST = "sql.example.com"
$env:R2DBC_MSSQL_INTEGRATED_PORT = "1433"
$env:R2DBC_MSSQL_INTEGRATED_DATABASE = "master"
$env:R2DBC_MSSQL_INTEGRATED_EXPECTED_USER = "DOMAIN\user"
$env:R2DBC_MSSQL_INTEGRATED_SSL = "false"
$env:R2DBC_MSSQL_INTEGRATED_TRUST_SERVER_CERTIFICATE = "false"

.\mvnw.cmd "-Dtest=WindowsIntegratedSecurityIntegrationTests" test
```

If `R2DBC_MSSQL_INTEGRATED_HOST` is not set, the integration test is skipped.

#### Kerberos prerequisites and SPN troubleshooting

Kerberos requires a matching service principal name (SPN) registered in Active
Directory on the account representing the SQL Server service. Enabling
`integratedSecurity=true` does not guarantee Kerberos: Windows `Negotiate` can
select NTLM when Kerberos is unavailable and policy permits fallback.

SQL Server attempts to register its SPNs at startup and unregister them at
shutdown. Automatic registration requires the appropriate Active Directory
permissions. If registration fails, an administrator must correct those
permissions or register the required SPN manually. The driver and SQL Server
Browser do not register SPNs.

For this driver's TCP connections, the SPN is `MSSQLSvc/<host>:<port>`.
Use the server FQDN as `host`. With `instanceName` and no explicit port, the driver
uses the TCP port returned by SQL Server Browser when constructing the SPN.
For example, `sql.example.com` on port `64190` requires
`MSSQLSvc/sql.example.com:64190`; an entry ending in `:1433` or `:MYINSTANCE`
does not match that TCP connection.

| Configuration issue | Possible result | Action |
| --- | --- | --- |
| Required SPN is missing, or only an SPN for another port exists | NTLM fallback, or authentication failure when fallback is unavailable or prohibited | Register the SPN matching the connection hostname and actual TCP port. |
| SPN is registered on the wrong account, or duplicated | Kerberos/SSPI authentication errors | Have an AD administrator resolve ownership and duplicates. |
| Dynamic TCP port changes but the matching SPN is not registered | Discovery succeeds but Kerberos cannot authenticate the new endpoint | Restore automatic registration or update manually managed SPNs after port changes. |
| SQL Server logs `0x2098` (`8344`, `ERROR_DS_INSUFF_ACCESS_RIGHTS`) during registration | Automatic registration lacks sufficient AD permissions | Review SPN permissions on the relevant AD object. |

A domain service account owns its SQL Server SPNs. For a virtual service account
such as `NT SERVICE\MSSQL$MYINSTANCE`, the network identity is the server's
computer account, for example `EXAMPLE\SQLHOST$`.

An administrator can query ownership and, if necessary, register the matching
SPN with duplicate checking. The following PowerShell example assumes a virtual
service account; replace the hostname, port, and computer account for your setup:

```powershell
setspn -Q "MSSQLSvc/sql.example.com:64190"
setspn -S "MSSQLSvc/sql.example.com:64190" 'EXAMPLE\SQLHOST$'
```

Inspect registration messages on the affected SQL Server instance:

```sql
EXEC master.dbo.xp_readerrorlog 0, 1, N'Service Principal Name';
```

Verify the authentication scheme on the actual application connection from the
remote client, rather than a separate SSMS connection:

```sql
SELECT SUSER_SNAME() AS login_name,
       CAST(CONNECTIONPROPERTY('auth_scheme') AS varchar(40)) AS auth_scheme;
```

Expect `KERBEROS` when validating Kerberos; a successful login using `NTLM`
does not establish that Kerberos works. TLS certificate validation is a separate
requirement: `trustServerCertificate=true` does not resolve SPN problems.

See Microsoft's documentation on
[SPN registration](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/register-a-service-principal-name-for-kerberos-connections),
[service accounts](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-windows-service-accounts-and-permissions), and
[misplaced SPNs](https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/connect/explicit-spn-is-misplaced).

#### TLS certificate validation with Kerberos and named instances

Use `ssl=true` and `trustServerCertificate=false` to enable TLS with certificate
validation. Kerberos authentication and TLS server certificate validation are
separate checks; a successful Kerberos login does not establish certificate trust.

For a private CA, configure a Java truststore containing its public certificate.
Importing the CA into Windows Trusted Root Certification Authorities does not
necessarily make it trusted by Java. A separate PKCS12 truststore avoids changing
the JDK-wide `cacerts` file.

Example using the driver configuration directly (obtain `trustStorePassword`
from your application's secret configuration as a `char[]`):

```java
MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
    .host("sql.example.com")
    .instanceName("MYINSTANCE")
    .database("master")
    .integratedSecurity()
    .enableSsl()
    .trustServerCertificate(false)
    .trustStore("C:/MyLabTLS/mylab-sql-truststore.p12")
    .trustStoreType("PKCS12")
    .trustStorePassword(trustStorePassword)
    .build();
```

Omit the explicit port to exercise SQL Server Browser discovery. The SQL Server
instance must have TCP/IP enabled; its TCP port and Browser UDP port 1434 must
be reachable. With Listen All enabled, configure the TCP settings under IPAll.
Restart the instance after changing its TCP or certificate configuration.

The server certificate must be valid at the client time and include the connection
hostname in its DNS SAN. Its private key must be accessible to the SQL service.
Use a SQL-compatible certificate with Server Authentication EKU and
`AT_KEYEXCHANGE`; bind it to the correct instance. The instance name and TCP port
are not DNS SAN values. Correct server/client clocks before issuing certificates:
changing the clock afterward does not change an existing certificate's validity.

| Symptom | Check |
| --- | --- |
| `PKIX path building failed` / no valid certification path | CA trust in the actual Java truststore, the served certificate, and the certificate chain. |
| Certificate not yet valid or expired | Client/server UTC time and certificate validity; reissue certificates created with an incorrect clock. |
| Hostname validation failure | Connection FQDN against certificate DNS SAN entries. |
| TLS succeeds but authentication is NTLM | SPN ownership and the actual resolved TCP port, separately from certificate trust. |

For JSSE default trust configuration, the integration test can receive
`javax.net.ssl.trustStore`, `javax.net.ssl.trustStoreType=PKCS12`, and
`javax.net.ssl.trustStorePassword` as Maven `-D` properties. Treat command lines
and generated test reports as potentially containing the truststore password.

A negative trust test must keep `ssl=true` and `trustServerCertificate=false`:
with a truststore that lacks the issuing CA, the handshake should fail due to
certificate trust. A timeout, SQL login error, or skipped test is not evidence
that certificate validation works.

See [SQL Server certificate requirements](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/certificate-requirements)
and the [JSSE truststore documentation](https://docs.oracle.com/en/java/javase/18/security/java-secure-socket-extension-jsse-reference-guide.html).

#### Recorded validation and scope

Manual lab validation on September 11, 2026 used the `1.1.0-danielp.1` release
checkout. The integration test was locally extended to accept a named-instance
setting; that test-only extension is not part of this release's repository test.
The production driver source is unchanged in `1.1.0-danielp.2`.

| Check | Observed result |
| --- | --- |
| Prior release unit suite | 948 tests passed. |
| Explicit TCP port with Windows Integrated Security | Expected domain identity and `KERBEROS` verified. |
| Named instance with dynamic TCP port discovery | Expected domain identity and `KERBEROS` verified without an explicit port. |
| Combined named instance + Kerberos + TLS with trusted private CA | One integration test passed with `trustServerCertificate=false`. |
| Same TLS test without private CA trust in Java | Expected `SSLHandshakeException` / `PKIX path building failed`. |

These results cover the tested lab configuration, not every SQL Server deployment.
A dedicated wrong-hostname negative test and the complete database integration
suite were not run. Local and JitPack release builds use `-DskipITs`; the live
Windows tests are separate from those builds. The release workflow rebuilds and
resolves the new JitPack version before publishing the regular GitHub release.

### Named SQL Server Instances

Named instances can use dynamic TCP ports. Configure the instance name with the
`instanceName` option and omit `port` to let the driver discover the current TCP
port through SQL Server Browser using the SQL Server Resolution Protocol (SSRP).

The SQL Server Browser service must be running on the target host and UDP port
`1434` must be reachable from the client.

Connection target precedence is:

* Host only: connect to the default TCP port `1433`.
* Host and explicit `port`: connect to the configured port.
* Host and `instanceName`: resolve the TCP port through SQL Server Browser.
* Host, `instanceName`, and explicit `port`: the explicit port takes precedence
  and SQL Server Browser discovery is skipped.

SQL Server routing redirects use the server-provided host and port directly and
do not trigger another named-instance lookup.

Microsoft SQL Server uses named parameters that are prefixed with `@`. The following SQL statement makes use of parameters:

```sql
INSERT INTO person (id, first_name, last_name) VALUES(@id, @firstname, @lastname)
```

Parameters are referenced without the `@` prefix when binding these:

```java
connection.createStatement("INSERT INTO person (id, first_name, last_name) VALUES(@id, @firstname, @lastname)")
            .bind("id", 1)
            .bind("firstname", "Walter")
            .bind("lastname", "White")
            .execute()
``` 

Binding also allows positional index (zero-based) references. The parameter index is derived from the parameter discovery order when parsing the query.

### Upstream Maven configuration

The following coordinates are for upstream releases. For this fork, use the
JitPack dependency shown above.

Artifacts can be found on [Maven Central](https://central.sonatype.com/search?q=r2dbc-mssql).

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-mssql</artifactId>
  <version>${version}</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-mssql</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>central-portal-snapshots</id>
  <name>Central Portal Snapshots</name>
  <url>https://central.sonatype.com/repository/maven-snapshots/</url>
</repository>
``` 

## Transaction Definitions

SQL Server supports additional options when starting a transaction. In particular, the following options can be specified:

* Isolation Level (`isolationLevel`) (reset after the transaction to previous value)
* Transaction Name (`name`)
* Transaction Log Mark (`mark`)
* Lock Wait Timeout (`lockWaitTimeout`) (reset after the transaction to `-1`)

These options can be specified upon transaction begin to start the transaction and apply options in a single command roundtrip:

```java
MssqlConnection connection= …;

        connection.beginTransaction(MssqlTransactionDefinition.from(IsolationLevel.READ_UNCOMMITTED)
        .name("my-transaction").mark("tx-log-mark")
        .lockTimeout(Duration.ofMinutes(1)));
```

See also: https://docs.microsoft.com/en-us/sql/t-sql/language-elements/begin-transaction-transact-sql

### Data Type Mapping

This reference table shows the type mapping between [Microsoft SQL Server][m] and Java data types:

| Microsoft SQL Server Type                | Java Data Type                                                                                                                                                                                                          | 
|:-----------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`bit`][sql-bit-ref]                     | [**`Boolean`**][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`tinyint`][sql-all-int-ref]             | [**`Byte`**][java-byte-ref], [`Boolean`][java-boolean-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`smallint`][sql-all-int-ref]            | [**`Short`**][java-short-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Integer`][java-integer-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`int`][sql-all-int-ref]                 | [**`Integer`**][java-integer-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Long`][java-long-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`bigint`][sql-all-int-ref]              | [**`Long`**][java-long-ref], [`Boolean`][java-boolean-ref], [`Byte`][java-byte-ref], [`Short`][java-short-ref], [`Integer`][java-integer-ref], [`BigDecimal`][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref] |
| [`real`][sql-float-real-ref]             | [**`Float`**][java-float-ref], [`Double`][java-double-ref]                                                                                                                                                              
| [`float`][sql-float-real-ref]            | [**`Double`**][java-double-ref], [`Float`][java-float-ref]                                                                                                                                                              
| [`decimal`][sql-decimal-ref]             | [**`BigDecimal`**][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]                                                                                                                                            
| [`numeric`][sql-decimal-ref]             | [**`BigDecimal`**][java-bigdecimal-ref], [`BigInteger`][java-biginteger-ref]                                                                                                                                            
| [`uniqueidentifier`][sql-uid-ref]        | [**`UUID`**][java-uuid-ref], [`String`][java-string-ref]                                                                                                                                                                
| [`smalldatetime`][sql-smalldatetime-ref] | [`LocalDateTime`][java-ldt-ref]                                                                                                                                                                                         
| [`datetime`][sql-datetime-ref]           | [`LocalDateTime`][java-ldt-ref]                                                                                                                                                                                         
| [`datetime2`][sql-datetime2-ref]         | [`LocalDateTime`][java-ldt-ref]                                                                                                                                                                                         
| [`date`][sql-date-ref]                   | [`LocalDate`][java-ld-ref]                                                                                                                                                                                              
| [`time`][sql-time-ref]                   | [`LocalTime`][java-lt-ref]                                                                                                                                                                                              
| [`datetimeoffset`][sql-dtof-ref]         | [**`OffsetDateTime`**][java-odt-ref], [`ZonedDateTime`][java-zdt-ref]                                                                                                                                                   
| [`timestamp`][sql-timestamp-ref]         | [`byte[]`][java-byte-ref]                                                                                                                                                                                               
| [`smallmoney`][sql-money-ref]            | [`BigDecimal`][java-bigdecimal-ref]                                                                                                                                                                                     
| [`money`][sql-money-ref]                 | [`BigDecimal`][java-bigdecimal-ref]                                                                                                                                                                                     
| [`char`][sql-(var)char-ref]              | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`varchar`][sql-(var)char-ref]           | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`varcharmax`][sql-(var)char-ref]        | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`nchar`][sql-n(var)char-ref]            | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`nvarchar`][sql-n(var)char-ref]         | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`nvarcharmax`][sql-n(var)char-ref]      | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`text`][sql-(n)text-ref]                | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`ntext`][sql-(n)text-ref]               | [`String`][java-string-ref], [`Clob`][r2dbc-clob-ref]                                                                                                                                                                   
| [`image`][sql-(n)text-ref]               | [**`ByteBuffer`**][java-ByteBuffer-ref], [`byte[]`][java-byte-ref], [`Blob`][r2dbc-blob-ref]                                                                                                                            
| [`binary`][sql-binary-ref]               | [**`ByteBuffer`**][java-ByteBuffer-ref], [`byte[]`][java-byte-ref], [`Blob`][r2dbc-blob-ref]                                                                                                                            
| [`varbinary`][sql-binary-ref]            | [**`ByteBuffer`**][java-ByteBuffer-ref], [`byte[]`][java-byte-ref], [`Blob`][r2dbc-blob-ref]                                                                                                                            
| [`varbinarymax`][sql-binary-ref]         | [**`ByteBuffer`**][java-ByteBuffer-ref], [`byte[]`][java-byte-ref], [`Blob`][r2dbc-blob-ref]                                                                                                                            
| [`sql_variant`][sql-sql-variant-ref]     | Not yet supported.                                                                                                                                                                                                      
| [`xml`][sql-xml-ref]                     | Not yet supported.                                                                                                                                                                                                      
| [`udt`][sql-udt-ref]                     | Not yet supported.                                                                                                                                                                                                      
| [`geometry`][sql-geometry-ref]           | Experimental since v1.0.6 through `com.microsoft.sqlserver.jdbc.Geometry` (JDBC driver type)                                                                                                                            
| [`geography`][sql-geography-ref]         | Experimental since v1.0.6 through `com.microsoft.sqlserver.jdbc.Geography` (JDBC driver type)                                                                                                                           

Types in **bold** indicate the native (default) Java type.

**Note:** BLOB (`image`, `binary`, `varbinary` and `varbinary(max)`) and CLOB (`text`, `ntext`, `varchar(max)` and `nvarchar(max)`)
values are fully materialized in the client before decoding. Make sure to account for proper memory sizing.


[sql-bit-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql?view=sql-server-2017
[sql-all-int-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql?view=sql-server-2017
[sql-float-real-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-2017
[sql-decimal-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-2017
[sql-smalldatetime-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/smalldatetime-transact-sql?view=sql-server-2017
[sql-datetime-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-2017
[sql-datetime2-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-2017
[sql-date-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-2017
[sql-time-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/time-transact-sql?view=sql-server-2017
[sql-timestamp-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-2017
[sql-uid-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql?view=sql-server-2017
[sql-dtof-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-2017
[sql-money-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/money-and-smallmoney-transact-sql?view=sql-server-2017
[sql-(var)char-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-2017

[sql-n(var)char-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql?view=sql-server-2017

[sql-(n)text-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql?view=sql-server-2017

[sql-binary-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-2017

[sql-sql-variant-ref]: https://docs.microsoft.com/en-us/sql/t-sql/data-types/sql-variant-transact-sql?view=sql-server-2017

[sql-xml-ref]: https://docs.microsoft.com/en-us/sql/t-sql/xml/xml-transact-sql?view=sql-server-2017

[sql-udt-ref]: https://docs.microsoft.com/en-us/sql/relational-databases/clr-integration-database-objects-user-defined-types/clr-user-defined-types?view=sql-server-2017

[sql-geometry-ref]: https://docs.microsoft.com/en-us/sql/t-sql/spatial-geometry/spatial-types-geometry-transact-sql?view=sql-server-2017

[sql-geography-ref]: https://docs.microsoft.com/en-us/sql/t-sql/spatial-geography/spatial-types-geography?view=sql-server-2017

[r2dbc-blob-ref]: https://r2dbc.io/spec/1.0.0.RELEASE/api/io/r2dbc/spi/Blob.html

[r2dbc-clob-ref]: https://r2dbc.io/spec/1.0.0.RELEASE/api/io/r2dbc/spi/Clob.html

[java-bigdecimal-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html

[java-biginteger-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigInteger.html

[java-boolean-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Boolean.html

[java-byte-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html

[java-ByteBuffer-ref]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html

[java-double-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html

[java-float-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Float.html

[java-integer-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Integer.html

[java-long-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html
[java-ldt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java-ld-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java-lt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java-odt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html
[java-short-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Short.html
[java-string-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
[java-uuid-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/UUID.html
[java-zdt-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html

## Logging

If SL4J is on the classpath, it will be used. Otherwise, there are two possible fallbacks: Console or `java.util.logging.Logger`). By default, the Console fallback is used. To use the JDK loggers, set the `reactor.logging.fallback` System property to `JDK`.

Logging facilities:

* Driver Logging (`io.r2dbc.mssql`)
* Query Logging (`io.r2dbc.mssql.QUERY` on `DEBUG` level)
* Transport Logging (`io.r2dbc.mssql.client`)
    * `DEBUG` enables `Message` exchange logging
    * `TRACE` enables traffic logging

## Getting Help

Having trouble with R2DBC? We'd love to help!

* Check the [spec documentation](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/), and [Javadoc](https://r2dbc.io/spec/1.0.0.RELEASE/api/).
* If you are upgrading, check out the [changelog](https://r2dbc.io/spec/1.0.0.RELEASE/CHANGELOG.txt) for "new and noteworthy" features.
* Ask a question - we monitor [stackoverflow.com](https://stackoverflow.com) for questions tagged with [`r2dbc`](https://stackoverflow.com/tags/r2dbc). You can also chat with the community
  on [Gitter](https://gitter.im/r2dbc/r2dbc).
* Report bugs with R2DBC MSSQL at [github.com/r2dbc/r2dbc-mssql/issues](https://github.com/r2dbc/r2dbc-mssql/issues).

## Reporting Issues

R2DBC uses GitHub as issue tracking system to record bugs and feature requests. 
If you want to raise an issue, please follow the recommendations below:

* Before you log a bug, please search the [issue tracker](https://github.com/r2dbc/r2dbc-mssql/issues) to see if someone has already reported the problem.
* If the issue doesn't already exist, [create a new issue](https://github.com/r2dbc/r2dbc-mssql/issues/new).
* Please provide as much information as possible with the issue report, we like to know the version of R2DBC MSSQL that you are using and JVM version.
* If you need to paste code, or include a stack trace use Markdown ``` escapes before and after your text.
* If possible try to create a test-case or project that replicates the issue. 
Attach a link to your code or a compressed file containing your code.

## Building from Source

You don't need to build from source to use R2DBC MSSQL (binaries in Maven Central), but if you want to try out the latest and greatest, R2DBC MSSQL can be easily built with the
[maven wrapper](https://github.com/takari/maven-wrapper). You also need JDK 1.8 and Docker to run integration tests.

```bash
 $ ./mvnw clean install
```

If you want to build with the regular `mvn` command, you will need [Maven v3.6.0 or above](https://maven.apache.org/run-maven/index.html).

_Also see [CONTRIBUTING.adoc](https://github.com/r2dbc/.github/blob/main/CONTRIBUTING.adoc) if you wish to submit pull requests. Commits require `Signed-off-by` (`git commit -s`) to ensure [Developer Certificate of Origin](https://developercertificate.org/)._

### Running JMH Benchmarks

Running the JMH benchmarks builds and runs the benchmarks without running tests.

```bash
 $ ./mvnw clean install -Pjmh
```

## Staging to Maven Central

To stage a release to Maven Central, you need to create a release tag (release version) that contains the desired state
and version numbers (
`mvn versions:set versions:commit -q -o -DgenerateBackupPoms=false -DnewVersion=x.y.z.(RELEASE|Mnnn|RCnnn`) and
force-push it to the `release` branch. This push will trigger a Maven staging build.

## License

R2DBC MSSQL is Open Source software released under the [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0.html).
