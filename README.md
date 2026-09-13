# Reactive Relational Database Connectivity Microsoft SQL Server Implementation [![Java CI with Maven](https://github.com/r2dbc/r2dbc-mssql/actions/workflows/ci.yml/badge.svg)](https://github.com/r2dbc/r2dbc-mssql/actions/workflows/ci.yml) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-mssql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-mssql)
 

This project contains the [Microsoft SQL Server][m] implementation of the [R2DBC SPI][r]. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to

[m]: https://microsoft.com/sqlserver
[r]: https://github.com/r2dbc/r2dbc-spi

This driver provides the following features:

* Complies with R2DBC 1.0
* Login with username/password with temporary SSL encryption
* Full SSL encryption support (for e.g. Azure usage).
* Transaction Control
* Simple execution of SQL batches (direct and cursored execution)
* Execution of parametrized statements (direct and cursored execution)
* Extensive type support (including `TEXT`, `VARCHAR(MAX)`, `IMAGE`, `VARBINARY(MAX)` and national variants, see below for exceptions)
* Execution of stored procedures
* Input table-valued parameters with buffered or publisher-backed rows and Blob/Clob cell streaming

Next steps:

* Extend TVP type and metadata support, and add UDT support

## Code of Conduct

This project is governed by the [R2DBC Code of Conduct](https://github.com/r2dbc/.github/blob/main/CODE_OF_CONDUCT.adoc). By participating, you are expected to uphold this code of conduct. Please
report unacceptable behavior to [info@r2dbc.io](mailto:info@r2dbc.io).

## Getting Started

Here is a quick teaser of how to use R2DBC MSSQL in Java:

**URL Connection Factory Discovery**

```java
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:mssql://<host>:1433/<database>");

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
```

**Programmatic Connection Factory Discovery**

```java
ConnectionFactoryOptions options = builder()
    .option(DRIVER, "sqlserver")
    .option(HOST, "…")
    .option(PORT, …)  // optional, defaults to 1433
    .option(USER, "…")
    .option(PASSWORD, "…")
    .option(DATABASE, "…") // optional
    .option(SSL, true) // optional, defaults to false
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
| `port`                          | Server port to connect to. Defaults to `1433`. _(Optional)_                                                                                                                                                                                                               
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
    .username("…")
    .password("…")
    .database("…")
    .preferCursoredExecution(…)
    .build();

MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

Mono<MssqlConnection> connectionMono = factory.create();
```

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

### Maven configuration

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

## Table-Valued Parameters

`MssqlTableValue` binds a table value to a parameterized statement.
Create the corresponding SQL Server table type in the target database first.
TVPs are input-only. They are declared `READONLY`; query results are consumed through the usual R2DBC result API.

### Buffered rows

Create this type once as part of the database schema:

```sql
CREATE TYPE dbo.OrderInput AS TABLE (
    id INT NOT NULL,
    amount DECIMAL(18, 2) NULL,
    note NVARCHAR(MAX) NULL,
    payload VARBINARY(MAX) NULL
);
```

Declare columns and cells in the same order as the SQL table type:

```java
import io.r2dbc.mssql.MssqlTableValue;
import io.r2dbc.mssql.message.type.SqlServerType;
import java.math.BigDecimal;
import reactor.core.publisher.Flux;

MssqlTableValue orders = MssqlTableValue.builder("dbo.OrderInput")
    .column("id", SqlServerType.INTEGER)
    .column("amount", SqlServerType.DECIMAL, 18, 2)
    .columnMax("note", SqlServerType.NVARCHAR)
    .columnMax("payload", SqlServerType.VARBINARY)
    .row(1, new BigDecimal("12.50"), "First order", new byte[] {1, 2})
    .row(2, null, "", new byte[0])
    .build();

// connection is an open R2DBC Connection managed by the caller.
Flux<Integer> ids = Flux.from(connection.createStatement(
        "SELECT id FROM @orders ORDER BY id")
    .bind("orders", orders)
    .execute())
    .concatMap(result -> result.map((row, metadata) -> row.get("id", Integer.class)));
```

Subscribe to and consume `ids` as part of the application's reactive pipeline.
The same binding can be used in a SQL command that calls a stored procedure accepting the table type.

Each row must have exactly one cell per declared column. Column order determines the mapping; column names do not reorder cells.
A table built without rows is an empty TVP. A `null` cell is SQL `NULL`; for a single-column table use `.row((Object) null)`.
Empty strings and empty binary values remain distinct from `NULL`.
Bind the table value directly; support for `Parameters.in(table)` is pending.
`bindNull(..., MssqlTableValue.class)` cannot supply the required SQL table type name; use an explicitly constructed empty table when no rows are needed.

### Supported TVP cell types

This table describes TVP input cells. The general data type mapping above does not imply the same conversions inside TVPs.

| SQL column | Builder type | Java cell value |
| --- | --- | --- |
| `BIT` | `SqlServerType.BIT` | `Boolean` |
| `SMALLINT` | `SqlServerType.SMALLINT` | `Short` |
| `INT` | `SqlServerType.INTEGER` | `Integer` |
| `BIGINT` | `SqlServerType.BIGINT` | `Long` |
| `UNIQUEIDENTIFIER` | `SqlServerType.GUID` | `java.util.UUID` |
| `DATE` | `SqlServerType.DATE` | `java.time.LocalDate`, years 1 through 9999 |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `DECIMAL` or `NUMERIC`, with precision and scale | `BigDecimal` |
| `NVARCHAR(n)` | `SqlServerType.NVARCHAR` | `String`, at most 4000 UTF-16 code units |
| `NVARCHAR(MAX)` | `.columnMax(..., SqlServerType.NVARCHAR)` | `String` or `io.r2dbc.spi.Clob` |
| `VARBINARY(n)` | `SqlServerType.VARBINARY` | `byte[]`, at most 8000 bytes |
| `VARBINARY(MAX)` | `.columnMax(..., SqlServerType.VARBINARY)` | `byte[]` or `io.r2dbc.spi.Blob` |

All listed cell types also accept `null`, subject to the server's table constraints.
Decimal precision must be 1 through 38 and scale must be 0 through precision.
Values are rescaled exactly; precision overflow and values requiring rounding are rejected.
Non-MAX string and binary columns advertise the limits above; the SQL table type can impose a smaller limit.
Use `columnMax` explicitly for MAX columns. It does not automatically follow from a large value.
Explicit TVP `NVARCHAR` columns remain Unicode when `sendStringParametersAsUnicode=false`.

### Streaming rows and cells

Use `.rows(Publisher<? extends List<?>>)` for rows produced during execution.
It cannot be combined with `.row(...)`.
Clobs and Blobs stream their contents in MAX columns, including when the surrounding rows were added with `.row(...)`.

The following example uses the same `dbo.OrderInput` type:

```java
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

MssqlTableValue streamedOrders = MssqlTableValue.builder("dbo.OrderInput")
    .column("id", SqlServerType.INTEGER)
    .column("amount", SqlServerType.DECIMAL, 18, 2)
    .columnMax("note", SqlServerType.NVARCHAR)
    .columnMax("payload", SqlServerType.VARBINARY)
    .rows(Flux.defer(() -> {
        Clob note = Clob.from(Flux.<CharSequence>just("First ", "order"));
        Blob payload = Blob.from(Flux.just(
            ByteBuffer.wrap(new byte[] {1, 2}),
            ByteBuffer.wrap(new byte[] {3, 4})));
        return Flux.<List<?>>just(
            Arrays.asList(1, new BigDecimal("12.50"), note, payload));
    }))
    .build();
```

Bind `streamedOrders` as in the buffered example.
Use demand-aware publishers; a publisher that has already collected all rows still retains that collection.
The encoder does not subscribe to row or cell sources at bind time.
It processes rows and LOB cells sequentially, without row or cell prefetch.
Scalar-only buffered tables are encoded as a complete value; publisher-backed scalar rows are encoded one row at a time.
Strings and byte arrays remain fully materialized even in MAX columns.
LOB streaming allocates an encoded buffer for each source chunk; memory also depends on source chunk sizes and downstream transport buffering.
Clob encoding preserves UTF-16 code units across source chunk boundaries, including split surrogate pairs.
Blob encoding preserves the position and limit of supplied ByteBuffers.

### Ownership, validation, and current limits

* Do not mutate buffered cell objects after building a table, or published rows and chunks after emitting them. Mutable cell objects are not deep-copied.
* Row publishers must not emit `null` rows. Cells may be `null`. Row values are validated when encoded; with streaming, errors can occur after earlier rows have been transmitted.
* Blob and Clob instances are consumed once. For repeated execution, supply a repeatable row publisher that creates fresh LOB instances for each subscription, as above.
* After a LOB stream is subscribed, cancellation is propagated to that stream. The driver calls `discard()` for an acquired LOB only if its stream has not been subscribed.
* Applications remain responsible for LOBs not reached by the encoder, including values in later rows or cells after an earlier failure, and values belonging to a statement that is never executed.
* Type names currently require a simple `schema.type` form: each component starts with an ASCII letter or underscore, followed by up to 127 ASCII letters, digits, or underscores. Quoted identifiers and database-qualified names are not supported.
* Declare between 1 and 1024 columns. Server constraints remain authoritative; column declarations are not discovered from the server.
* Server-default columns and sort/unique metadata are not supported. Datatypes not listed above are not currently accepted by the TVP encoder.

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
