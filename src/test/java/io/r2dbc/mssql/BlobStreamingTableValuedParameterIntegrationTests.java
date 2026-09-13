/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL Server integration coverage for streamed Blob cells in TVPs.
 */
class BlobStreamingTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        SERVER.getJdbcOperations().execute("CREATE TYPE dbo.r2dbc_tvp_blob_stream_table AS TABLE " +
                "(ordinal INT NOT NULL, value VARBINARY(MAX) NULL, marker INT NOT NULL)");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldStreamLargeBlobFromBufferedRow() throws Exception {
        assertLargeBlob(false);
    }

    @Test
    void shouldStreamLargeBlobFromRowPublisher() throws Exception {
        assertLargeBlob(true);
    }

    @Test
    void shouldRoundTripBlobEmptyNullAndByteArrayCells() {
        Blob blob = Blob.from(Flux.just(ByteBuffer.wrap(new byte[]{0, (byte) 0x80}),
                ByteBuffer.allocate(0), ByteBuffer.wrap(new byte[]{(byte) 0xff, 0})));
        MssqlTableValue table = tableBuilder().rows(Flux.<List<?>>just(
                Arrays.asList(1, blob, 101),
                Arrays.asList(2, Blob.from(Flux.empty()), 102),
                Arrays.asList(3, null, 103),
                Arrays.asList(4, new byte[]{1, 2}, 104))).build();
        assertRows(table, "1:0080ff00:101", "2::102", "3:NULL:103", "4:0102:104");
    }

    @Test
    void shouldCreateFreshBlobForEachExecution() {
        AtomicInteger subscriptions = new AtomicInteger();
        MssqlTableValue table = tableBuilder().rows(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            Blob blob = Blob.from(Flux.just(ByteBuffer.wrap(new byte[]{1, 2})));
            return Flux.<List<?>>just(Arrays.asList(1, blob, 101));
        })).build();
        assertRows(table, "1:0102:101");
        assertRows(table, "1:0102:101");
        assertThat(subscriptions.get()).isEqualTo(2);
    }

    private static void assertLargeBlob(boolean publisherRows) throws Exception {
        // Generate 257 chunks of 4096 bytes: slightly more than 1 MiB.
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < 257; i++) {
            byte[] bytes = new byte[4096];
            Arrays.fill(bytes, (byte) i);
            expected.update(bytes);
        }
        AtomicInteger emitted = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        Blob blob = Blob.from(Flux.range(0, 257).map(i -> {
            emitted.incrementAndGet();
            byte[] bytes = new byte[4096];
            Arrays.fill(bytes, (byte) (int) i);
            return ByteBuffer.wrap(bytes);
        }).doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        MssqlTableValue.Builder builder = tableBuilder();
        MssqlTableValue table = publisherRows ?
                builder.rows(Flux.<List<?>>just(Arrays.asList(1, blob, 77))).build() :
                builder.row(1, blob, 77).build();
        assertThat(subscriptions.get()).isZero();
        String hash = ByteBufUtil.hexDump(expected.digest()).toUpperCase(Locale.ROOT);
        connection.createStatement("SELECT DATALENGTH(value) AS bytes, " +
                        "CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', value), 2) AS digest, marker FROM @values")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("bytes", Long.class) + ":" + row.get("digest", String.class) + ":" +
                                row.get("marker", Integer.class)))
                .as(StepVerifier::create).expectNext("1052672:" + hash + ":77")
                .expectComplete().verify(Duration.ofSeconds(60));
        assertThat(subscriptions.get()).isEqualTo(1);
        assertThat(emitted.get()).isEqualTo(257);
    }

    private static MssqlTableValue.Builder tableBuilder() {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_blob_stream_table")
                .column("ordinal", SqlServerType.INTEGER)
                .columnMax("value", SqlServerType.VARBINARY)
                .column("marker", SqlServerType.INTEGER);
    }

    private static void assertRows(MssqlTableValue table, String... expected) {
        connection.createStatement("SELECT ordinal, value, marker FROM @values ORDER BY ordinal")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) -> {
                    byte[] bytes = row.get("value", byte[].class);
                    return row.get("ordinal", Integer.class) + ":" +
                            (bytes == null ? "NULL" : ByteBufUtil.hexDump(bytes)) + ":" +
                            row.get("marker", Integer.class);
                }))
                .as(StepVerifier::create).expectNext(expected)
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {
        SERVER.getJdbcOperations().execute("IF TYPE_ID(N'dbo.r2dbc_tvp_blob_stream_table') IS NOT NULL " +
                "DROP TYPE dbo.r2dbc_tvp_blob_stream_table");
    }
}
