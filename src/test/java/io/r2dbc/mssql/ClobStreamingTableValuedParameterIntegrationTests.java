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
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL Server integration coverage for streamed Clob cells in TVPs.
 */
class ClobStreamingTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        SERVER.getJdbcOperations().execute("CREATE TYPE dbo.r2dbc_tvp_clob_stream_table AS TABLE " +
                "(ordinal INT NOT NULL, value NVARCHAR(MAX) NULL, marker INT NOT NULL)");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldStreamLargeClobFromBufferedRow() throws Exception {
        assertLargeClob(false);
    }

    @Test
    void shouldStreamLargeClobFromRowPublisher() throws Exception {
        assertLargeClob(true);
    }

    @Test
    void shouldRoundTripClobEmptyNullAndStringCells() {
        Clob clob = Clob.from(Flux.<CharSequence>just("A\u6f22\ud83d", "", "\ude0aZ"));
        MssqlTableValue table = tableBuilder().rows(Flux.<List<?>>just(
                Arrays.asList(1, clob, 101),
                Arrays.asList(2, Clob.from(Flux.empty()), 102),
                Arrays.asList(3, null, 103),
                Arrays.asList(4, "\u00e4\u6f22\ud83d\ude0a", 104))).build();
        assertRows(table, "1:A\u6f22\ud83d\ude0aZ:101", "2::102", "3:NULL:103",
                "4:\u00e4\u6f22\ud83d\ude0a:104");
    }

    @Test
    void shouldCreateFreshClobForEachExecution() {
        AtomicInteger subscriptions = new AtomicInteger();
        MssqlTableValue table = tableBuilder().rows(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            Clob clob = Clob.from(Flux.<CharSequence>just("\u00e4", "\u6f22"));
            return Flux.<List<?>>just(Arrays.asList(1, clob, 101));
        })).build();
        assertRows(table, "1:\u00e4\u6f22:101");
        assertRows(table, "1:\u00e4\u6f22:101");
        assertThat(subscriptions.get()).isEqualTo(2);
    }

    private static void assertLargeClob(boolean publisherRows) throws Exception {
        // More than 1 MiB of UTF-16LE text. Every pair of source chunks splits an emoji.
        char[] padding = new char[2044];
        Arrays.fill(padding, '\u6f22');
        String prefix = new String(padding) + "\ud83d";
        String suffix = "\ude0a\u00e4Z";
        String text = prefix + suffix; // 2048 UTF-16 code units, 4096 bytes.
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < 257; i++) {
            expected.update(text.getBytes(StandardCharsets.UTF_16LE));
        }
        AtomicInteger emitted = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        Clob clob = Clob.from(Flux.range(0, 257)
                .concatMap(i -> Flux.<CharSequence>just(prefix, "", suffix), 0)
                .doOnNext(ignored -> emitted.incrementAndGet())
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        MssqlTableValue.Builder builder = tableBuilder();
        MssqlTableValue table = publisherRows ?
                builder.rows(Flux.<List<?>>just(Arrays.asList(1, clob, 77))).build() :
                builder.row(1, clob, 77).build();
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
        assertThat(emitted.get()).isEqualTo(771);
    }

    private static MssqlTableValue.Builder tableBuilder() {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_clob_stream_table")
                .column("ordinal", SqlServerType.INTEGER)
                .columnMax("value", SqlServerType.NVARCHAR)
                .column("marker", SqlServerType.INTEGER);
    }

    private static void assertRows(MssqlTableValue table, String... expected) {
        connection.createStatement("SELECT ordinal, value, marker FROM @values ORDER BY ordinal")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) -> {
                    String value = row.get("value", String.class);
                    return row.get("ordinal", Integer.class) + ":" +
                            (value == null ? "NULL" : value) + ":" +
                            row.get("marker", Integer.class);
                }))
                .as(StepVerifier::create).expectNext(expected)
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {
        SERVER.getJdbcOperations().execute("IF TYPE_ID(N'dbo.r2dbc_tvp_clob_stream_table') IS NOT NULL " +
                "DROP TYPE dbo.r2dbc_tvp_clob_stream_table");
    }
}
