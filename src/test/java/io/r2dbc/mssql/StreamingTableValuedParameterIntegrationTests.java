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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL Server integration coverage for publisher-backed TVP rows.
 */
class StreamingTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        SERVER.getJdbcOperations().execute("CREATE TYPE dbo.r2dbc_tvp_stream_table AS TABLE " +
                "(ordinal INT NOT NULL, amount DECIMAL(9,2) NULL, textvalue NVARCHAR(MAX) NULL, " +
                "binaryvalue VARBINARY(MAX) NULL, marker INT NOT NULL)");
        SERVER.getJdbcOperations().execute("CREATE PROCEDURE dbo.r2dbc_tvp_stream_echo " +
                "@values dbo.r2dbc_tvp_stream_table READONLY AS BEGIN SET NOCOUNT ON; " +
                "SELECT ordinal, amount, textvalue, binaryvalue, marker FROM @values ORDER BY ordinal; END");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldRoundTripMixedTypesAndMaxCells() {
        String text = String.join("", Collections.nCopies(4001, "x")) + "\u6f22\ud83d\ude0a";
        byte[] bytes = new byte[8001];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        AtomicInteger emitted = new AtomicInteger();
        Flux<List<?>> rows = Flux.<List<?>>just(
                Arrays.asList(1, new BigDecimal("1.2300"), text, bytes, 101),
                Arrays.asList(2, null, null, null, 102),
                Arrays.asList(3, BigDecimal.ZERO, "", new byte[0], 103))
                .doOnNext(ignored -> emitted.incrementAndGet());
        MssqlTableValue table = table(rows);
        assertThat(emitted.get()).isZero();
        assertRows(table, "1:1.23:" + text + ":" + ByteBufUtil.hexDump(bytes) + ":101",
                "2:null:null:NULL:102", "3:0.00:::103");
        assertThat(emitted.get()).isEqualTo(3);
    }

    @Test
    void shouldSendEmptyRowPublisher() {
        assertRows(table(Flux.empty()));
    }

    @Test
    void shouldSendNullColumnsFromPublisher() {
        assertRows(table(Flux.<List<?>>just(Arrays.asList(1, null, null, null, 101))),
                "1:null:null:NULL:101");
    }

    @Test
    void shouldStreamTenThousandRows() {
        AtomicInteger emitted = new AtomicInteger();
        MssqlTableValue table = table(Flux.range(1, 10000).<List<?>>map(ordinal -> {
            emitted.incrementAndGet();
            return Arrays.asList(ordinal, null, null, null, ordinal);
        }));
        connection.createStatement("SELECT COUNT_BIG(*) AS row_count, " +
                        "SUM(CAST(ordinal AS BIGINT)) AS total FROM @values")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("row_count", Long.class) + ":" + row.get("total", Long.class)))
                .as(StepVerifier::create).expectNext("10000:50005000")
                .expectComplete().verify(Duration.ofSeconds(60));
        assertThat(emitted.get()).isEqualTo(10000);
    }

    @Test
    void shouldExecuteSameTableWithRepeatableSourceTwice() {
        AtomicInteger subscriptions = new AtomicInteger();
        MssqlTableValue table = table(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.<List<?>>just(Arrays.asList(1, new BigDecimal("2.5"), "repeat", new byte[]{1}, 101));
        }));
        assertRows(table, "1:2.50:repeat:01:101");
        assertRows(table, "1:2.50:repeat:01:101");
        assertThat(subscriptions.get()).isEqualTo(2);
    }

    private static MssqlTableValue table(Flux<? extends List<?>> rows) {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_stream_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("amount", SqlServerType.DECIMAL, 9, 2)
                .columnMax("textvalue", SqlServerType.NVARCHAR)
                .columnMax("binaryvalue", SqlServerType.VARBINARY)
                .column("marker", SqlServerType.INTEGER).rows(rows).build();
    }

    private static void assertRows(MssqlTableValue table, String... expected) {
        connection.createStatement("EXEC dbo.r2dbc_tvp_stream_echo @values")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) -> {
                    byte[] binary = row.get("binaryvalue", byte[].class);
                    return row.get("ordinal", Integer.class) + ":" + row.get("amount", BigDecimal.class) + ":" +
                            row.get("textvalue", String.class) + ":" +
                            (binary == null ? "NULL" : ByteBufUtil.hexDump(binary)) + ":" +
                            row.get("marker", Integer.class);
                }))
                .as(StepVerifier::create).expectNext(expected)
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {
        SERVER.getJdbcOperations().execute("IF OBJECT_ID(N'dbo.r2dbc_tvp_stream_echo', N'P') IS NOT NULL " +
                "DROP PROCEDURE dbo.r2dbc_tvp_stream_echo");
        SERVER.getJdbcOperations().execute("IF TYPE_ID(N'dbo.r2dbc_tvp_stream_table') IS NOT NULL " +
                "DROP TYPE dbo.r2dbc_tvp_stream_table");
    }
}
