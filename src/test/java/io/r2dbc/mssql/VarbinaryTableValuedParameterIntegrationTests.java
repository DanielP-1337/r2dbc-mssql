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
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * SQL Server round-trip tests for VARBINARY TVP cells.
 */
class VarbinaryTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_varbinary_table AS TABLE " +
                        "(ordinal INT NOT NULL, value VARBINARY(8000) NULL, marker INT NOT NULL)");
        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_varbinary_echo " +
                        "@values dbo.r2dbc_tvp_varbinary_table READONLY AS " +
                        "BEGIN SET NOCOUNT ON; " +
                        "SELECT ordinal, value, marker FROM @values ORDER BY ordinal; END");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldRoundTripAllByteValuesEmptyNullAndMaximumLength() {
        byte[] allBytes = new byte[256];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) i;
        }
        byte[] maximum = new byte[8000];
        for (int i = 0; i < maximum.length; i++) {
            maximum[i] = (byte) (i % 256);
        }
        MssqlTableValue table = binaryTable()
                .row(1, allBytes, 101)
                .row(2, new byte[0], 102)
                .row(3, null, 103)
                .row(4, maximum, 104)
                .row(5, new byte[]{0, (byte) 0xff, 0, 0}, 105)
                .build();
        assertRows(table,
                "1:" + ByteBufUtil.hexDump(allBytes) + ":101",
                "2::102", "3:NULL:103",
                "4:" + ByteBufUtil.hexDump(maximum) + ":104",
                "5:00ff0000:105");
    }

    @Test
    void shouldSendEmptyVarbinaryTable() {
        assertRows(binaryTable().build());
    }

    @Test
    void shouldSendAllNullVarbinaryColumn() {
        assertRows(binaryTable().row(1, null, 101).row(2, null, 102).build(),
                "1:NULL:101", "2:NULL:102");
    }

    private static MssqlTableValue.Builder binaryTable() {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_varbinary_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("value", SqlServerType.VARBINARY)
                .column("marker", SqlServerType.INTEGER);
    }

    private static void assertRows(MssqlTableValue table, String... expected) {
        connection.createStatement("EXEC dbo.r2dbc_tvp_varbinary_echo @values")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) -> {
                    byte[] value = row.get("value", byte[].class);
                    // Hex preserves every byte and distinguishes empty values from SQL NULL.
                    return row.get("ordinal", Integer.class) + ":" +
                            (value == null ? "NULL" : ByteBufUtil.hexDump(value)) + ":" +
                            row.get("marker", Integer.class);
                }))
                .as(StepVerifier::create)
                .expectNext(expected).expectComplete().verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {
        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_varbinary_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_varbinary_echo");
        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_varbinary_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_varbinary_table");
    }
}
