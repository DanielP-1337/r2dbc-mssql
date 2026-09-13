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

import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;

/**
 * Integration tests for NVARCHAR cells in table-valued parameters.
 */
class NvarcharTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {

        dropSchema();
        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_nvarchar_table AS TABLE " +
                        "(ordinal INT NOT NULL, value NVARCHAR(4000) NULL)");

        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_nvarchar_echo " +
                        "@values dbo.r2dbc_tvp_nvarchar_table READONLY AS " +
                        "BEGIN SET NOCOUNT ON; " +
                        "SELECT ordinal, value FROM @values ORDER BY ordinal; END");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldRoundTripUnicodeEmptyNullAndMaximumLengthWithIntegerColumn() {
        assertUnicodeRoundTrip(connection);
    }

    @Test
    void shouldPreserveExplicitNvarcharWhenUnicodeOptionIsDisabled() {

        MssqlConnectionFactory factory = new MssqlConnectionFactoryProvider().create(builder()
                .option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, false)
                .build());

        MssqlConnection nonUnicodeConnection = factory.create().block(Duration.ofSeconds(30));
        try {
            assertUnicodeRoundTrip(nonUnicodeConnection);
        } finally {
            nonUnicodeConnection.close().block(Duration.ofSeconds(30));
        }
    }

    @Test
    void shouldSendEmptyNvarcharTable() {
        assertRows(connection, nvarcharTable().build());
    }

    @Test
    void shouldSendAllNullNvarcharColumn() {
        assertRows(connection, nvarcharTable().row(1, null).row(2, null).build(), "1:null", "2:null");
    }

    private static void assertUnicodeRoundTrip(MssqlConnection testConnection) {

        String unicode = "A\u00e4\u6f22\ud83d\ude0a";
        String maximum = String.join("", Collections.nCopies(3998, "x")) + "\ud83d\ude0a";
        MssqlTableValue table = nvarcharTable()
                .row(1, unicode)
                .row(2, "")
                .row(3, null)
                .row(4, maximum)
                .row(5, " trailing spaces  ")
                .build();

        assertRows(testConnection, table,
                "1:" + unicode, "2:", "3:null", "4:" + maximum, "5: trailing spaces  ");
    }

    private static MssqlTableValue.Builder nvarcharTable() {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_nvarchar_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("value", SqlServerType.NVARCHAR);
    }

    private static void assertRows(MssqlConnection testConnection, MssqlTableValue table, String... expected) {

        testConnection.createStatement("EXEC dbo.r2dbc_tvp_nvarchar_echo @values")
                .bind("@values", table)
                .execute()
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("ordinal", Integer.class) + ":" + row.get("value", String.class)))
                .as(StepVerifier::create)
                .expectNext(expected)
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_nvarchar_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_nvarchar_echo");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_nvarchar_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_nvarchar_table");
    }
}
