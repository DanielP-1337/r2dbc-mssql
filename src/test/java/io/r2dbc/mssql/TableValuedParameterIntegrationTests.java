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

/**
 * Integration tests for table-valued parameters.
 */
class TableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {

        dropSchema();

        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_int_table AS TABLE " +
                        "(value INT NOT NULL)");

        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_echo " +
                        "@values dbo.r2dbc_tvp_int_table READONLY AS " +
                        "BEGIN " +
                        "SET NOCOUNT ON; " +
                        "SELECT value FROM @values ORDER BY value; " +
                        "END");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldSendSingleIntegerRow() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_int_table")
                .column("value", SqlServerType.INTEGER)
                .row(42)
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_echo @values")
                .bind("@values", table)
                .execute()
                .concatMap(result -> result.map(
                        (row, metadata) -> row.get("value", Integer.class)))
                .as(StepVerifier::create)
                .expectNext(42)
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    @Test
    void shouldSendMultipleIntegerRows() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_int_table")
                .column("value", SqlServerType.INTEGER)
                .row(42)
                .row(-7)
                .row(0)
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_echo @values")
                .bind("@values", table)
                .execute()
                .concatMap(result -> result.map(
                        (row, metadata) -> row.get("value", Integer.class)))
                .as(StepVerifier::create)
                .expectNext(-7, 0, 42)
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    @Test
    void shouldSendEmptyIntegerTable() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_int_table")
                .column("value", SqlServerType.INTEGER)
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_echo @values")
                .bind("@values", table)
                .execute()
                .concatMap(result -> result.map(
                        (row, metadata) -> row.get("value", Integer.class)))
                .as(StepVerifier::create)
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    @Test
    void shouldSendNullIntegerCell() {

        dropNullableSchema();
        try {
            SERVER.getJdbcOperations().execute(
                    "CREATE TYPE dbo.r2dbc_tvp_nullable_int_table AS TABLE " +
                            "(value INT NULL)");

            SERVER.getJdbcOperations().execute(
                    "CREATE PROCEDURE dbo.r2dbc_tvp_nullable_summary " +
                            "@values dbo.r2dbc_tvp_nullable_int_table READONLY AS " +
                            "BEGIN " +
                            "SET NOCOUNT ON; " +
                            "SELECT COUNT(*) AS total_count, COUNT(value) AS non_null_count, " +
                            "SUM(value) AS total_value FROM @values; " +
                            "END");

            MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_nullable_int_table")
                    .column("value", SqlServerType.INTEGER)
                    .row(0)
                    .row((Object) null)
                    .row(42)
                    .build();

            // Use aggregates to verify NULL without emitting a null reactive item.
            // A dropped row or a NULL converted to zero must not pass this test.
            connection.createStatement("EXEC dbo.r2dbc_tvp_nullable_summary @values")
                    .bind("@values", table)
                    .execute()
                    .concatMap(result -> result.map((row, metadata) ->
                            row.get("total_count", Integer.class) + ":" +
                                    row.get("non_null_count", Integer.class) + ":" +
                                    row.get("total_value", Integer.class)))
                    .as(StepVerifier::create)
                    .expectNext("3:2:42")
                    .expectComplete()
                    .verify(Duration.ofSeconds(30));
        } finally {
            dropNullableSchema();
        }
    }

    private static void dropNullableSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_nullable_summary', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_nullable_summary");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_nullable_int_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_nullable_int_table");
    }

    private static void dropSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_echo");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_int_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_int_table");
    }
}
