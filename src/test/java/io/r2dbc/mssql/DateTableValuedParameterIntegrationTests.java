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
 * Integration tests for DATE cells in table-valued parameters.
 */
class DateTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {

        dropSchema();
        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_date_table AS TABLE " +
                        "(ordinal INT NOT NULL, value DATE NULL)");

        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_date_echo " +
                        "@values dbo.r2dbc_tvp_date_table READONLY AS " +
                        "BEGIN " +
                        "SET NOCOUNT ON; " +
                        "SELECT ordinal, value FROM @values ORDER BY ordinal; " +
                        "END");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldRoundTripDateBoundariesLeapDayAndNullWithIntegerColumn() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_date_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("value", SqlServerType.DATE)
                .row(1, java.time.LocalDate.of(1, 1, 1))
                .row(2, java.time.LocalDate.of(2000, 2, 29))
                .row(3, null)
                .row(4, java.time.LocalDate.of(1970, 1, 1))
                .row(5, java.time.LocalDate.of(9999, 12, 31))
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_date_echo @values")
                .bind("@values", table)
                .execute()
                // Map each row to a non-null string; retain SQL NULL explicitly.
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("ordinal", Integer.class) + ":" +
                                row.get("value", java.time.LocalDate.class)))
                .as(StepVerifier::create)
                .expectNext(
                        "1:0001-01-01",
                        "2:2000-02-29",
                        "3:null",
                        "4:1970-01-01",
                        "5:9999-12-31")
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_date_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_date_echo");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_date_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_date_table");
    }
}
