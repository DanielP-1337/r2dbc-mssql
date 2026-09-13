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
 * Integration tests for SMALLINT cells in table-valued parameters.
 */
class SmallintTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {

        dropSchema();
        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_smallint_table AS TABLE " +
                        "(ordinal INT NOT NULL, value SMALLINT NULL)");

        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_smallint_echo " +
                        "@values dbo.r2dbc_tvp_smallint_table READONLY AS " +
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
    void shouldRoundTripSmallintBoundariesAndNullWithIntegerColumn() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_smallint_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("value", SqlServerType.SMALLINT)
                .row(1, Short.MIN_VALUE)
                .row(2, (short) -1)
                .row(3, null)
                .row(4, (short) 0)
                .row(5, Short.MAX_VALUE)
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_smallint_echo @values")
                .bind("@values", table)
                .execute()
                // Map each row to a non-null string; retain SQL NULL explicitly.
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("ordinal", Integer.class) + ":" +
                                row.get("value", Short.class)))
                .as(StepVerifier::create)
                .expectNext(
                        "1:-32768",
                        "2:-1",
                        "3:null",
                        "4:0",
                        "5:32767")
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_smallint_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_smallint_echo");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_smallint_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_smallint_table");
    }
}
