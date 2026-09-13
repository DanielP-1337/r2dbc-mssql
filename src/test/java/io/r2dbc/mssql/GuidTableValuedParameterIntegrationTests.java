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
 * Integration tests for UNIQUEIDENTIFIER cells in table-valued parameters.
 */
class GuidTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {

        dropSchema();
        SERVER.getJdbcOperations().execute(
                "CREATE TYPE dbo.r2dbc_tvp_guid_table AS TABLE " +
                        "(ordinal INT NOT NULL, value UNIQUEIDENTIFIER NULL)");

        SERVER.getJdbcOperations().execute(
                "CREATE PROCEDURE dbo.r2dbc_tvp_guid_echo " +
                        "@values dbo.r2dbc_tvp_guid_table READONLY AS " +
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
    void shouldRoundTripGuidValuesAndNullWithIntegerColumn() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.r2dbc_tvp_guid_table")
                .column("ordinal", SqlServerType.INTEGER)
                .column("value", SqlServerType.GUID)
                .row(1, java.util.UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"))
                .row(2, java.util.UUID.fromString("fedcba98-7654-3210-fedc-ba9876543210"))
                .row(3, null)
                .row(4, new java.util.UUID(0L, 0L))
                .row(5, java.util.UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"))
                .build();

        connection.createStatement("EXEC dbo.r2dbc_tvp_guid_echo @values")
                .bind("@values", table)
                .execute()
                // Map each row to a non-null string; retain SQL NULL explicitly.
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("ordinal", Integer.class) + ":" +
                                row.get("value", java.util.UUID.class)))
                .as(StepVerifier::create)
                .expectNext(
                        "1:00112233-4455-6677-8899-aabbccddeeff",
                        "2:fedcba98-7654-3210-fedc-ba9876543210",
                        "3:null",
                        "4:00000000-0000-0000-0000-000000000000",
                        "5:ffffffff-ffff-ffff-ffff-ffffffffffff")
                .expectComplete()
                .verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {

        SERVER.getJdbcOperations().execute(
                "IF OBJECT_ID(N'dbo.r2dbc_tvp_guid_echo', N'P') IS NOT NULL " +
                        "DROP PROCEDURE dbo.r2dbc_tvp_guid_echo");

        SERVER.getJdbcOperations().execute(
                "IF TYPE_ID(N'dbo.r2dbc_tvp_guid_table') IS NOT NULL " +
                        "DROP TYPE dbo.r2dbc_tvp_guid_table");
    }
}
