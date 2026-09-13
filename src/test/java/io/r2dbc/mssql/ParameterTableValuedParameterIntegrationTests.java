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
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TVP input wrappers through statement binding and the SQL Server RPC path.
 */
class ParameterTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        SERVER.getJdbcOperations().execute("CREATE TYPE dbo.r2dbc_tvp_parameter_table AS TABLE " +
                "(id INT NOT NULL, value NVARCHAR(MAX) NULL)");
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldBindNamedBufferedTableWithTypedAndInferredScalars() {
        MssqlTableValue table = tableBuilder().row(1, "\u00e4\u6f22").row(2, null).build();
        connection.createStatement("SELECT id + @offset AS id, value, @marker AS marker FROM @values ORDER BY id")
                .bind("offset", Parameters.in(10))
                .bind("marker", Parameters.in(SqlServerType.INTEGER, 7))
                .bind("values", Parameters.in(table)).execute()
                .concatMap(result -> result.map((row, metadata) -> row.get("id", Integer.class) + ":" +
                        row.get("value", String.class) + ":" + row.get("marker", Integer.class)))
                .as(StepVerifier::create).expectNext("11:\u00e4\u6f22:7", "12:null:7")
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    @Test
    void shouldBindPositionalStreamedClobWithUnicodeOptionDisabled() {
        MssqlConnectionFactory factory = new MssqlConnectionFactoryProvider().create(builder()
                .option(MssqlConnectionFactoryProvider.SEND_STRING_PARAMETERS_AS_UNICODE, false).build());
        MssqlConnection testConnection = factory.create().block(Duration.ofSeconds(30));
        AtomicInteger subscriptions = new AtomicInteger();
        try {
            MssqlTableValue table = tableBuilder().rows(Flux.defer(() -> {
                subscriptions.incrementAndGet();
                Clob clob = Clob.from(Flux.<CharSequence>just("A\ud83d", "\ude0a\u6f22"));
                return Flux.<List<?>>just(Arrays.asList(1, clob));
            })).build();
            MssqlStatement statement = testConnection.createStatement("SELECT value FROM @values")
                    .bind(0, Parameters.in(table));
            assertThat(subscriptions.get()).isZero();
            statement.execute().concatMap(result -> result.map((row, metadata) -> row.get("value", String.class)))
                    .as(StepVerifier::create).expectNext("A\ud83d\ude0a\u6f22")
                    .expectComplete().verify(Duration.ofSeconds(30));
            assertThat(subscriptions.get()).isEqualTo(1);
        } finally {
            testConnection.close().block(Duration.ofSeconds(30));
        }
    }

    @Test
    void shouldBindEmptyTableAlongsideTypedNullScalar() {
        connection.createStatement("SELECT COUNT(*) AS total, @marker AS marker FROM @values")
                .bind("values", Parameters.in(tableBuilder().build()))
                .bind("marker", Parameters.in(SqlServerType.INTEGER)).execute()
                .concatMap(result -> result.map((row, metadata) ->
                        row.get("total", Integer.class) + ":" + row.get("marker", Integer.class)))
                .as(StepVerifier::create).expectNext("0:null")
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    private static MssqlTableValue.Builder tableBuilder() {
        return MssqlTableValue.builder("dbo.r2dbc_tvp_parameter_table")
                .column("id", SqlServerType.INTEGER).columnMax("value", SqlServerType.NVARCHAR);
    }

    private static void dropSchema() {
        SERVER.getJdbcOperations().execute("IF TYPE_ID(N'dbo.r2dbc_tvp_parameter_table') IS NOT NULL " +
                "DROP TYPE dbo.r2dbc_tvp_parameter_table");
    }
}
