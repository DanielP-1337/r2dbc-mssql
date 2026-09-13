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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL Server round-trip coverage for decimal TVP metadata and values.
 */
class DecimalTableValuedParameterIntegrationTests extends IntegrationTestSupport {

    private static final int[] PRECISIONS = {1, 9, 10, 19, 20, 28, 29, 38};

    @BeforeEach
    void prepareSchema() {
        dropSchema();
        for (SqlServerType type : new SqlServerType[]{SqlServerType.DECIMAL, SqlServerType.NUMERIC}) {
            String name = type == SqlServerType.DECIMAL ? "decimal" : "numeric";
            StringBuilder columns = new StringBuilder("ordinal INT NOT NULL");
            for (int precision : PRECISIONS) {
                columns.append(", p").append(precision).append(" ").append(name)
                        .append("(").append(precision).append(",0) NULL");
            }
            columns.append(", scaled ").append(name).append("(9,2) NULL, fraction ")
                    .append(name).append("(38,38) NULL");
            SERVER.getJdbcOperations().execute("CREATE TYPE dbo.r2dbc_tvp_" + name +
                    "_table AS TABLE (" + columns + ")");
            SERVER.getJdbcOperations().execute("CREATE PROCEDURE dbo.r2dbc_tvp_" + name +
                    "_echo @values dbo.r2dbc_tvp_" + name + "_table READONLY AS " +
                    "BEGIN SET NOCOUNT ON; SELECT * FROM @values ORDER BY ordinal; END");
        }
    }

    @AfterEach
    void cleanUpSchema() {
        dropSchema();
    }

    @Test
    void shouldRoundTripDecimalBoundariesAndScales() {
        assertBoundaries(SqlServerType.DECIMAL);
    }

    @Test
    void shouldRoundTripNumericBoundariesAndScales() {
        assertBoundaries(SqlServerType.NUMERIC);
    }

    @Test
    void shouldSendEmptyAndAllNullDecimalTables() {
        assertEmptyAndNull(SqlServerType.DECIMAL);
    }

    @Test
    void shouldSendEmptyAndAllNullNumericTables() {
        assertEmptyAndNull(SqlServerType.NUMERIC);
    }

    private static void assertBoundaries(SqlServerType type) {
        MssqlTableValue.Builder table = decimalTable(type);
        List<List<Object>> expected = new ArrayList<>();
        for (int ordinal = 1; ordinal <= 4; ordinal++) {
            List<Object> row = new ArrayList<>();
            row.add(ordinal);
            for (int precision : PRECISIONS) {
                BigDecimal maximum = new BigDecimal(String.join("", Collections.nCopies(precision, "9")));
                row.add(ordinal == 1 ? maximum : ordinal == 2 ? maximum.negate() : ordinal == 3 ? null : BigDecimal.ZERO);
            }
            row.add(ordinal == 1 ? new BigDecimal("1.2300") : ordinal == 2 ?
                    new BigDecimal("-1E+2") : ordinal == 3 ? null : BigDecimal.ZERO);
            row.add(ordinal == 1 ? new BigDecimal("0." + String.join("", Collections.nCopies(38, "9"))) :
                    ordinal == 2 ? new BigDecimal("-1E-38") : ordinal == 3 ? null : BigDecimal.ZERO);
            table.row(row.toArray());
            // Assert returned scale as well as numeric value.
            List<Object> normalized = new ArrayList<>(row);
            if (ordinal != 3) {
                normalized.set(9, ((BigDecimal) row.get(9)).setScale(2));
                normalized.set(10, ((BigDecimal) row.get(10)).setScale(38));
            }
            expected.add(normalized);
        }
        assertRows(type, table.build(), expected);
    }

    private static void assertEmptyAndNull(SqlServerType type) {
        assertRows(type, decimalTable(type).build(), Collections.emptyList());
        Object[] row = new Object[11];
        row[0] = 1;
        assertRows(type, decimalTable(type).row(row).build(), Collections.singletonList(Arrays.asList(row)));
    }

    private static MssqlTableValue.Builder decimalTable(SqlServerType type) {
        String name = type == SqlServerType.DECIMAL ? "decimal" : "numeric";
        MssqlTableValue.Builder table = MssqlTableValue.builder("dbo.r2dbc_tvp_" + name + "_table")
                .column("ordinal", SqlServerType.INTEGER);
        for (int precision : PRECISIONS) {
            table.column("p" + precision, type, precision, 0);
        }
        return table.column("scaled", type, 9, 2).column("fraction", type, 38, 38);
    }

    private static void assertRows(SqlServerType type, MssqlTableValue table, List<List<Object>> expected) {
        String name = type == SqlServerType.DECIMAL ? "decimal" : "numeric";
        connection.createStatement("EXEC dbo.r2dbc_tvp_" + name + "_echo @values")
                .bind("@values", table).execute()
                .concatMap(result -> result.map((row, metadata) -> {
                    List<Object> values = new ArrayList<>();
                    values.add(row.get("ordinal", Integer.class));
                    for (int precision : PRECISIONS) {
                        values.add(row.get("p" + precision, BigDecimal.class));
                    }
                    values.add(row.get("scaled", BigDecimal.class));
                    values.add(row.get("fraction", BigDecimal.class));
                    return values;
                }))
                .collectList()
                .as(StepVerifier::create)
                .assertNext(actual -> assertThat(actual).isEqualTo(expected))
                .expectComplete().verify(Duration.ofSeconds(30));
    }

    private static void dropSchema() {
        for (String name : new String[]{"decimal", "numeric"}) {
            SERVER.getJdbcOperations().execute("IF OBJECT_ID(N'dbo.r2dbc_tvp_" + name +
                    "_echo', N'P') IS NOT NULL DROP PROCEDURE dbo.r2dbc_tvp_" + name + "_echo");
            SERVER.getJdbcOperations().execute("IF TYPE_ID(N'dbo.r2dbc_tvp_" + name +
                    "_table') IS NOT NULL DROP TYPE dbo.r2dbc_tvp_" + name + "_table");
        }
    }
}
