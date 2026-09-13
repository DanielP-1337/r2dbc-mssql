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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * In-memory value for a SQL Server table-valued parameter.
 *
 * <p>This initial model does not yet provide TDS encoding. Column metadata
 * and cell validation will be extended with the corresponding tests.
 */
public final class MssqlTableValue {

    private final String typeName;

    private final List<Column> columns;

    private final List<List<Object>> rows;

    private MssqlTableValue(Builder builder) {
        this.typeName = builder.typeName;
        this.columns = Collections.unmodifiableList(new ArrayList<>(builder.columns));
        this.rows = Collections.unmodifiableList(new ArrayList<>(builder.rows));
    }

    /**
     * Create a builder for a user-defined SQL table type.
     *
     * @param typeName the SQL table type name
     * @return a new builder
     */
    public static Builder builder(String typeName) {
        return new Builder(typeName);
    }

    public String getTypeName() {
        return this.typeName;
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public List<List<Object>> getRows() {
        return this.rows;
    }

    public static final class Column {

        private final String name;

        private final SqlServerType type;

        private final int precision;

        private final int scale;

        private final boolean max;

        private Column(String name, SqlServerType type) {
            this(name, type, 0, 0);
        }

        private Column(String name, SqlServerType type, int precision, int scale) {
            this(name, type, precision, scale, false);
        }

        private Column(String name, SqlServerType type, int precision, int scale, boolean max) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
            this.max = max;
        }

        public String getName() {
            return this.name;
        }

        public SqlServerType getType() {
            return this.type;
        }

        /**
         * @return the declared decimal precision, or zero when unspecified
         */
        public int getPrecision() {
            return this.precision;
        }

        /**
         * @return the declared decimal scale, or zero when unspecified
         */
        public int getScale() {
            return this.scale;
        }

        /**
         * @return whether this column explicitly uses the SQL MAX length
         */
        public boolean isMax() {
            return this.max;
        }
    }

    /**
     * Builder for an in-memory table value.
     */
    public static final class Builder {

        private final String typeName;

        private final List<Column> columns = new ArrayList<>();

        private final List<List<Object>> rows = new ArrayList<>();

        private Builder(String typeName) {
            this.typeName = typeName;
        }

        /**
         * Append a column in table type declaration order.
         *
         * @param name the column name
         * @param type the SQL Server type
         * @return this builder
         */
        public Builder column(String name, SqlServerType type) {
            this.columns.add(new Column(name, type));
            return this;
        }

        /**
         * Append an NVARCHAR(MAX) or VARBINARY(MAX) column.
         *
         * @param name the column name
         * @param type NVARCHAR or VARBINARY
         * @return this builder
         */
        public Builder columnMax(String name, SqlServerType type) {
            this.columns.add(new Column(name, type, 0, 0, true));
            return this;
        }

        /**
         * Append a DECIMAL or NUMERIC column with explicit precision and scale.
         *
         * @param name the column name
         * @param type DECIMAL or NUMERIC
         * @param precision the total number of digits (1 through 38)
         * @param scale the fractional digits (0 through precision)
         * @return this builder
         */
        public Builder column(String name, SqlServerType type, int precision, int scale) {
            this.columns.add(new Column(name, type, precision, scale));
            return this;
        }

        /**
         * Append a row in column declaration order.
         *
         * @param values the cell values
         * @return this builder
         */
        public Builder row(Object... values) {
            this.rows.add(Collections.unmodifiableList(
                    new ArrayList<>(Arrays.asList(values))));
            return this;
        }

        /**
         * Create a snapshot of the table structure and rows.
         *
         * <p>Mutable cell objects are not deep-copied.
         *
         * @return the table value
         */
        public MssqlTableValue build() {
            return new MssqlTableValue(this);
        }
    }
}
