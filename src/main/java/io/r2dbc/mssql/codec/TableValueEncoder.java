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

package io.r2dbc.mssql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.mssql.MssqlTableValue;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Initial input TVP encoder for scalar columns, including NULL cells.
 * Uses MS-TDS 2.2.5.5.5 metadata and row tokens.
 * This first implementation buffers the complete value; it is not streaming.
 */
final class TableValueEncoder {

    private TableValueEncoder() {
    }

    static Encoded encode(ByteBufAllocator allocator, RpcParameterContext context, MssqlTableValue table) {

        String typeName = table.getTypeName();
        if (typeName == null || !typeName.matches("[A-Za-z_][A-Za-z0-9_]{0,127}\\.[A-Za-z_][A-Za-z0-9_]{0,127}")) {
            throw new IllegalArgumentException("Initial TVP support requires a simple schema.type name");
        }
        String[] names = typeName.split("\\.", -1);

        if (table.getColumns().isEmpty() || table.getColumns().size() > 1024) {
            throw new IllegalArgumentException("TVP requires between 1 and 1024 columns");
        }
        Collation collation = null;
        for (MssqlTableValue.Column column : table.getColumns()) {
            if (column.getType() != SqlServerType.BIT && column.getType() != SqlServerType.SMALLINT &&
                    column.getType() != SqlServerType.INTEGER && column.getType() != SqlServerType.BIGINT &&
                    column.getType() != SqlServerType.GUID && column.getType() != SqlServerType.DATE &&
                    column.getType() != SqlServerType.NVARCHAR && column.getType() != SqlServerType.VARBINARY &&
                    !isDecimal(column.getType())) {
                throw new IllegalArgumentException("Initial TVP support accepts only BIT, SMALLINT, INTEGER, BIGINT, GUID, DATE, NVARCHAR, VARBINARY, DECIMAL and NUMERIC columns");
            }
            if (isDecimal(column.getType())) {
                if (column.getPrecision() < 1 || column.getPrecision() > 38) {
                    throw new IllegalArgumentException("DECIMAL/NUMERIC precision must be between 1 and 38");
                }
                if (column.getScale() < 0 || column.getScale() > column.getPrecision()) {
                    throw new IllegalArgumentException("DECIMAL/NUMERIC scale must be between 0 and precision");
                }
            } else if (column.getPrecision() != 0 || column.getScale() != 0) {
                throw new IllegalArgumentException("Precision and scale are supported only for DECIMAL/NUMERIC columns");
            }
            if (column.getType() == SqlServerType.NVARCHAR) {
                collation = context.getRequiredValueContext(RpcParameterContext.CharacterValueContext.class).getCollation();
            }
        }
        boolean[] nullable = new boolean[table.getColumns().size()];
        for (List<Object> row : table.getRows()) {
            if (row.size() != table.getColumns().size()) {
                throw new IllegalArgumentException("TVP row width must match the column count");
            }
            for (int i = 0; i < row.size(); i++) {
                Object cell = row.get(i);
                if (cell == null) {
                    nullable[i] = true;
                } else {
                    SqlServerType type = table.getColumns().get(i).getType();
                    if (isDecimal(type)) {
                        normalizeDecimal(cell, table.getColumns().get(i));
                    }
                    if (type == SqlServerType.VARBINARY) {
                        if (!(cell instanceof byte[])) {
                            throw new IllegalArgumentException("VARBINARY columns require byte[] or null cells");
                        }
                        if (((byte[]) cell).length > 8000) {
                            throw new IllegalArgumentException("Initial VARBINARY support accepts at most 8000 bytes per cell");
                        }
                    }
                    if (type == SqlServerType.NVARCHAR) {
                        if (!(cell instanceof String)) {
                            throw new IllegalArgumentException("NVARCHAR columns require String or null cells");
                        }
                        if (((String) cell).length() > 4000) {
                            throw new IllegalArgumentException("Initial NVARCHAR support accepts at most 4000 UTF-16 code units per cell");
                        }
                    }
                    if (type == SqlServerType.DATE) {
                        if (!(cell instanceof java.time.LocalDate)) {
                            throw new IllegalArgumentException("DATE columns require LocalDate or null cells");
                        }
                        int year = ((java.time.LocalDate) cell).getYear();
                        if (year < 1 || year > 9999) {
                            throw new IllegalArgumentException("DATE values must be between 0001-01-01 and 9999-12-31");
                        }
                    }
                    if (type == SqlServerType.GUID && !(cell instanceof java.util.UUID)) {
                        throw new IllegalArgumentException("GUID columns require UUID or null cells");
                    }
                    if (type == SqlServerType.BIT && !(cell instanceof Boolean)) {
                        throw new IllegalArgumentException("BIT columns require Boolean or null cells");
                    }
                    if (type == SqlServerType.SMALLINT && !(cell instanceof Short)) {
                        throw new IllegalArgumentException("SMALLINT columns require Short or null cells");
                    }
                    if (type == SqlServerType.INTEGER && !(cell instanceof Integer)) {
                        throw new IllegalArgumentException("INTEGER columns require Integer or null cells");
                    }
                    if (type == SqlServerType.BIGINT && !(cell instanceof Long)) {
                        throw new IllegalArgumentException("BIGINT columns require Long or null cells");
                    }
                }
            }
        }

        ByteBuf buffer = allocator.buffer();
        try {
            // RpcEncoding.encodeHeader writes TVPTYPE (0xF3) separately.
            buffer.writeByte(0); // DbName must be empty.
            writeIdentifier(buffer, names[0]);
            writeIdentifier(buffer, names[1]);

            buffer.writeShortLE(table.getColumns().size());
            for (int i = 0; i < table.getColumns().size(); i++) {
                buffer.writeIntLE(0); // UserType.
                // Infer wire nullability from this buffered value, per column.
                // SQL Server still enforces the declared table type constraints.
                buffer.writeShortLE(nullable[i] ? 1 : 0); // fNullable.
                SqlServerType type = table.getColumns().get(i).getType();
                if (isDecimal(type)) {
                    MssqlTableValue.Column column = table.getColumns().get(i);
                    buffer.writeByte(type == SqlServerType.DECIMAL ? 0x6a : 0x6c);
                    buffer.writeByte(decimalLength(column.getPrecision()));
                    buffer.writeByte(column.getPrecision());
                    buffer.writeByte(column.getScale());
                } else if (type == SqlServerType.VARBINARY) {
                    buffer.writeByte(0xa5); // BIGVARBINARY.
                    buffer.writeShortLE(8000); // Default capacity in bytes; no collation.
                } else if (type == SqlServerType.NVARCHAR) {
                    buffer.writeByte(0xe7); // NVARCHARTYPE.
                    buffer.writeShortLE(8000); // Default capacity: 4000 UTF-16 code units.
                    collation.encode(buffer);
                } else if (type == SqlServerType.DATE) {
                    buffer.writeByte(0x28); // DATENTYPE has no length in TYPE_INFO.
                } else if (type == SqlServerType.GUID) {
                    buffer.writeByte(0x24); // GUIDTYPE.
                    buffer.writeByte(16);
                } else if (type == SqlServerType.BIT) {
                    buffer.writeByte(0x68); // BITNTYPE.
                    buffer.writeByte(1);
                } else {
                    buffer.writeByte(0x26); // INTNTYPE.
                    buffer.writeByte(type == SqlServerType.BIGINT ? 8 : type == SqlServerType.SMALLINT ? 2 : 4);
                }
                buffer.writeByte(0); // Column name must be empty in a TVP.
            }
            buffer.writeByte(0); // End of optional metadata.

            for (List<Object> row : table.getRows()) {
                buffer.writeByte(1); // TVP_ROW.
                for (int i = 0; i < row.size(); i++) {
                    Object cell = row.get(i);
                    if (table.getColumns().get(i).getType() == SqlServerType.VARBINARY) {
                        if (cell == null) {
                            buffer.writeShortLE(0xffff);
                        } else {
                            byte[] value = (byte[]) cell;
                            buffer.writeShortLE(value.length);
                            buffer.writeBytes(value);
                        }
                    } else if (table.getColumns().get(i).getType() == SqlServerType.NVARCHAR) {
                        if (cell == null) {
                            buffer.writeShortLE(0xffff);
                        } else {
                            String value = (String) cell;
                            buffer.writeShortLE(value.length() * 2);
                            buffer.writeCharSequence(value, StandardCharsets.UTF_16LE);
                        }
                    } else if (cell == null) {
                        buffer.writeByte(0); // NULL: zero length, no value bytes.
                    } else if (isDecimal(table.getColumns().get(i).getType())) {
                        MssqlTableValue.Column column = table.getColumns().get(i);
                        BigDecimal value = normalizeDecimal(cell, column);
                        int length = decimalLength(column.getPrecision());
                        byte[] magnitude = value.unscaledValue().abs().toByteArray();
                        buffer.writeByte(length);
                        buffer.writeByte(value.signum() < 0 ? 0 : 1);
                        // Convert the magnitude to fixed-width unsigned little-endian bytes.
                        for (int j = 0; j < length - 1; j++) {
                            int source = magnitude.length - 1 - j;
                            buffer.writeByte(source >= 0 ? magnitude[source] : 0);
                        }
                    } else if (table.getColumns().get(i).getType() == SqlServerType.DATE) {
                        java.time.LocalDate date = (java.time.LocalDate) cell;
                        long days = java.time.temporal.ChronoUnit.DAYS.between(
                                java.time.LocalDate.of(1, 1, 1), date);
                        buffer.writeByte(3);
                        buffer.writeMediumLE((int) days);
                    } else if (table.getColumns().get(i).getType() == SqlServerType.GUID) {
                        java.util.UUID uuid = (java.util.UUID) cell;
                        long msb = uuid.getMostSignificantBits();
                        buffer.writeByte(16);
                        // SQL Server GUID order: first 4/2/2 bytes little-endian,
                        // followed by the final eight bytes in UUID order.
                        buffer.writeIntLE((int) (msb >>> 32));
                        buffer.writeShortLE((int) (msb >>> 16));
                        buffer.writeShortLE((int) msb);
                        buffer.writeLong(uuid.getLeastSignificantBits());
                    } else if (table.getColumns().get(i).getType() == SqlServerType.BIT) {
                        buffer.writeByte(1);
                        buffer.writeByte((Boolean) cell ? 1 : 0);
                    } else if (table.getColumns().get(i).getType() == SqlServerType.SMALLINT) {
                        buffer.writeByte(2);
                        buffer.writeShortLE((Short) cell);
                    } else if (table.getColumns().get(i).getType() == SqlServerType.BIGINT) {
                        buffer.writeByte(8);
                        buffer.writeLongLE((Long) cell);
                    } else {
                        buffer.writeByte(4);
                        buffer.writeIntLE((Integer) cell);
                    }
                }
            }
            buffer.writeByte(0); // End of rows.

            final String formalType = "[" + names[0] + "].[" + names[1] + "] READONLY";
            return new Encoded(TdsDataType.TVP, new Encoded.DisposableSupplier(buffer)) {
                @Override
                public String getFormalType() {
                    return formalType;
                }
            };
        } catch (RuntimeException | Error e) {
            buffer.release();
            throw e;
        }
    }

    private static boolean isDecimal(SqlServerType type) {
        return type == SqlServerType.DECIMAL || type == SqlServerType.NUMERIC;
    }

    private static int decimalLength(int precision) {
        return precision <= 9 ? 5 : precision <= 19 ? 9 : precision <= 28 ? 13 : 17;
    }

    private static BigDecimal normalizeDecimal(Object cell, MssqlTableValue.Column column) {

        if (!(cell instanceof BigDecimal)) {
            throw new IllegalArgumentException("DECIMAL/NUMERIC columns require BigDecimal or null cells");
        }
        BigDecimal value = (BigDecimal) cell;
        // Reject excessive integer digits before rescaling (including negative Java scales).
        if (value.signum() != 0 && (long) value.precision() - value.scale() >
                column.getPrecision() - column.getScale()) {
            throw new IllegalArgumentException("DECIMAL/NUMERIC value exceeds declared precision");
        }
        BigDecimal normalized;
        try {
            normalized = value.setScale(column.getScale(), RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("DECIMAL/NUMERIC value cannot be represented at declared scale without rounding", e);
        }
        if (normalized.precision() > column.getPrecision()) {
            throw new IllegalArgumentException("DECIMAL/NUMERIC value exceeds declared precision");
        }
        return normalized;
    }

    private static void writeIdentifier(ByteBuf buffer, String value) {
        buffer.writeByte(value.length());
        buffer.writeCharSequence(value, StandardCharsets.UTF_16LE);
    }
}
