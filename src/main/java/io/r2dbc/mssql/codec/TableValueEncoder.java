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
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Initial input TVP encoder for scalar columns, including NULL cells.
 * Uses MS-TDS 2.2.5.5.5 metadata and row tokens.
 * Buffered scalar tables retain their original encoding. Blob and Clob cells stream one chunk at a time.
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
            if (column.isMax() && column.getType() != SqlServerType.NVARCHAR && column.getType() != SqlServerType.VARBINARY) {
                throw new IllegalArgumentException("MAX support accepts only NVARCHAR and VARBINARY columns");
            }
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
        if (table.getRowPublisher() != null || table.getRows().stream().anyMatch(TableValueEncoder::hasLob)) {
            return encodeStream(allocator, table, names, collation);
        }
        boolean[] nullable = new boolean[table.getColumns().size()];
        for (List<Object> row : table.getRows()) {
            validateRow(row, table.getColumns(), nullable);
        }

        ByteBuf buffer = allocator.buffer();
        try {
            writeMetadata(buffer, table, names, collation, nullable);
            for (List<Object> row : table.getRows()) {
                writeRow(buffer, row, table.getColumns());
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

    private static Encoded encodeStream(ByteBufAllocator allocator, MssqlTableValue table, String[] names, Collation collation) {
        return new StreamingEncoded(TdsDataType.TVP) {
            @Override
            public String getFormalType() {
                return "[" + names[0] + "].[" + names[1] + "] READONLY";
            }

            @Override
            public Flux<ByteBuf> stream() {
                return Flux.defer(() -> {
                    // Future rows are unknown. Server-side table constraints remain authoritative.
                    boolean[] nullable = new boolean[table.getColumns().size()];
                    java.util.Arrays.fill(nullable, true);
                    Mono<ByteBuf> metadata = Mono.fromSupplier(() -> allocateAndWrite(allocator,
                            buffer -> writeMetadata(buffer, table, names, collation, nullable)));
                    Flux<? extends List<?>> source = table.getRowPublisher() == null ?
                            Flux.fromIterable(table.getRows()) : Flux.from(table.getRowPublisher());
                    Flux<ByteBuf> rows = source.concatMap(row -> Flux.defer(() -> {
                        validateRow(row, table.getColumns(), nullable);
                        if (!hasLob(row)) {
                            return Mono.fromSupplier(() -> allocateAndWrite(allocator,
                                    buffer -> writeRow(buffer, row, table.getColumns()))).flux();
                        }
                        return Flux.concat(
                                Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeByte(1))),
                                Flux.range(0, row.size()).concatMap(index -> {
                                    Object cell = row.get(index);
                                    if (cell instanceof Blob) {
                                        return streamBlob(allocator, (Blob) cell);
                                    }
                                    if (cell instanceof Clob) {
                                        return streamClob(allocator, (Clob) cell);
                                    }
                                    return Mono.fromSupplier(() -> allocateAndWrite(allocator,
                                            buffer -> writeCell(buffer, cell, table.getColumns().get(index)))).flux();
                                }, 0));
                    }), 0); // No row or cell prefetch.
                    return Flux.concat(metadata, rows,
                            Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeByte(0))));
                });
            }
        };
    }

    private static ByteBuf allocateAndWrite(ByteBufAllocator allocator, java.util.function.Consumer<ByteBuf> writer) {
        ByteBuf buffer = allocator.buffer();
        try {
            writer.accept(buffer);
            return buffer;
        } catch (RuntimeException | Error e) {
            buffer.release();
            throw e;
        }
    }

    private static void validateRow(List<?> row, List<MssqlTableValue.Column> columns, boolean[] nullable) {
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException("TVP row width must match the column count");
        }
        for (int i = 0; i < row.size(); i++) {
            Object cell = row.get(i);
            if (cell == null) {
                nullable[i] = true;
            } else {
                SqlServerType type = columns.get(i).getType();
                if (isDecimal(type)) {
                    normalizeDecimal(cell, columns.get(i));
                }
                if (type == SqlServerType.VARBINARY) {
                    if (cell instanceof Blob && columns.get(i).isMax()) {
                        continue;
                    }
                    if (!(cell instanceof byte[])) {
                        throw new IllegalArgumentException("VARBINARY columns require byte[] or null cells");
                    }
                    if (!columns.get(i).isMax() && ((byte[]) cell).length > 8000) {
                        throw new IllegalArgumentException("Initial VARBINARY support accepts at most 8000 bytes per cell");
                    }
                }
                if (type == SqlServerType.NVARCHAR) {
                    if (cell instanceof Clob && columns.get(i).isMax()) {
                        continue;
                    }
                    if (!(cell instanceof String)) {
                        throw new IllegalArgumentException("NVARCHAR columns require String or null cells");
                    }
                    if (!columns.get(i).isMax() && ((String) cell).length() > 4000) {
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

    private static void writeMetadata(ByteBuf buffer, MssqlTableValue table, String[] names, Collation collation, boolean[] nullable) {
        // RpcEncoding.encodeHeader writes TVPTYPE (0xF3) separately.
        buffer.writeByte(0); // DbName must be empty.
        writeIdentifier(buffer, names[0]);
        writeIdentifier(buffer, names[1]);

        buffer.writeShortLE(table.getColumns().size());
        for (int i = 0; i < table.getColumns().size(); i++) {
            buffer.writeIntLE(0); // UserType.
            // Buffered tables infer nullability; streaming tables allow future NULL cells.
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
                buffer.writeShortLE(table.getColumns().get(i).isMax() ? 0xffff : 8000); // No collation.
            } else if (type == SqlServerType.NVARCHAR) {
                buffer.writeByte(0xe7); // NVARCHARTYPE.
                buffer.writeShortLE(table.getColumns().get(i).isMax() ? 0xffff : 8000);
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
    }

    private static void writeRow(ByteBuf buffer, List<?> row, List<MssqlTableValue.Column> columns) {
        buffer.writeByte(1); // TVP_ROW.
        for (int i = 0; i < row.size(); i++) {
            writeCell(buffer, row.get(i), columns.get(i));
        }
    }

    private static void writeCell(ByteBuf buffer, Object cell, MssqlTableValue.Column column) {
        if (column.getType() == SqlServerType.VARBINARY) {
            if (column.isMax()) {
                writePlpBytes(buffer, (byte[]) cell);
            } else if (cell == null) {
                buffer.writeShortLE(0xffff);
            } else {
                byte[] value = (byte[]) cell;
                buffer.writeShortLE(value.length);
                buffer.writeBytes(value);
            }
        } else if (column.getType() == SqlServerType.NVARCHAR) {
            if (column.isMax()) {
                writePlpBytes(buffer, cell == null ? null : ((String) cell).getBytes(StandardCharsets.UTF_16LE));
            } else if (cell == null) {
                buffer.writeShortLE(0xffff);
            } else {
                String value = (String) cell;
                buffer.writeShortLE(value.length() * 2);
                buffer.writeCharSequence(value, StandardCharsets.UTF_16LE);
            }
        } else if (cell == null) {
            buffer.writeByte(0); // NULL: zero length, no value bytes.
        } else if (isDecimal(column.getType())) {
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
        } else if (column.getType() == SqlServerType.DATE) {
            java.time.LocalDate date = (java.time.LocalDate) cell;
            long days = java.time.temporal.ChronoUnit.DAYS.between(
                    java.time.LocalDate.of(1, 1, 1), date);
            buffer.writeByte(3);
            buffer.writeMediumLE((int) days);
        } else if (column.getType() == SqlServerType.GUID) {
            java.util.UUID uuid = (java.util.UUID) cell;
            long msb = uuid.getMostSignificantBits();
            buffer.writeByte(16);
            // SQL Server GUID order: first 4/2/2 bytes little-endian,
            // followed by the final eight bytes in UUID order.
            buffer.writeIntLE((int) (msb >>> 32));
            buffer.writeShortLE((int) (msb >>> 16));
            buffer.writeShortLE((int) msb);
            buffer.writeLong(uuid.getLeastSignificantBits());
        } else if (column.getType() == SqlServerType.BIT) {
            buffer.writeByte(1);
            buffer.writeByte((Boolean) cell ? 1 : 0);
        } else if (column.getType() == SqlServerType.SMALLINT) {
            buffer.writeByte(2);
            buffer.writeShortLE((Short) cell);
        } else if (column.getType() == SqlServerType.BIGINT) {
            buffer.writeByte(8);
            buffer.writeLongLE((Long) cell);
        } else {
            buffer.writeByte(4);
            buffer.writeIntLE((Integer) cell);
        }
    }


    private static boolean hasLob(List<?> row) {
        return row.stream().anyMatch(cell -> cell instanceof Blob || cell instanceof Clob);
    }

    private static Flux<ByteBuf> streamClob(ByteBufAllocator allocator, Clob clob) {
        return Flux.defer(() -> {
            java.util.concurrent.atomic.AtomicBoolean subscribed = new java.util.concurrent.atomic.AtomicBoolean();
            return Flux.usingWhen(Mono.just(clob), value -> Flux.concat(
                        Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeLongLE(-2L))),
                        Flux.defer(() -> Flux.from(value.stream()))
                                .doOnSubscribe(subscription -> subscribed.set(true))
                                .filter(chunk -> chunk.length() != 0)
                                .concatMap(chunk -> Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> {
                                    int length = chunk.length();
                                    buffer.writeIntLE(Math.multiplyExact(length, 2));
                                    // Preserve UTF-16 code units across PLP chunk boundaries.
                                    // Encoding each chunk as an independent String would replace
                                    // surrogate halves when a source splits an emoji between chunks.
                                    for (int i = 0; i < length; i++) {
                                        buffer.writeShortLE(chunk.charAt(i));
                                    }
                                })), 0),
                        Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeIntLE(0)))),
                    value -> discardUnsubscribedClob(value, subscribed),
                    (value, error) -> discardUnsubscribedClob(value, subscribed),
                    value -> discardUnsubscribedClob(value, subscribed));
        });
    }

    private static Mono<Void> discardUnsubscribedClob(Clob clob, java.util.concurrent.atomic.AtomicBoolean subscribed) {
        // As with Blob, an already subscribed stream owns cleanup and cancellation.
        return Mono.defer(() -> subscribed.get() ? Mono.empty() : Mono.from(clob.discard()));
    }

    private static Flux<ByteBuf> streamBlob(ByteBufAllocator allocator, Blob blob) {
        return Flux.defer(() -> {
            java.util.concurrent.atomic.AtomicBoolean subscribed = new java.util.concurrent.atomic.AtomicBoolean();
            return Flux.usingWhen(Mono.just(blob), value -> Flux.concat(
                        Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeLongLE(-2L))),
                        Flux.defer(() -> Flux.from(value.stream()))
                                .doOnSubscribe(subscription -> subscribed.set(true))
                                .filter(java.nio.ByteBuffer::hasRemaining)
                                .concatMap(chunk -> Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> {
                                    java.nio.ByteBuffer bytes = chunk.duplicate();
                                    buffer.writeIntLE(bytes.remaining());
                                    buffer.writeBytes(bytes);
                                })), 0),
                        Mono.fromSupplier(() -> allocateAndWrite(allocator, buffer -> buffer.writeIntLE(0)))),
                    value -> discardUnsubscribedBlob(value, subscribed),
                    (value, error) -> discardUnsubscribedBlob(value, subscribed),
                    value -> discardUnsubscribedBlob(value, subscribed));
        });
    }

    private static Mono<Void> discardUnsubscribedBlob(Blob blob, java.util.concurrent.atomic.AtomicBoolean subscribed) {
        // SPI discard() is for unconsumed Blobs. Once subscribed, the stream owns
        // cleanup and cancellation; discard() may otherwise subscribe a second time.
        return Mono.defer(() -> subscribed.get() ? Mono.empty() : Mono.from(blob.discard()));
    }

    private static void writePlpBytes(ByteBuf buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.writeLongLE(-1L); // PLP_NULL has no chunk or terminator.
            return;
        }

        // Both MAX types use byte counts. This implementation still buffers the complete TVP.
        buffer.writeLongLE(bytes.length);
        if (bytes.length != 0) {
            buffer.writeIntLE(bytes.length);
            buffer.writeBytes(bytes);
        }
        buffer.writeIntLE(0); // PLP_TERMINATOR, also required for an empty value.
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
