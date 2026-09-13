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
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.mssql.MssqlTableValue;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-level regression tests for integer TVPs, based on MS-TDS 2.2.5.5.5.
 * Expected bytes are literal fixtures, independent of the encoder.
 */
class TableValueEncoderUnitTests {

    // RPC name @p (two UTF-16LE characters), input status, TVPTYPE.
    private static final String RPC_HEADER = "02 40 00 70 00 00 f3 ";

    // Empty database name; schema dbo; type t.
    private static final String TYPE_NAME = "00 03 64 00 62 00 6f 00 01 74 00 ";

    // UserType, flags, INTN, four-byte maximum length, empty column name.
    private static final String REQUIRED_INTEGER = "00 00 00 00 00 00 26 04 00 ";

    private static final String NULLABLE_INTEGER = "00 00 00 00 01 00 26 04 00 ";

    @Test
    void shouldEncodeSingleIntegerWithRpcHeader() {

        MssqlTableValue table = integerTable().row(42).build();

        assertWire(table, TYPE_NAME + "01 00 " + REQUIRED_INTEGER +
                "00 " +                // End metadata.
                "01 04 2a 00 00 00 " + // Row: 42.
                "00");                 // End rows.
    }

    @Test
    void shouldEncodeEmptyTableWithBothTerminators() {

        assertWire(integerTable().build(),
                TYPE_NAME + "01 00 " + REQUIRED_INTEGER + "00 00");
    }

    @Test
    void shouldDistinguishNullFromZeroAndPreserveNextRow() {

        MssqlTableValue table = integerTable()
                .row(0)
                .row((Object) null)
                .row(42)
                .build();

        assertWire(table, TYPE_NAME + "01 00 " + NULLABLE_INTEGER +
                "00 " +
                "01 04 00 00 00 00 " + // Zero has four value bytes.
                "01 00 " +             // NULL has no value bytes.
                "01 04 2a 00 00 00 " +
                "00");
    }

    @Test
    void shouldMarkOnlyColumnContainingNullAsNullable() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("left_value", SqlServerType.INTEGER)
                .column("right_value", SqlServerType.INTEGER)
                .row(7, null)
                .row(8, 9)
                .build();

        assertWire(table, TYPE_NAME + "02 00 " +
                REQUIRED_INTEGER + NULLABLE_INTEGER +
                "00 " +
                "01 04 07 00 00 00 00 " +
                "01 04 08 00 00 00 04 09 00 00 00 " +
                "00");
    }

    @Test
    void shouldEncodeSignedIntegerBoundariesInLittleEndian() {

        MssqlTableValue table = integerTable()
                .row(Integer.MIN_VALUE)
                .row(-1)
                .row(Integer.MAX_VALUE)
                .build();

        assertWire(table, TYPE_NAME + "01 00 " + REQUIRED_INTEGER +
                "00 " +
                "01 04 00 00 00 80 " +
                "01 04 ff ff ff ff " +
                "01 04 ff ff ff 7f " +
                "00");
    }

    @Test
    void shouldEncodeBigintBoundariesAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.BIGINT)
                .row(Long.MIN_VALUE)
                .row(2147483648L)
                .row((Object) null)
                .row(Long.MAX_VALUE)
                .build();

        // Nullable INTN with an eight-byte maximum length.
        // Literal little-endian fixtures catch accidental narrowing to int.
        assertWire(table, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 26 08 00 " +
                "00 " +
                "01 08 00 00 00 00 00 00 00 80 " +
                "01 08 00 00 00 80 00 00 00 00 " +
                "01 00 " +
                "01 08 ff ff ff ff ff ff ff 7f " +
                "00");
    }

    @Test
    void shouldEncodeSmallintBoundariesAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.SMALLINT)
                .row(Short.MIN_VALUE)
                .row((short) -1)
                .row((short) 0)
                .row((Object) null)
                .row(Short.MAX_VALUE)
                .build();

        // Nullable INTN with a two-byte maximum length.
        assertWire(table, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 26 02 00 " +
                "00 " +
                "01 02 00 80 " +
                "01 02 ff ff " +
                "01 02 00 00 " +
                "01 00 " +
                "01 02 ff 7f " +
                "00");
    }

    @Test
    void shouldEncodeBitValuesAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.BIT)
                .row(true)
                .row(false)
                .row((Object) null)
                .build();

        // Nullable BITN (0x68), one-byte maximum length.
        // A false value has length one and value zero; NULL has length zero.
        assertWire(table, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 68 01 00 " +
                "00 " +
                "01 01 01 " +
                "01 01 00 " +
                "01 00 " +
                "00");
    }

    @Test
    void shouldEncodeGuidByteOrderAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.GUID)
                .row(java.util.UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"))
                .row((Object) null)
                .row(new java.util.UUID(0L, 0L))
                .build();

        // GUID (0x24), 16-byte maximum length, nullable column.
        // The first 4/2/2-byte fields are little-endian; the final eight bytes
        // retain their UUID order. An all-zero UUID is distinct from NULL.
        assertWire(table, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 24 10 00 " +
                "00 " +
                "01 10 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff " +
                "01 00 " +
                "01 10 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 " +
                "00");
    }

    @Test
    void shouldEncodeDateBoundariesLeapDayAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.DATE)
                .row(java.time.LocalDate.of(1, 1, 1))
                .row(java.time.LocalDate.of(2000, 2, 29))
                .row((Object) null)
                .row(java.time.LocalDate.of(9999, 12, 31))
                .build();

        // DATENTYPE (0x28) has no maximum-length byte in TYPE_INFO.
        // Values contain a length byte and three little-endian bytes counting
        // days since 0001-01-01. Day zero is distinct from NULL.
        // Independently calculated day counts: 0, 730178 and 3652058.
        assertWire(table, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 28 00 " +
                "00 " +
                "01 03 00 00 00 " +
                "01 03 42 24 0b " +
                "01 00 " +
                "01 03 da b9 37 " +
                "00");
    }

    @Test
    void shouldRejectDateBeforeSqlServerRange() {
        assertDateRejected(java.time.LocalDate.of(0, 12, 31),
                "DATE values must be between 0001-01-01 and 9999-12-31");
    }

    @Test
    void shouldRejectDateAfterSqlServerRange() {
        assertDateRejected(java.time.LocalDate.of(10000, 1, 1),
                "DATE values must be between 0001-01-01 and 9999-12-31");
    }

    @Test
    void shouldRejectStringInDateColumn() {
        assertDateRejected("2000-02-29", "DATE columns require LocalDate or null cells");
    }

    private static void assertDateRejected(Object value, String expectedMessage) {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.DATE)
                .row(value)
                .build();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
            Encoded encoded = new DefaultCodecs().encode(
                    TestByteBufAllocator.TEST, RpcParameterContext.in(), table);
            // Release the buffer even if the expected rejection is missing.
            encoded.dispose();
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    void shouldEncodeNvarcharUnicodeEmptyAndNull() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.NVARCHAR)
                .row("A\u00e4\u6f22\ud83d\ude0a")
                .row("")
                .row((Object) null)
                .build();

        RpcParameterContext context = RpcParameterContext.in(
                new RpcParameterContext.CharacterValueContext(
                        io.r2dbc.mssql.message.type.Collation.from(13632521, 52), false));

        // Explicit NVARCHAR stays Unicode even when sendStringParametersAsUnicode is false.
        // Default capacity: 4000 UTF-16 code units (8000 bytes). The emoji uses two units.
        // Empty strings use a zero USHORT length; NULL uses 0xffff.
        assertWire(table, context, TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 e7 40 1f 09 04 d0 00 34 00 " +
                "00 " +
                "01 0a 00 41 00 e4 00 22 6f 3d d8 0a de " +
                "01 00 00 " +
                "01 ff ff " +
                "00");
    }

    @Test
    void shouldEncodeEmptyNvarcharTableWithDifferentCollation() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.NVARCHAR).build();

        assertWire(table, nvarcharContext(), TYPE_NAME + "01 00 " +
                "00 00 00 00 00 00 e7 40 1f 09 04 00 00 00 00 " +
                "00 00");
    }

    @Test
    void shouldEncodeAllNullNvarcharColumnWithDifferentCollation() {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.NVARCHAR).row((Object) null).build();

        assertWire(table, nvarcharContext(), TYPE_NAME + "01 00 " +
                "00 00 00 00 01 00 e7 40 1f 09 04 00 00 00 00 " +
                "00 01 ff ff 00");
    }

    @Test
    void shouldEncodeNvarcharAt4000Utf16CodeUnits() {

        // 3998 BMP characters plus one surrogate pair: exactly 4000 code units.
        String value = String.join("", java.util.Collections.nCopies(3998, "x")) + "\ud83d\ude0a";
        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.NVARCHAR).row(value).build();

        assertWire(table, nvarcharContext(), TYPE_NAME + "01 00 " +
                "00 00 00 00 00 00 e7 40 1f 09 04 00 00 00 00 " +
                "00 01 40 1f " +
                String.join("", java.util.Collections.nCopies(3998, "78 00 ")) +
                "3d d8 0a de 00");
    }

    @Test
    void shouldRejectNvarcharAbove4000Utf16CodeUnits() {

        String value = String.join("", java.util.Collections.nCopies(3999, "x")) + "\ud83d\ude0a";
        assertNvarcharRejected(value,
                "Initial NVARCHAR support accepts at most 4000 UTF-16 code units per cell");
    }

    @Test
    void shouldRejectIntegerInNvarcharColumn() {
        assertNvarcharRejected(42, "NVARCHAR columns require String or null cells");
    }

    private static RpcParameterContext nvarcharContext() {
        return RpcParameterContext.in(new RpcParameterContext.CharacterValueContext(
                io.r2dbc.mssql.message.type.Collation.from(1033, 0), true));
    }

    private static void assertNvarcharRejected(Object value, String expectedMessage) {

        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.NVARCHAR).row(value).build();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
            Encoded encoded = new DefaultCodecs().encode(TestByteBufAllocator.TEST, nvarcharContext(), table);
            encoded.dispose();
        }).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    }

    private static MssqlTableValue.Builder integerTable() {
        return MssqlTableValue.builder("dbo.t").column("value", SqlServerType.INTEGER);
    }

    private static void assertWire(MssqlTableValue table, String expectedPayload) {

        assertWire(table, RpcParameterContext.in(), expectedPayload);
    }

    private static void assertWire(MssqlTableValue table, RpcParameterContext context, String expectedPayload) {

        Encoded encoded = new DefaultCodecs().encode(
                TestByteBufAllocator.TEST, context, table);
        try {
            assertThat(encoded.getFormalType()).isEqualTo("[dbo].[t] READONLY");
            ByteBuf wire = TestByteBufAllocator.TEST.buffer();
            try {
                RpcEncoding.encodeHeader(wire, "p", RpcDirection.IN, encoded.getDataType());
                wire.writeBytes(encoded.getValue());
                assertThat(ByteBufUtil.hexDump(wire))
                        .isEqualTo((RPC_HEADER + expectedPayload).replace(" ", ""));
            } finally {
                wire.release();
            }
        } finally {
            encoded.dispose();
        }
    }
}
