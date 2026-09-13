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

    private static MssqlTableValue.Builder integerTable() {
        return MssqlTableValue.builder("dbo.t").column("value", SqlServerType.INTEGER);
    }

    private static void assertWire(MssqlTableValue table, String expectedPayload) {

        Encoded encoded = new DefaultCodecs().encode(
                TestByteBufAllocator.TEST, RpcParameterContext.in(), table);
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
