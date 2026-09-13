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
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.message.type.TdsDataType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Input parameter wrappers must preserve TVP encoding and deferred LOB consumption.
 */
class TableValueParameterUnitTests {

    @Test
    void shouldEncodeBufferedTableInsideInferredInputParameter() {
        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.INTEGER).row(42).build();
        Encoded encoded = new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in(), Parameters.in(table));
        try {
            assertThat(encoded.getDataType()).isEqualTo(TdsDataType.TVP);
            assertThat(encoded.getFormalType()).isEqualTo("[dbo].[t] READONLY");
            // Type name, column count, required INTN(4), metadata end, row 42, TVP end.
            assertThat(ByteBufUtil.hexDump(encoded.getValue())).isEqualTo(
                    "0003640062006f0001740001000000000000002604000001042a00000000");
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldPreserveClobLazinessInsideInferredInputParameter() {
        AtomicInteger subscriptions = new AtomicInteger();
        Clob clob = Clob.from(Flux.<CharSequence>just("\u00e4")
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .columnMax("value", SqlServerType.NVARCHAR)
                .rows(Flux.just(Collections.singletonList(clob))).build();
        Encoded encoded = new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in(new RpcParameterContext.CharacterValueContext(Collation.from(1033, 0), false)),
                Parameters.in(table));
        try {
            assertThat(subscriptions.get()).isZero();
            assertThat(encoded).isInstanceOf(StreamingEncoded.class);
            assertThat(encoded.getFormalType()).isEqualTo("[dbo].[t] READONLY");
            ((StreamingEncoded) encoded).stream().map(TableValueParameterUnitTests::hexAndRelease)
                    .collectList().as(StepVerifier::create)
                    .assertNext(parts -> assertThat(String.join("", parts)).isEqualTo(
                            "0003640062006f000174000100000000000100e7ffff09040000000000" +
                            "01feffffffffffffff02000000e4000000000000"))
                    .expectComplete().verify(Duration.ofSeconds(10));
            assertThat(subscriptions.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldRejectInOutTableEvenWithInputContext() {
        assertThatThrownBy(() -> new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in(), Parameters.inOut(integerTable())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("input-only");
    }

    @Test
    void shouldRejectDirectTableWithOutputContext() {
        assertThatThrownBy(() -> new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.out(), integerTable()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("input-only");
    }

    @Test
    void shouldRejectExplicitScalarTypeOnTableParameter() {
        assertThatThrownBy(() -> new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in(), Parameters.in(SqlServerType.INTEGER, integerTable())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TVP type metadata");
    }

    @Test
    void shouldRejectContextTypeOverrideOnDirectTable() {
        assertThatThrownBy(() -> new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in().withServerType(SqlServerType.INTEGER), integerTable()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TVP type metadata");
    }

    private static MssqlTableValue integerTable() {
        return MssqlTableValue.builder("dbo.t").column("value", SqlServerType.INTEGER).row(42).build();
    }

    private static String hexAndRelease(ByteBuf buffer) {
        try {
            return ByteBufUtil.hexDump(buffer);
        } finally {
            buffer.release();
        }
    }
}
