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
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.tds.TdsFragment;
import io.r2dbc.mssql.message.token.RpcRequest;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Streaming TVP behavior at the RPC encoding boundary.
 */
class TableValueStreamingUnitTests {

    @Test
    void shouldReadRowsOnDemandAndCancelSource() {
        AtomicInteger emitted = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicLong largestRequest = new AtomicLong();
        Flux<List<?>> rows = Flux.range(1, 100000)
                .<List<?>>map(value -> {
                    emitted.incrementAndGet();
                    return Collections.singletonList(value);
                })
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet())
                .doOnRequest(n -> largestRequest.accumulateAndGet(n, Math::max))
                .doOnCancel(() -> cancelled.set(true));

        Encoded encoded = encode(rows);
        try {
            Flux<TdsFragment> fragments = fragments(encoded);
            assertThat(subscriptions.get()).isZero();
            assertThat(emitted.get()).isZero();

            // Ignore metadata-only fragments; cancel after a fragment produced from rows.
            StepVerifier.create(fragments.map(fragment -> {
                fragment.getByteBuf().release();
                return emitted.get() > 0;
            }).filter(Boolean::booleanValue), 0)
                    .then(() -> assertThat(emitted.get()).isZero())
                    .thenRequest(1)
                    .expectNext(true)
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

            assertThat(subscriptions.get()).isEqualTo(1);
            assertThat(emitted.get()).isBetween(1, 99999);
            assertThat(largestRequest.get()).isPositive().isLessThan(100000L);
            assertThat(cancelled.get()).isTrue();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldEncodeStreamedIntegerAndNullRows() {
        AtomicInteger emitted = new AtomicInteger();
        Flux<List<?>> rows = Flux.<List<?>>just(Collections.singletonList(42), Collections.singletonList(null))
                .doOnNext(ignored -> emitted.incrementAndGet());
        Encoded encoded = encode(rows);
        try {
            assertThat(emitted.get()).isZero();
            fragments(encoded).map(TableValueStreamingUnitTests::hexAndRelease)
                    .collectList()
                    .as(StepVerifier::create)
                    .assertNext(parts -> {
                        assertThat(emitted.get()).isEqualTo(2);
                        // TVP_ROW + INTN length/value, TVP_ROW + NULL, TVP_END.
                        assertThat(String.join("", parts)).endsWith("01042a000000010000");
                    })
                    .expectComplete().verify(Duration.ofSeconds(10));
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldPropagateRowSourceFailure() {
        IllegalStateException failure = new IllegalStateException("row source failed");
        Encoded encoded = encode(Flux.error(failure));
        try {
            fragments(encoded).map(TableValueStreamingUnitTests::hexAndRelease)
                    .collectList()
                    .as(StepVerifier::create)
                    .expectErrorMatches(error -> error == failure)
                    .verify(Duration.ofSeconds(10));
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldCancelBeforeDemandWithoutSubscribingToRows() {
        AtomicInteger subscriptions = new AtomicInteger();
        Encoded encoded = encode(Flux.<List<?>>just(Collections.singletonList(42))
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        try {
            StepVerifier.create(fragments(encoded).map(TableValueStreamingUnitTests::hexAndRelease), 0)
                    .thenCancel().verify(Duration.ofSeconds(10));
            assertThat(subscriptions.get()).isZero();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldResubscribeToRowsForRepeatedEncoding() {
        AtomicInteger subscriptions = new AtomicInteger();
        Encoded encoded = encode(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.<List<?>>just(Collections.singletonList(42));
        }));
        try {
            for (int i = 0; i < 2; i++) {
                fragments(encoded).map(TableValueStreamingUnitTests::hexAndRelease).collectList()
                        .as(StepVerifier::create)
                        .assertNext(parts -> assertThat(String.join("", parts)).endsWith("01042a00000000"))
                        .expectComplete().verify(Duration.ofSeconds(10));
            }
            assertThat(subscriptions.get()).isEqualTo(2);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldNotFinishRpcWhenSourceFailsAfterARow() {
        IllegalStateException failure = new IllegalStateException("failure after first row");
        Encoded encoded = encode(Flux.<List<?>>just(Collections.singletonList(42)).concatWith(Flux.error(failure)));
        AtomicBoolean lastFragment = new AtomicBoolean();
        StringBuilder bytes = new StringBuilder();
        try {
            fragments(encoded).doOnNext(fragment -> {
                if (fragment instanceof io.r2dbc.mssql.message.tds.LastTdsFragment) {
                    lastFragment.set(true);
                }
            }).map(TableValueStreamingUnitTests::hexAndRelease)
                    .doOnNext(bytes::append).then().as(StepVerifier::create)
                    .expectErrorMatches(error -> error == failure).verify(Duration.ofSeconds(10));
            assertThat(bytes.toString()).endsWith("01042a000000");
            assertThat(lastFragment.get()).isFalse();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldRejectInvalidStreamedRowsAndCancelSource() {
        for (List<?> row : java.util.Arrays.<List<?>>asList(Collections.emptyList(), Collections.singletonList("wrong type"))) {
            AtomicBoolean cancelled = new AtomicBoolean();
            Encoded encoded = encode(Flux.<List<?>>just(row).concatWith(Flux.never())
                    .doOnCancel(() -> cancelled.set(true)));
            try {
                fragments(encoded).map(TableValueStreamingUnitTests::hexAndRelease).then()
                        .as(StepVerifier::create).expectError(IllegalArgumentException.class)
                        .verify(Duration.ofSeconds(10));
                assertThat(cancelled.get()).isTrue();
            } finally {
                encoded.dispose();
            }
        }
    }

    @Test
    void shouldEncodeScalarParameterAfterStreamingTvp() {
        Encoded encoded = encode(Flux.just(Collections.singletonList(42)));
        try {
            RpcRequest request = RpcRequest.builder().withProcId(RpcRequest.Sp_ExecuteSql)
                    .withTransactionDescriptor(TransactionDescriptor.empty())
                    .withParameter(RpcDirection.IN, encoded).withParameter(RpcDirection.IN, 77).build();
            Flux.from(request.encode(TestByteBufAllocator.TEST, 512))
                    .map(TableValueStreamingUnitTests::hexAndRelease).collectList().as(StepVerifier::create)
                    .assertNext(parts -> assertThat(String.join("", parts))
                            .endsWith("01042a00000000" + "00002604044d000000"))
                    .expectComplete().verify(Duration.ofSeconds(10));
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldEncodeEmptyAndNonEmptyBlobAlongsideStreamingTvp() {
        for (boolean empty : new boolean[]{true, false}) {
            Encoded table = encode(Flux.just(Collections.singletonList(42)));
            io.r2dbc.spi.Blob blob = io.r2dbc.spi.Blob.from(empty ? Flux.empty() :
                    Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1, 2, 3})));
            Encoded binary = BlobCodec.INSTANCE.encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), blob);
            try {
                RpcRequest request = RpcRequest.builder().withProcId(RpcRequest.Sp_ExecuteSql)
                        .withTransactionDescriptor(TransactionDescriptor.empty())
                        .withParameter(RpcDirection.IN, table).withParameter(RpcDirection.IN, binary)
                        .withParameter(RpcDirection.IN, 77).build();
                String plp = "0000a5fffffeffffffffffffff" + (empty ? "" : "03000000010203") + "00000000";
                Flux.from(request.encode(TestByteBufAllocator.TEST, 512))
                        .map(TableValueStreamingUnitTests::hexAndRelease).collectList().as(StepVerifier::create)
                        .assertNext(parts -> assertThat(String.join("", parts))
                                .endsWith("01042a00000000" + plp + "00002604044d000000"))
                        .expectComplete().verify(Duration.ofSeconds(10));
            } finally {
                table.dispose();
                binary.dispose();
            }
        }
    }

    private static Encoded encode(Flux<? extends List<?>> rows) {
        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .column("value", SqlServerType.INTEGER).rows(rows).build();
        return new DefaultCodecs().encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), table);
    }

    private static Flux<TdsFragment> fragments(Encoded encoded) {
        RpcRequest request = RpcRequest.builder()
                .withProcId(RpcRequest.Sp_ExecuteSql)
                .withTransactionDescriptor(TransactionDescriptor.empty())
                .withParameter(RpcDirection.IN, encoded).build();
        return Flux.from(request.encode(TestByteBufAllocator.TEST, 512));
    }

    private static String hexAndRelease(TdsFragment fragment) {
        ByteBuf buffer = fragment.getByteBuf();
        try {
            return ByteBufUtil.hexDump(buffer);
        } finally {
            buffer.release();
        }
    }
}
