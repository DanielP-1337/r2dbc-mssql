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
import io.netty.buffer.Unpooled;
import io.r2dbc.mssql.MssqlTableValue;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.mssql.util.TestByteBufAllocator;
import io.r2dbc.spi.Clob;
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
 * Streaming Clob cells, including surrogate pairs split across source chunks.
 */
class TableValueClobStreamingUnitTests {

    @Test
    void shouldPreserveSplitSurrogatePairAndDistinguishEmptyClobFromNull() {
        AtomicInteger subscriptions = new AtomicInteger();
        Clob clob = Clob.from(Flux.<CharSequence>just("A\u6f22\ud83d", "", "\ude0aZ")
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        MssqlTableValue table = MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.NVARCHAR)
                .rows(Flux.<List<?>>just(Collections.singletonList(clob),
                        Collections.singletonList(Clob.from(Flux.empty())), Collections.singletonList(null))).build();
        Encoded encoded = encode(table);
        try {
            assertThat(subscriptions.get()).isZero();
            assertPayload(encoded, "4100226f3dd80ade5a00", "", null);
            assertThat(subscriptions.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldStreamClobInBufferedRowWithoutReadingAtBindTime() {
        AtomicInteger subscriptions = new AtomicInteger();
        Clob clob = Clob.from(Flux.<CharSequence>just(new StringBuilder("\u00e4"))
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        Encoded encoded = encode(MssqlTableValue.builder("dbo.t")
                .columnMax("value", SqlServerType.NVARCHAR).row(clob).build());
        try {
            assertThat(subscriptions.get()).isZero();
            assertPayload(encoded, "e400");
            assertThat(subscriptions.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldCancelClobSourceWithoutReadingAllChunks() {
        AtomicInteger emitted = new AtomicInteger();
        AtomicLong largestRequest = new AtomicLong();
        AtomicBoolean cancelled = new AtomicBoolean();
        Clob clob = Clob.from(Flux.range(1, 100000).<CharSequence>map(value -> {
            emitted.incrementAndGet();
            return "x";
        }).doOnRequest(n -> largestRequest.accumulateAndGet(n, Math::max))
                .doOnCancel(() -> cancelled.set(true)));
        Encoded encoded = encode(MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.NVARCHAR)
                .rows(Flux.just(Collections.singletonList(clob))).build());
        try {
            StepVerifier.create(stream(encoded).map(buffer -> {
                buffer.release();
                return emitted.get() > 0;
            }).filter(Boolean::booleanValue), 0)
                    .then(() -> assertThat(emitted.get()).isZero()).thenRequest(1)
                    .expectNext(true).thenCancel().verify(Duration.ofSeconds(10));
            assertThat(emitted.get()).isBetween(1, 99999);
            assertThat(largestRequest.get()).isPositive().isLessThan(100000L);
            assertThat(cancelled.get()).isTrue();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldPropagateClobSourceFailure() {
        IllegalStateException failure = new IllegalStateException("Clob source failed");
        Clob clob = Clob.from(Flux.error(failure));
        Encoded encoded = encode(MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.NVARCHAR)
                .rows(Flux.just(Collections.singletonList(clob))).build());
        try {
            stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectErrorMatches(error -> error == failure)
                    .verify(Duration.ofSeconds(10));
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldNotDiscardAfterSuccessfulClobConsumption() {
        TrackingClob clob = new TrackingClob(Flux.<CharSequence>just("x"));
        Encoded encoded = encode(clobTable(clob));
        try {
            stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(10));
            assertThat(clob.subscriptions.get()).isEqualTo(1);
            assertThat(clob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
        assertThat(clob.discards.get()).isZero();
    }

    @Test
    void shouldNotDiscardOrResubscribeAfterClobSourceError() {
        IllegalStateException failure = new IllegalStateException("tracked failure");
        TrackingClob clob = new TrackingClob(Flux.error(failure));
        Encoded encoded = encode(clobTable(clob));
        try {
            stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectErrorMatches(error -> error == failure)
                    .verify(Duration.ofSeconds(10));
            assertThat(clob.subscriptions.get()).isEqualTo(1);
            assertThat(clob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldDiscardAcquiredClobWhenCancelledBeforeItsSourceSubscription() {
        TrackingClob clob = new TrackingClob(Flux.<CharSequence>just("x"));
        Encoded encoded = encode(clobTable(clob));
        try {
            // Cancel after the PLP unknown-length header, before requesting source chunks.
            StepVerifier.create(stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease)
                    .filter("feffffffffffffff"::equals), 0)
                    .thenRequest(1).expectNext("feffffffffffffff").thenCancel()
                    .verify(Duration.ofSeconds(10));
            assertThat(clob.subscriptions.get()).isZero();
            assertThat(clob.discards.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
        assertThat(clob.discards.get()).isEqualTo(1);
    }

    @Test
    void shouldCancelActiveClobWithoutCallingDiscard() {
        TrackingClob clob = new TrackingClob(Flux.<CharSequence>just("x").concatWith(Flux.never()));
        Encoded encoded = encode(clobTable(clob));
        try {
            StepVerifier.create(stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease)
                    .filter("020000007800"::equals), 0)
                    .thenRequest(1).expectNext("020000007800").thenCancel()
                    .verify(Duration.ofSeconds(10));
            assertThat(clob.subscriptions.get()).isEqualTo(1);
            assertThat(clob.cancellations.get()).isEqualTo(1);
            assertThat(clob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldRejectClobInNonMaxCharacterColumn() {
        TrackingClob clob = new TrackingClob(Flux.<CharSequence>just("x"));
        MssqlTableValue table = MssqlTableValue.builder("dbo.t").column("value", SqlServerType.NVARCHAR)
                .rows(Flux.just(Collections.singletonList(clob))).build();
        Encoded encoded = encode(table);
        try {
            stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectError(IllegalArgumentException.class)
                    .verify(Duration.ofSeconds(10));
            assertThat(clob.subscriptions.get()).isZero();
        } finally {
            encoded.dispose();
            reactor.core.publisher.Mono.from(clob.discard()).block(Duration.ofSeconds(10));
        }
    }

    private static MssqlTableValue clobTable(Clob clob) {
        return MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.NVARCHAR)
                .rows(Flux.just(Collections.singletonList(clob))).build();
    }

    private static final class TrackingClob implements Clob {
        final AtomicInteger subscriptions = new AtomicInteger();
        final AtomicInteger discards = new AtomicInteger();
        final AtomicInteger cancellations = new AtomicInteger();
        private final Flux<CharSequence> source;

        TrackingClob(Flux<CharSequence> source) {
            this.source = source;
        }

        @Override
        public org.reactivestreams.Publisher<CharSequence> stream() {
            return this.source.doOnSubscribe(ignored -> this.subscriptions.incrementAndGet())
                    .doOnCancel(() -> this.cancellations.incrementAndGet());
        }

        @Override
        public org.reactivestreams.Publisher<Void> discard() {
            return reactor.core.publisher.Mono.fromRunnable(() -> this.discards.incrementAndGet());
        }
    }

    private static Encoded encode(MssqlTableValue table) {
        return new DefaultCodecs().encode(TestByteBufAllocator.TEST,
                RpcParameterContext.in(new RpcParameterContext.CharacterValueContext(Collation.from(1033, 0), false)), table);
    }

    private static Flux<ByteBuf> stream(Encoded encoded) {
        assertThat(encoded).isInstanceOf(StreamingEncoded.class);
        return ((StreamingEncoded) encoded).stream();
    }

    private static String hexAndRelease(ByteBuf buffer) {
        try {
            return ByteBufUtil.hexDump(buffer);
        } finally {
            buffer.release();
        }
    }

    private static void assertPayload(Encoded encoded, String... expectedCells) {
        stream(encoded).map(TableValueClobStreamingUnitTests::hexAndRelease).collectList()
                .as(StepVerifier::create).assertNext(parts -> {
                    ByteBuf payload = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(String.join("", parts)));
                    try {
                        String metadata = "0003640062006f000174000100000000000100e7ffff09040000000000";
                        assertThat(ByteBufUtil.hexDump(payload.readSlice(metadata.length() / 2))).isEqualTo(metadata);
                        for (String expected : expectedCells) {
                            assertThat(payload.readUnsignedByte()).isEqualTo((short) 1);
                            long length = payload.readLongLE();
                            if (expected == null) {
                                assertThat(length).isEqualTo(-1L);
                            } else {
                                assertThat(length).isEqualTo(-2L);
                                StringBuilder actual = new StringBuilder();
                                int chunkLength;
                                while ((chunkLength = payload.readIntLE()) != 0) {
                                    assertThat(chunkLength).isPositive().isLessThanOrEqualTo(payload.readableBytes());
                                    actual.append(ByteBufUtil.hexDump(payload.readSlice(chunkLength)));
                                }
                                // Compare complete UTF-16LE data independently of PLP chunk boundaries.
                                assertThat(actual.toString()).isEqualTo(expected);
                            }
                        }
                        assertThat(payload.readUnsignedByte()).isEqualTo((short) 0);
                        assertThat(payload.isReadable()).isFalse();
                    } finally {
                        payload.release();
                    }
                }).expectComplete().verify(Duration.ofSeconds(10));
    }
}
