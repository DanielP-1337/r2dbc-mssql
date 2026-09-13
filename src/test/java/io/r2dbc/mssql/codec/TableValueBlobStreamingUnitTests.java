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
import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Streaming Blob cells inside VARBINARY(MAX) TVP columns.
 */
class TableValueBlobStreamingUnitTests {

    @Test
    void shouldStreamBlobChunksEmptyBlobAndNull() {
        ByteBuffer first = ByteBuffer.wrap(new byte[]{0x55, 0, (byte) 0x80, (byte) 0xff, 0x66});
        first.position(1);
        first.limit(4);
        AtomicInteger subscriptions = new AtomicInteger();
        Blob blob = Blob.from(Flux.just(first, ByteBuffer.allocate(0), ByteBuffer.wrap(new byte[]{1, 0}))
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        Blob empty = Blob.from(Flux.empty());
        MssqlTableValue table = MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.VARBINARY)
                .rows(Flux.<List<?>>just(Collections.singletonList(blob), Collections.singletonList(empty),
                        Collections.singletonList(null))).build();
        Encoded encoded = encode(table);
        try {
            assertThat(subscriptions.get()).isZero();
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).collectList()
                    .as(StepVerifier::create)
                    .assertNext(parts -> assertThat(String.join("", parts)).endsWith(
                            // Unknown PLP length, then non-empty source chunks and a terminator.
                            "01feffffffffffffff030000000080ff02000000010000000000" +
                            // Empty Blob is not NULL. It has an unknown length and only the terminator.
                            "01feffffffffffffff00000000" +
                            "01ffffffffffffffff00"))
                    .expectComplete().verify(Duration.ofSeconds(10));
            assertThat(subscriptions.get()).isEqualTo(1);
            assertThat(first.position()).isEqualTo(1);
            assertThat(first.limit()).isEqualTo(4);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldStreamBlobInBufferedRowWithoutReadingAtBindTime() {
        AtomicInteger subscriptions = new AtomicInteger();
        Blob blob = Blob.from(Flux.just(ByteBuffer.wrap(new byte[]{1, 2}))
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet()));
        MssqlTableValue table = MssqlTableValue.builder("dbo.t")
                .columnMax("value", SqlServerType.VARBINARY).row(blob).build();
        Encoded encoded = encode(table);
        try {
            assertThat(subscriptions.get()).isZero();
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).collectList()
                    .as(StepVerifier::create)
                    .assertNext(parts -> assertThat(String.join("", parts))
                            .endsWith("01feffffffffffffff0200000001020000000000"))
                    .expectComplete().verify(Duration.ofSeconds(10));
            assertThat(subscriptions.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldCancelBlobSourceWithoutReadingAllChunks() {
        AtomicInteger emitted = new AtomicInteger();
        AtomicLong largestRequest = new AtomicLong();
        AtomicBoolean cancelled = new AtomicBoolean();
        Blob blob = Blob.from(Flux.range(1, 100000).map(value -> {
            emitted.incrementAndGet();
            return ByteBuffer.wrap(new byte[]{(byte) (int) value});
        }).doOnRequest(n -> largestRequest.accumulateAndGet(n, Math::max))
                .doOnCancel(() -> cancelled.set(true)));
        Encoded encoded = encode(MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.VARBINARY)
                .rows(Flux.just(Collections.singletonList(blob))).build());
        try {
            StepVerifier.create(fragments(encoded).map(fragment -> {
                fragment.getByteBuf().release();
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
    void shouldPropagateBlobSourceFailure() {
        IllegalStateException failure = new IllegalStateException("Blob source failed");
        Blob blob = Blob.from(Flux.error(failure));
        Encoded encoded = encode(MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.VARBINARY)
                .rows(Flux.just(Collections.singletonList(blob))).build());
        try {
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectErrorMatches(error -> error == failure)
                    .verify(Duration.ofSeconds(10));
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldNotDiscardAfterSuccessfulBlobConsumption() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1})));
        Encoded encoded = encode(blobTable(blob));
        try {
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(10));
            assertThat(blob.subscriptions.get()).isEqualTo(1);
            assertThat(blob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
        assertThat(blob.discards.get()).isZero();
    }

    @Test
    void shouldNotDiscardOrResubscribeAfterBlobSourceError() {
        IllegalStateException failure = new IllegalStateException("tracked failure");
        TrackingBlob blob = new TrackingBlob(Flux.error(failure));
        Encoded encoded = encode(blobTable(blob));
        try {
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectErrorMatches(error -> error == failure)
                    .verify(Duration.ofSeconds(10));
            assertThat(blob.subscriptions.get()).isEqualTo(1);
            assertThat(blob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldDiscardAcquiredBlobWhenCancelledBeforeItsSourceSubscription() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1})));
        Encoded encoded = encode(blobTable(blob));
        try {
            // Cancel after the PLP unknown-length header, before requesting source chunks.
            StepVerifier.create(fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease)
                    .filter("feffffffffffffff"::equals), 0)
                    .thenRequest(1).expectNext("feffffffffffffff").thenCancel()
                    .verify(Duration.ofSeconds(10));
            assertThat(blob.subscriptions.get()).isZero();
            assertThat(blob.discards.get()).isEqualTo(1);
        } finally {
            encoded.dispose();
        }
        assertThat(blob.discards.get()).isEqualTo(1);
    }

    @Test
    void shouldCancelActiveBlobWithoutCallingDiscard() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1})).concatWith(Flux.never()));
        Encoded encoded = encode(blobTable(blob));
        try {
            StepVerifier.create(fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease)
                    .filter("0100000001"::equals), 0)
                    .thenRequest(1).expectNext("0100000001").thenCancel()
                    .verify(Duration.ofSeconds(10));
            assertThat(blob.subscriptions.get()).isEqualTo(1);
            assertThat(blob.cancellations.get()).isEqualTo(1);
            assertThat(blob.discards.get()).isZero();
        } finally {
            encoded.dispose();
        }
    }

    @Test
    void shouldRejectBlobInNonMaxBinaryColumn() {
        TrackingBlob blob = new TrackingBlob(Flux.just(ByteBuffer.wrap(new byte[]{1})));
        MssqlTableValue table = MssqlTableValue.builder("dbo.t").column("value", SqlServerType.VARBINARY)
                .rows(Flux.just(Collections.singletonList(blob))).build();
        Encoded encoded = encode(table);
        try {
            fragments(encoded).map(TableValueBlobStreamingUnitTests::hexAndRelease).then()
                    .as(StepVerifier::create).expectError(IllegalArgumentException.class)
                    .verify(Duration.ofSeconds(10));
            assertThat(blob.subscriptions.get()).isZero();
        } finally {
            encoded.dispose();
            reactor.core.publisher.Mono.from(blob.discard()).block(Duration.ofSeconds(10));
        }
    }

    private static MssqlTableValue blobTable(Blob blob) {
        return MssqlTableValue.builder("dbo.t").columnMax("value", SqlServerType.VARBINARY)
                .rows(Flux.just(Collections.singletonList(blob))).build();
    }

    private static final class TrackingBlob implements Blob {
        final AtomicInteger subscriptions = new AtomicInteger();
        final AtomicInteger discards = new AtomicInteger();
        final AtomicInteger cancellations = new AtomicInteger();
        private final Flux<ByteBuffer> source;

        TrackingBlob(Flux<ByteBuffer> source) {
            this.source = source;
        }

        @Override
        public org.reactivestreams.Publisher<ByteBuffer> stream() {
            return this.source.doOnSubscribe(ignored -> this.subscriptions.incrementAndGet())
                    .doOnCancel(() -> this.cancellations.incrementAndGet());
        }

        @Override
        public org.reactivestreams.Publisher<Void> discard() {
            return reactor.core.publisher.Mono.fromRunnable(() -> this.discards.incrementAndGet());
        }
    }

    private static Encoded encode(MssqlTableValue table) {
        return new DefaultCodecs().encode(TestByteBufAllocator.TEST, RpcParameterContext.in(), table);
    }

    private static Flux<TdsFragment> fragments(Encoded encoded) {
        RpcRequest request = RpcRequest.builder().withProcId(RpcRequest.Sp_ExecuteSql)
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
