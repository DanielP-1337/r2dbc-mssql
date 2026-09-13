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
import io.r2dbc.mssql.message.type.TdsDataType;
import reactor.core.publisher.Flux;

/**
 * An encoded parameter whose complete payload is supplied on demand.
 * Unlike PLP cell chunks, these buffers already contain all type-specific framing.
 * No buffers or subscriptions are retained before subscription. The consumer owns
 * emitted buffers and must cancel its subscription when transmission stops.
 */
public abstract class StreamingEncoded extends Encoded {

    protected StreamingEncoded(TdsDataType dataType) {
        super(dataType, () -> {
            throw new IllegalStateException("Streaming parameters require stream() consumption");
        });
    }

    /**
     * @return the deferred parameter payload, excluding the RPC parameter header
     */
    public abstract Flux<ByteBuf> stream();
}
