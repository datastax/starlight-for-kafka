/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.format;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import lombok.Getter;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * The OutputStream class that uses direct buffer from Netty's buffer allocator as its underlying buffer.
 *
 * The methods that may be called in `MemoryRecordsBuilder` are all overridden.
 */
public class DirectBufferOutputStream extends ByteBufferOutputStream {

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static final ByteBufAllocator ALLOCATOR = PulsarByteBufAllocator.DEFAULT;

    private final int initialCapacity;
    @Getter
    private final ByteBuf byteBuf;
    private final byte[] singleByteWritesBuffer = new byte[64];
    private final int maxPos = singleByteWritesBuffer.length - 1;
    private int singleByteWritesBufferPos = -1;

    private void flushSingleByteWritesBuffer() {
        if (singleByteWritesBufferPos >= 0) {
            byteBuf.writeBytes(singleByteWritesBuffer, 0, singleByteWritesBufferPos + 1);
            singleByteWritesBufferPos = -1;
        }
    }

    public DirectBufferOutputStream(int initialCapacity) {
        super(EMPTY_BUFFER);
        this.initialCapacity = initialCapacity;
        this.byteBuf = ALLOCATOR.directBuffer(initialCapacity);
    }

    @Override
    public void write(int b) {
        // Kafka encoder calls this method very frequently
        // writing single bytes to a Netty ByteBuf is overkilling.
        singleByteWritesBuffer[++singleByteWritesBufferPos] = (byte) b;
        if (singleByteWritesBufferPos == maxPos) {
            flushSingleByteWritesBuffer();
        }
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        flushSingleByteWritesBuffer();
        byteBuf.writeBytes(bytes, off, len);
    }

    @Override
    public void write(ByteBuffer sourceBuffer) {
        flushSingleByteWritesBuffer();
        byteBuf.writeBytes(sourceBuffer);
    }

    @Override
    public ByteBuffer buffer() {
        flushSingleByteWritesBuffer();
        // When this method is called, the internal NIO ByteBuffer should be treated as a buffer that has only been
        // written. In this case, the position should be the same with the limit because the caller side will usually
        // call `ByteBuffer#flip()` to reset position and limit.
        final ByteBuffer byteBuffer = byteBuf.nioBuffer();
        byteBuffer.position(byteBuffer.limit());
        return byteBuffer;
    }

    @Override
    public int position() {
        flushSingleByteWritesBuffer();
        return byteBuf.readerIndex();
    }

    @Override
    public void position(int position) {
        flushSingleByteWritesBuffer();
        if (position > byteBuf.capacity()) {
            byteBuf.capacity(position);
        }
        byteBuf.writerIndex(position);
    }

    @Override
    public int initialCapacity() {
        return initialCapacity;
    }

    @Override
    public void flush() {
        flushSingleByteWritesBuffer();
    }

    @Override
    public int remaining() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int limit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        flush();
    }
}
