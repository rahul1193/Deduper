package com.deduper.wrapper;

import com.deduper.encoderdecoder.ByteEncoderDecoder;

import java.io.Serializable;

/**
 * authored by @rahulanishetty on 8/7/16.
 */
public class ByteArrayWrapper<T extends Comparable & Serializable> implements Comparable<ByteArrayWrapper<T>> {

    private byte[] bytes;

    private ByteEncoderDecoder<T> encoderDecoder;

    private T constructedObj;

    private ByteArrayWrapper(ByteArrayWrapperBuilder<T> builder) {
        //private constructor
        this.bytes = builder.bytes;
        this.encoderDecoder = builder.encoderDecoder;

    }

    public byte[] getBytes() {
        return bytes;
    }

    public ByteEncoderDecoder<T> getEncoderDecoder() {
        return encoderDecoder;
    }

    private T constructObj() {
        if (constructedObj == null) {
            constructedObj = encoderDecoder.fromByte(bytes);
        }
        return constructedObj;
    }

    @Override
    public int compareTo(ByteArrayWrapper<T> o) {
        return constructObj().compareTo(o.constructObj());
    }

    @Override
    public int hashCode() {
        return constructObj().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ByteArrayWrapper)) {
            return false;
        }
        return constructObj().equals(((ByteArrayWrapper) obj).constructObj());
    }

    public static class ByteArrayWrapperBuilder<T extends Comparable & Serializable> {

        private byte[] bytes;

        private ByteEncoderDecoder<T> encoderDecoder;

        public ByteArrayWrapperBuilder bytes(byte[] bytes) {
            this.bytes = bytes;
            return this;
        }

        public ByteArrayWrapperBuilder encoderDecoder(ByteEncoderDecoder<T> encoderDecoder) {
            this.encoderDecoder = encoderDecoder;
            return this;
        }

        public ByteArrayWrapper<T> build() {
            assert bytes != null;
            assert encoderDecoder != null;
            return new ByteArrayWrapper<T>(this);
        }

    }
}
