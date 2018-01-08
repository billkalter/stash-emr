package com.bazaarvoice.emodb.stash.emr;

import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.Arrays;

/**
 * Encoders for storing document JSON in Parquet.
 * @see ContentEncoding
 */
public abstract class ContentEncoder {

    abstract public byte[] fromJson(String json);

    abstract public String toJson(byte[] bytes);

    /**
     * Encoder for simple UTF-8 text.
     */
    static class TextContentEncoder extends ContentEncoder {
        @Override
        public byte[] fromJson(String json) {
            if (json != null) {
                return json.getBytes(Charsets.UTF_8);
            }
            return null;
        }

        @Override
        public String toJson(byte[] bytes) {
            if (bytes != null) {
                return new String(bytes, Charsets.UTF_8);
            }
            return null;
        }
    }

    /**
     * Encoder for LZ4 compressed UTF-8 bytes.  Since decompression is fastest when the compressed size is known a-priori
     * the first 4 bytes of the returned array is an integer containing the original content length.
     */
    static class LZ4ContentEncoder extends ContentEncoder {

        private final LZ4Factory factory = LZ4Factory.fastestInstance();

        @Override
        public byte[] fromJson(String json) {
            if (json != null) {
                byte[] uncompressedBytes = json.getBytes(Charsets.UTF_8);
                LZ4Compressor compressor = factory.fastCompressor();
                int maxCompressedLength = compressor.maxCompressedLength(uncompressedBytes.length);
                byte[] compressedBytes = new byte[maxCompressedLength + Ints.BYTES];
                System.arraycopy(Ints.toByteArray(uncompressedBytes.length), 0, compressedBytes, 0, Ints.BYTES);
                int compressedLength = compressor.compress(uncompressedBytes, 0, uncompressedBytes.length, compressedBytes, Ints.BYTES, maxCompressedLength);
                if (compressedLength != maxCompressedLength) {
                    compressedBytes = Arrays.copyOf(compressedBytes, compressedLength + Ints.BYTES);
                }
                return compressedBytes;
            }
            return null;
        }

        @Override
        public String toJson(byte[] bytes) {
            if (bytes != null) {
                int uncompressedLength = Ints.fromByteArray(bytes);
                byte[] uncompressedBytes = new byte[uncompressedLength];
                factory.fastDecompressor().decompress(bytes, Ints.BYTES, uncompressedBytes, 0, uncompressedLength);
                return new String(uncompressedBytes, Charsets.UTF_8);
            }
            return null;
        }
    }
}
