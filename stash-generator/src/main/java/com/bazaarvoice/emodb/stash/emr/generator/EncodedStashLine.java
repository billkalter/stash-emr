package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.ContentEncoder;
import com.bazaarvoice.emodb.stash.emr.ContentEncoding;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;

import java.io.Serializable;

/**
 * POJO for a single line of Stash JSON.  To reduce memory utilization the Spark job will typically put a compressed
 * JSON line in the RDD which will not be decompressed until it is actually being written to a Stash document.
 *
 * Valid values for {@link #_encoding} come from {@link ContentEncoding#getCode()} and {@link #_content} should
 * be the result from passing the original JSON string to {@link ContentEncoder#fromJson(String)} using the encoder
 * from {@link ContentEncoding#getEncoder()}.
 */
public class EncodedStashLine implements Serializable, Comparable<EncodedStashLine> {

    private final int _encoding;
    private final byte[] _content;

    public EncodedStashLine(int encoding, byte[] content) {
        _encoding = encoding;
        _content = content;
    }

    public int getEncoding() {
        return _encoding;
    }

    public byte[] getContent() {
        return _content;
    }

    /**
     * Convenience method to decode the content back to a JSON string.
     */
    @JsonIgnore
    public String getJson() {
        return ContentEncoding.fromCode(_encoding).getEncoder().toJson(_content);
    }

    /**
     * Sorting encoded strings doesn't really make must sense, but the Spark job requires a deterministic sort order.
     * Since we don't care what the actual order is just do a simple byte array comparison of the encoded content.
     */
    @Override
    public int compareTo(EncodedStashLine o) {
        int len = Math.min(_content.length, o._content.length);
        for (int i=0; i < len; i++) {
            if (_content[i] != o._content[i]) {
                return _content[i] < o._content[i] ? -1 : 1;
            }
        }
        return Ints.compare(_content.length, o._content.length);
    }
}
