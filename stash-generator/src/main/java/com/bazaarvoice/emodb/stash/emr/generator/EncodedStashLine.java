package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.primitives.Ints;

import java.io.Serializable;

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
