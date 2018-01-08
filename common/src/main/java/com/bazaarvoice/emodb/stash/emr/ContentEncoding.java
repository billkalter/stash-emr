package com.bazaarvoice.emodb.stash.emr;

public enum ContentEncoding {

    TEXT(0, new ContentEncoder.TextContentEncoder()),
    LZ4(1, new ContentEncoder.LZ4ContentEncoder());

    private final int _code;
    private final ContentEncoder _encoder;

    ContentEncoding(int code, ContentEncoder encoder) {
        _code = code;
        _encoder = encoder;
    }

    public static ContentEncoding fromCode(int code) {
        for (ContentEncoding encoding : ContentEncoding.values()) {
            if (encoding._code == code) {
                return encoding;
            }
        }
        throw new IllegalArgumentException("Unsupported code");
    }

    public int getCode() {
        return _code;
    }

    public ContentEncoder getEncoder() {
        return _encoder;
    }
}
