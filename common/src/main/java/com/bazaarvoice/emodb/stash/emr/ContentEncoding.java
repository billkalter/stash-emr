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
        // Faster to implement a switch on the known codes than to cycle through each value to find the matching code
        switch (code) {
            case 0:
                return TEXT;
            case 1:
                return LZ4;
            default:
                throw new IllegalArgumentException("Unsupported code");
        }
    }

    public int getCode() {
        return _code;
    }

    public ContentEncoder getEncoder() {
        return _encoder;
    }
}
