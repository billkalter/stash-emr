package com.bazaarvoice.emodb.stash.emr;

/**
 * Available encodings for JSON content when written to Parquet.    Parquet itself is stored with Snappy compression,
 * so the advantage to using a compressed format is to reduce the memory footprint required when querying for JSON and
 * building Spark Datasets from them.  The trade-off is that compressed data will require more CPU to write to Stash
 * documents since they will require an additional decompress call for each document.
 */
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
