package com.bazaarvoice.emodb.stash.emr.generator.io;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class StashFileWriter implements Closeable {

    private final BufferedWriter _writer;

    public StashFileWriter(OutputStream out) throws IOException {
        _writer = new BufferedWriter(new OutputStreamWriter(new GzipCompressorOutputStream(new BufferedOutputStream(out))));
    }

    public void writeJsonLine(String jsonLine) throws IOException {
        _writer.write(jsonLine);
        _writer.newLine();
    }

    @Override
    public void close() throws IOException {
        _writer.close();
    }
}
