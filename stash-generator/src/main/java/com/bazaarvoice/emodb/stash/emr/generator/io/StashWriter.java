package com.bazaarvoice.emodb.stash.emr.generator.io;

import java.util.Iterator;

public interface StashWriter {
    void writeStashTableFile(String table, String suffix, Iterator<String> jsonLines);
}
