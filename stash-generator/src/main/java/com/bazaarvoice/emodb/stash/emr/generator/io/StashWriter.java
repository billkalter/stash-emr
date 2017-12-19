package com.bazaarvoice.emodb.stash.emr.generator.io;

public interface StashWriter {
    StashFileWriter writeStashTableFile(String table, String suffix);

    void updateLatestFile();
}
