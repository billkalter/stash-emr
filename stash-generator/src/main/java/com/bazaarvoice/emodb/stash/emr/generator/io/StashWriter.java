package com.bazaarvoice.emodb.stash.emr.generator.io;

/**
 * Interface for defining write access to the new Stash.
 */
public interface StashWriter {
    StashFileWriter writeStashTableFile(String table, String suffix);

    void updateLatestFile();
}
