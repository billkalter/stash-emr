package com.bazaarvoice.emodb.stash.emr.generator.io;

import java.util.Iterator;
import java.util.List;

public interface StashReader {
    List<String> getTableFilesFromStash(String table);

    Iterator<String> readStashTableFile(String table, String file);

    void copyTableFile(StashWriter toStash, String table, String file);

    String getStashDirectory();
}
