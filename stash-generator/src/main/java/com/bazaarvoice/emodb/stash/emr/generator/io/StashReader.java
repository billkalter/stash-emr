package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public interface StashReader {
    List<String> getTableFilesFromStash(String table);

    Iterator<Tuple2<Integer, String>> readStashTableFileJson(String table, String file);

    Iterator<Tuple2<Integer, DocumentMetadata>> readStashTableFileMetadata(String table, String file);

    void copyTableFile(StashWriter toStash, String table, String file);

    String getStashDirectory();
}
