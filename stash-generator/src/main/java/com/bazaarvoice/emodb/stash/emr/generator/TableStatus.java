package com.bazaarvoice.emodb.stash.emr.generator;

/**
 * The Spark job operates on tables based on a set of attributes for those tables.  This class provides a memory-efficient
 * set of attributes associated with a table.
 */
public class TableStatus {

    public static final short NA = 0;
    public static final short EXISTS_IN_EMO = 1;
    public static final short CONTAINS_UPDATES = 2;

    public static boolean existsInEmo(short value) {
        return (value & EXISTS_IN_EMO) != 0;
    }

    public static boolean containsUpdates(short value) {
        return (value & CONTAINS_UPDATES) != 0;
    }
}
