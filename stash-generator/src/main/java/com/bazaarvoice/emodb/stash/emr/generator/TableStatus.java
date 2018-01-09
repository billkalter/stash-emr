package com.bazaarvoice.emodb.stash.emr.generator;

import org.apache.spark.api.java.Optional;

/**
 * The Spark job operates on tables based on a set of attributes for those tables.  This class provides a memory-efficient
 * set of attributes associated with a table.
 */
public final class TableStatus {

    public static final short NA = 0;
    public static final short EXISTS_IN_EMO = 1;
    public static final short CONTAINS_UPDATES = 2;

    public static boolean existsInEmo(short value) {
        return (value & EXISTS_IN_EMO) != 0;
    }

    public static boolean containsUpdates(short value) {
        return (value & CONTAINS_UPDATES) != 0;
    }

    public static short combine(Optional<Short> s0, Optional<Short> s1) {
        return (short) (s0.or(NA) | s1.or(NA));
    }

    public static short combine(Optional<Short> s0, Optional<Short> s1, Optional<Short> s2, Optional<Short> s3) {
        return (short) (s0.or(NA) | s1.or(NA) | s2.or(NA) | s3.or(NA));
    }
}
