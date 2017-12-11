package com.bazaarvoice.emodb.stash.emr.generator;

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
