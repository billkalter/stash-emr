package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import java.util.Map;

/**
 * Simple utility for maintaining Stash naming conventions.
 */
public class StashNaming {

    private static final BiMap<Character, Character> TABLE_CHAR_REPLACEMENTS =
            ImmutableBiMap.of(':', '~');


    public static String encodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS);
    }

    public static String decodeStashTable(String table) {
        return transformStashTable(table, TABLE_CHAR_REPLACEMENTS.inverse());
    }

    private static String transformStashTable(String table, Map<Character, Character> transformCharMap) {
        if (table == null) {
            return null;
        }

        for (Map.Entry<Character, Character> entry : transformCharMap.entrySet()) {
            table = table.replace(entry.getKey(), entry.getValue());
        }

        return table;
    }
}
