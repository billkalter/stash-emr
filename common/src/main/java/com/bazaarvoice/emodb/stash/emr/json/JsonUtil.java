package com.bazaarvoice.emodb.stash.emr.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InputStream;

public class JsonUtil {
    
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> T parseJson(String json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(byte[] json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(InputStream json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(String json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(byte[] json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(InputStream json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String toJsonString(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
