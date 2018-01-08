package com.bazaarvoice.emodb.stash.emr;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ContentEncodingTest {

    @DataProvider(name = "encoders")
    public static Object[][] encodersDataProvider() {
        return new Object[][] {
                new Object[] { ContentEncoding.TEXT },
                new Object[] { ContentEncoding.LZ4 }
        };
    }

    @Test(dataProvider = "encoders")
    public void testTestEncoder(ContentEncoding encoding) {
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"text\":\"");
        for (int i=0; i < 1000; i++) {
            jsonBuilder.append("abcdefghijklmnopqrstuvwxyz0123456789");
        }
        jsonBuilder.append("\"}");
        String json = jsonBuilder.toString();
        
        ContentEncoder encoder = encoding.getEncoder();
        byte[] encoded = encoder.fromJson(json);
        String decoded = encoder.toJson(encoded);
        assertEquals(decoded, json);
    }
}
