package com.bazaarvoice.emodb.stash.emr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DocumentMetadataDeserializerTest {

    @Test
    public void testParser() throws Exception {
        String json = "{\"~deleted\":false,\"~firstUpdateAt\":\"2017-01-30T22:23:41.956Z\",\"~id\":\"004ce43c-56e2-528a-909d-692cee88f640\",\"~lastMutateAt\":\"2017-03-24T16:09:57.721Z\",\"~lastUpdateAt\":\"2017-03-24T16:18:14.550Z\",\"~signature\":\"e55aaecdda4e9a889ab9b9622a3d90ef\",\"~table\":\"review:sample\",\"~version\":5,\"about\":{\"~id\":\"product::data-gen-itz3maln89ks2m0thaillpus4\",\"~table\":\"catalog:testcustomer:\"},\"browserLocale\":\"en_US\",\"cdv-rcvdProductSample\":\"false\",\"cdvOrder\":[\"cdv-rcvdProductSample\"],\"client\":\"reviewValidationCustomer\",\"contentCodes\":[\"REMOD\"],\"contributor\":{\"~id\":\"d55e0b4f-5243-51bd-b9c6-cd4dd07cdc66\",\"~table\":\"contributor:sample\"},\"displayCode\":\"0001email\",\"displayLocale\":\"en_US\",\"featured\":false,\"firstPublishTime\":\"2014-09-18T10:11:31.000Z\",\"language\":\"en\",\"lastPublishTime\":\"2014-09-18T10:58:01.000Z\",\"legacyInternalId\":23518803,\"rating\":5,\"ratingsOnly\":false,\"sendEmailAlertWhenCommented\":false,\"sendEmailAlertWhenPublished\":false,\"status\":\"APPROVED\",\"submissionTime\":\"2014-09-18T10:11:10.000Z\",\"text\":\"sample text\",\"type\":\"review\"}";

        ObjectMapper objectMapper = new ObjectMapper();
        DocumentMetadata documentMetadata = objectMapper.readValue(json, DocumentMetadata.class);

        assertEquals(documentMetadata.getDocumentId().getTable(), "review:sample");
        assertEquals(documentMetadata.getDocumentId().getKey(), "004ce43c-56e2-528a-909d-692cee88f640");
        assertEquals(documentMetadata.getDocumentVersion().getVersion(), 5);
        assertEquals(documentMetadata.getDocumentVersion().getLastUpdateTs(), 1490372294550L);
        assertFalse(documentMetadata.isDeleted());
    }
}
