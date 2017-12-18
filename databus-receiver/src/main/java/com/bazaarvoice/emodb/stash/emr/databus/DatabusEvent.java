package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;

import java.io.Serializable;
import java.util.UUID;

public class DatabusEvent implements Serializable {

    private final UUID _updateId;
    private final DocumentMetadata _documentMetadata;
    private final String _json;

    public DatabusEvent(UUID updateId, DocumentMetadata documentMetadata, String json) {
        _updateId = updateId;
        _documentMetadata = documentMetadata;
        _json = json;
    }

    public UUID getUpdateId() {
        return _updateId;
    }

    public DocumentMetadata getDocumentMetadata() {
        return _documentMetadata;
    }

    public String getJson() {
        return _json;
    }
}
