package com.bazaarvoice.emodb.stash.emr;

import com.bazaarvoice.emodb.stash.emr.json.DocumentMetadataDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonDeserialize(using = DocumentMetadataDeserializer.class)
public class DocumentMetadata implements Serializable {

    private final DocumentId _documentId;
    private final DocumentVersion _documentVersion;
    private final boolean _deleted;

    @JsonCreator
    public DocumentMetadata(@JsonProperty("documentId") DocumentId documentId,
                            @JsonProperty("documentVersion") DocumentVersion documentVersion,
                            @JsonProperty("deleted") boolean deleted) {
        _documentId = documentId;
        _documentVersion = documentVersion;
        _deleted = deleted;
    }

    public DocumentId getDocumentId() {
        return _documentId;
    }

    public DocumentVersion getDocumentVersion() {
        return _documentVersion;
    }

    public boolean isDeleted() {
        return _deleted;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentMetadata)) {
            return false;
        }

        DocumentMetadata that = (DocumentMetadata) o;

        return Objects.equal(_documentId, that._documentId) && Objects.equal(_documentVersion, that._documentVersion)
                && _deleted == that._deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_documentId, _documentVersion, _deleted);
    }
}
