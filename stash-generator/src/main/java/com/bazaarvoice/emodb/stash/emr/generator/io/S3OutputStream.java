package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.clearspring.analytics.util.Lists;
import org.apache.spark.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class S3OutputStream extends OutputStream {

    private final static Logger _log = LoggerFactory.getLogger(S3OutputStream.class);

    private final AmazonS3 _s3;
    private final String _bucket;
    private final String _key;
    private final ByteBuffer _buffer;

    private int _part = 1;
    private String _uploadId;
    private List<PartETag> _partETags;

    public S3OutputStream(AmazonS3 s3, String bucket, String key, ByteBuffer buffer) {
        _s3 = s3;
        _bucket = bucket;
        _key = key;
        _buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
        if (_buffer.remaining() == 0) {
            uploadBuffer();
        }
        _buffer.put((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = len;
        int pos = off;

        while (remaining != 0) {
            int available = Math.min(_buffer.remaining(), remaining);
            if (available != 0) {
                _buffer.put(b, pos, available);
                pos += available;
                remaining -= available;
            }

            if (remaining != 0) {
                uploadBuffer();
            }
        }
    }

    private void uploadBuffer() throws IOException {
        if (_part == 1) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("application/text");
            objectMetadata.setContentEncoding("gzip");
            InitiateMultipartUploadResult response = _s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(_bucket, _key, objectMetadata));
            _uploadId = response.getUploadId();
            _partETags = Lists.newArrayList();
        }

        try {
            UploadPartResult response = _s3.uploadPart(new UploadPartRequest()
                    .withBucketName(_bucket)
                    .withKey(_key)
                    .withUploadId(_uploadId)
                    .withPartNumber(_part)
                    .withPartSize(_buffer.position())
                    .withInputStream(asInputStream(_buffer)));

            _partETags.add(response.getPartETag());
            _part += 1;
            _buffer.position(0);
        } catch (AmazonClientException e) {
            throw abortMulitpartUpload(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (_part == 1) {
            // Single part file
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("application/text");
            objectMetadata.setContentEncoding("gzip");
            objectMetadata.setContentLength(_buffer.position());

            _s3.putObject(new PutObjectRequest(_bucket, _key, asInputStream(_buffer), objectMetadata));
        } else {
            // Multipart file
            if (_buffer.position() != 0) {
                uploadBuffer();
            }

            try {
                _s3.completeMultipartUpload(new CompleteMultipartUploadRequest(_bucket, _key, _uploadId, _partETags));
            } catch (AmazonClientException e) {
                throw abortMulitpartUpload(e);
            }
        }
    }

    private IOException abortMulitpartUpload(AmazonClientException e) throws IOException {
        if (_uploadId != null) {
            try {
                _s3.abortMultipartUpload(new AbortMultipartUploadRequest(_bucket, _key, _uploadId));
            } catch (AmazonClientException e2) {
                _log.warn("Failed to abort multipart upload", e2);
            }
        }
        throw new IOException(e);
    }

    private InputStream asInputStream(ByteBuffer buffer) {
        return new ByteBufferInputStream((ByteBuffer) buffer.asReadOnlyBuffer().flip());
    }
}
