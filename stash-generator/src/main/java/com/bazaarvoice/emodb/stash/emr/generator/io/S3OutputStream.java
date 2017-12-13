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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class S3OutputStream extends OutputStream {

    private final static Logger _log = LoggerFactory.getLogger(S3OutputStream.class);

    private final AmazonS3 _s3;
    private final String _bucket;
    private final String _key;
    private final byte[] _buffer;

    private int _part = 1;
    private int _pos = 0;
    private String _uploadId;
    private List<PartETag> _partETags;

    public S3OutputStream(AmazonS3 s3, String bucket, String key, byte[] buffer) {
        _s3 = s3;
        _bucket = bucket;
        _key = key;
        _buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
        if (_pos == _buffer.length) {
            uploadBuffer();
        }
        _buffer[_pos++] = (byte) b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = len;

        while (remaining != 0) {
            int available = Math.min(_buffer.length - _pos, remaining);
            if (available != 0) {
                System.arraycopy(b, off, _buffer, _pos, available);
                _pos += available;
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
                    .withPartSize(_pos)
                    .withInputStream(new ByteArrayInputStream(_buffer, 0, _pos)));

            _partETags.add(response.getPartETag());
            _part += 1;
            _pos = 0;
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
            objectMetadata.setContentLength(_pos);

            _s3.putObject(new PutObjectRequest(_bucket, _key, new ByteArrayInputStream(_buffer, 0, _pos), objectMetadata));
        } else {
            // Multipart file
            if (_pos != 0) {
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
}
