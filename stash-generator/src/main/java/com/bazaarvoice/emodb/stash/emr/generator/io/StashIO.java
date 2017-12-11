package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;

abstract public class StashIO implements Serializable {

    public static StashIO forStashAt(URI stashRoot) {
        if ("s3".equals(stashRoot.getScheme())) {
            return new S3StashIO(stashRoot);
        } else if ("file".equals(stashRoot.getScheme())) {
            return new LocalFileStashIO(stashRoot);
        }

        throw new IllegalArgumentException("Unsupported stash root: " + stashRoot);
    }

    abstract public String getLatest() throws IOException;

    private static class S3StashIO extends StashIO {
        private final String _bucket;
        private final String _rootPath;

        private volatile AmazonS3 _s3;

        S3StashIO(URI uri) {
            _bucket = uri.getHost();
            String rootPath = uri.getPath();
            if (rootPath.startsWith("/")) {
                rootPath = rootPath.substring(1);
            }
            if (rootPath.endsWith("/")) {
                rootPath = rootPath.substring(0, rootPath.length()-1);
            }
            _rootPath = rootPath;
        }

        private AmazonS3 s3() {
            if (_s3 == null) {
                AmazonS3Client s3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
                s3.setRegion(Region.getRegion(Regions.US_EAST_1));
                _s3 = s3;
            }
            return _s3;
        }

        @Override
        public String getLatest() throws IOException {
            S3Object s3Object;
            try {
                String latestPath = String.format("%s/_LATEST", _rootPath);
                s3Object = s3().getObject(new GetObjectRequest(_bucket, latestPath).withRange(0, 2048));
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    throw new IOException("No previous stash available");
                }
                throw e;
            }

            try (BufferedReader in = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), Charsets.UTF_8))) {
                return in.readLine();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static class LocalFileStashIO extends StashIO {

        private final File _rootDir;

        LocalFileStashIO(URI uri) {
            _rootDir = new File(uri.getPath());
        }

        @Override
        public String getLatest() throws IOException {
            return Files.readFirstLine(new File(_rootDir, "_LATEST"), Charsets.UTF_8);
        }
    }
}
