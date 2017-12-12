package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.spark.api.java.Optional;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.bazaarvoice.emodb.stash.emr.generator.StashUtil.encodeStashTable;

abstract public class StashIO implements Serializable {

    public static StashIO forStashAt(URI stashRoot, Optional<String> region) {
        if ("s3".equals(stashRoot.getScheme())) {
            return new S3StashIO(stashRoot, getRegionFrom(region));
        } else if ("file".equals(stashRoot.getScheme())) {
            return new LocalFileStashIO(stashRoot);
        }

        throw new IllegalArgumentException("Unsupported stash root: " + stashRoot);
    }

    /**
     * Normally we'd just use <code>region.or(Regions.getCurrentRegion().getName())</code>, but computing
     * the latter is time consuming in some circumstances, usually the ones where the region is explicitly
     * provided, so this method avoids introspecting the current region unless necessary.
     */
    private static String getRegionFrom(Optional<String> region) {
        return region.isPresent() ? region.get() : Regions.getCurrentRegion().getName();
    }

    abstract public String getLatest() throws IOException;

    abstract public List<String> getTableFilesFromStash(String stashDir, String table);

    abstract public Iterator<String> readStashTableFile(String stashDir, String table, String file);

    abstract public void copyTableFile(String fromStashDir, String toStashDir, String table, String file);

    private static class S3StashIO extends StashIO {
        private final String _bucket;
        private final String _rootPath;
        private final String _region;

        private volatile AmazonS3 _s3;

        S3StashIO(URI uri, String region) {
            _bucket = uri.getHost();
            String rootPath = uri.getPath();
            if (rootPath.startsWith("/")) {
                rootPath = rootPath.substring(1);
            }
            if (rootPath.endsWith("/")) {
                rootPath = rootPath.substring(0, rootPath.length()-1);
            }
            _rootPath = rootPath;
            _region = region;
        }

        private AmazonS3 s3() {
            if (_s3 == null) {
                AmazonS3Client s3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
                s3.setRegion(Region.getRegion(Regions.fromName(_region)));
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

        @Override
        public List<String> getTableFilesFromStash(String stashDir, String table) {
            String tablePath = String.format("%s/%s/%s/", _rootPath, stashDir, encodeStashTable(table));

            List<String> tableFiles = Lists.newArrayListWithCapacity(8);

            String marker = null;

            do {
                ObjectListing response = s3().listObjects(new ListObjectsRequest()
                        .withBucketName(_bucket)
                        .withPrefix(tablePath)
                        .withDelimiter("/")
                        .withMarker(marker)
                        .withMaxKeys(1000));

                for (S3ObjectSummary objectSummary : response.getObjectSummaries()) {
                    tableFiles.add(objectSummary.getKey().substring(tablePath.length()));
                }

                marker = response.getNextMarker();
            } while (marker != null);

            return tableFiles;
        }

        @Override
        public void copyTableFile(String fromStashDir, String toStashDir, String table, String file) {
            String encodedTable = encodeStashTable(table);
            String fromFile = String.format("%s/%s/%s/%s", _rootPath, fromStashDir, encodedTable, file);
            String toFile = String.format("%s/%s/%s/%s", _rootPath, toStashDir, encodedTable, file);

            s3().copyObject(_bucket, fromFile, _bucket, toFile);
        }

        @Override
        public Iterator<String> readStashTableFile(String stashDir, String table, String file) {
            // TODO:  Implement this
            return null;
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

        @Override
        public List<String> getTableFilesFromStash(String stashDir, String table) {
            File stashRootDir = new File(_rootDir, stashDir);
            File tableDir = new File(stashRootDir, encodeStashTable(table));
            if (tableDir.exists() && tableDir.isDirectory()) {
                File[] tableFiles = tableDir.listFiles(File::isFile);
                if (tableFiles != null) {
                    return Arrays.stream(tableFiles).map(File::getName).collect(Collectors.toList());
                }
            }
            return ImmutableList.of();
        }

        @Override
        public void copyTableFile(String fromStashDir, String toStashDir, String table, String file) {
            String encodedTable = encodeStashTable(table);
            File fromRootDir = new File(_rootDir, fromStashDir);
            File fromFile = new File(new File(fromRootDir, encodedTable), file);
            File toRootDir = new File(_rootDir, toStashDir);
            File toFile = new File(new File(toRootDir, encodedTable), file);

            if (fromFile.exists()) {
                try {
                    Files.createParentDirs(toFile);
                    Files.copy(fromFile, toFile);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        @Override
        public Iterator<String> readStashTableFile(String stashDir, String table, String file) {
            // TODO:  Implement this
            return null;
        }
    }
}
