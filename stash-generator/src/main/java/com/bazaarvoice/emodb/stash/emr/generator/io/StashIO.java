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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.spark.api.java.Optional;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static com.bazaarvoice.emodb.stash.emr.generator.StashUtil.encodeStashTable;

abstract public class StashIO implements Serializable, StashReader, StashWriter {

    public static StashReader getLatestStash(URI stashRoot, Optional<String> region) {
        if ("s3".equals(stashRoot.getScheme())) {
            return new S3StashIO(stashRoot, null, getRegionFrom(region));
        } else if ("file".equals(stashRoot.getScheme())) {
            return new LocalFileStashIO(stashRoot, null);
        }

        throw new IllegalArgumentException("Unsupported stash root: " + stashRoot);
    }

    public static StashWriter createStash(URI stashRoot, String stashDir, Optional<String> region) {
        if ("s3".equals(stashRoot.getScheme())) {
            return new S3StashIO(stashRoot, stashDir, getRegionFrom(region));
        } else if ("file".equals(stashRoot.getScheme())) {
            return new LocalFileStashIO(stashRoot, stashDir);
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

    abstract public void writeStashTableFile(String table, String suffix, Iterator<String> jsonLines);

    protected Iterator<String> readJsonLines(InputStream inputStream, String fileName) {
        try {
            if (fileName.endsWith(".gz")) {
                inputStream = new GzipCompressorInputStream(inputStream, true);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));

        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                try {
                    while (true) {
                        String line = bufferedReader.readLine();
                        if (line == null) {
                            closeReader();
                            return endOfData();
                        }
                        if (!line.isEmpty()) {
                            return line;
                        }
                    }
                } catch (IOException e) {
                    closeReader();
                    throw Throwables.propagate(e);
                }
            }

            @Override
            protected void finalize() throws Throwable {
                closeReader();
            }

            private void closeReader() {
                try {
                    Closeables.close(bufferedReader, true);
                } catch (IOException ignore) {
                    // Already logged
                }
            }
        };
    }

    protected void writeJsonLines(OutputStream out, Iterator<String> jsonLines) throws IOException {
        try (OutputStream gzipOut = new GzipCompressorOutputStream(new BufferedOutputStream(out));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzipOut))) {
            while (jsonLines.hasNext()) {
                writer.write(jsonLines.next());
                writer.newLine();
            }
        }
    }

    private static class S3StashIO extends StashIO {

        private static final BufferPool BUFFER_POOL = new BufferPool(16, 10 * MB);
        
        private final String _bucket;
        private final String _stashPath;
        private final String _region;

        private transient AmazonS3 _s3;

        S3StashIO(URI uri, @Nullable String stashDir, String region) {
            _bucket = uri.getHost();
            _region = region;
            String rootPath = uri.getPath();
            if (rootPath.startsWith("/")) {
                rootPath = rootPath.substring(1);
            }
            if (rootPath.endsWith("/")) {
                rootPath = rootPath.substring(0, rootPath.length()-1);
            }
            if (stashDir == null) {
                stashDir = getLatest(_bucket, rootPath);
            }
            _stashPath = String.format("%s/%s", rootPath, stashDir);
        }

        private AmazonS3 s3() {
            if (_s3 == null) {
                AmazonS3Client s3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
                s3.setRegion(Region.getRegion(Regions.fromName(_region)));
                _s3 = s3;
            }
            return _s3;
        }

        private String getLatest(String bucket, String rootPath) {
            S3Object s3Object;
            try {
                String latestPath = String.format("%s/_LATEST", rootPath);
                s3Object = s3().getObject(new GetObjectRequest(bucket, latestPath).withRange(0, 2048));
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    throw new IllegalStateException("No previous stash available");
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
        public List<String> getTableFilesFromStash(String table) {
            String tablePath = String.format("%s/%s/", _stashPath, encodeStashTable(table));

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
        public void copyTableFile(StashWriter toStash, String table, String file) {
            if (toStash instanceof S3StashIO) {
                S3StashIO s3Dest = (S3StashIO) toStash;
                String encodedTable = encodeStashTable(table);
                String fromFile = String.format("%s/%s/%s", _stashPath, encodedTable, file);
                String toFile = String.format("%s/%s/%s", s3Dest._stashPath, encodedTable, file);

                s3().copyObject(_bucket, fromFile, s3Dest._bucket, toFile);
            } else {
                throw new UnsupportedOperationException("Copying between different Stash schemes is currently unsupported");
            }
        }

        @Override
        public Iterator<String> readStashTableFile(String table, String file) {
            String encodedTable = encodeStashTable(table);
            String s3File = String.format("%s/%s/%s", _stashPath, encodedTable, file);

            try {
                S3Object s3Object = s3().getObject(_bucket, s3File);
                return readJsonLines(s3Object.getObjectContent(), file);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    return Iterators.emptyIterator();
                }
                throw e;
            }
        }

        @Override
        public void writeStashTableFile(String table, String suffix, Iterator<String> jsonLines) {
            String encodedTable = encodeStashTable(table);
            String s3File = String.format("%s/%s/%s-%s.gz", _stashPath, encodedTable, encodedTable, suffix);

            ByteBuffer buffer = null;
            try {
                buffer = BUFFER_POOL.getBuffer();
                S3OutputStream out = new S3OutputStream(s3(), _bucket, s3File, buffer);
                writeJsonLines(out, jsonLines);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            } finally {
                if (buffer != null) {
                    BUFFER_POOL.returnBuffer(buffer);
                }
            }
        }

        @Override
        public String getStashDirectory() {
            return _stashPath.substring(_stashPath.lastIndexOf('/') + 1);
        }
    }

    private static class LocalFileStashIO extends StashIO {

        private final File _stashDir;

        LocalFileStashIO(URI uri, @Nullable String stashDir) {
            File rootDir = new File(uri.getPath());
            if (stashDir == null) {
                stashDir = getLatest(rootDir);
            }
            _stashDir = new File(rootDir, stashDir);
        }

        private String getLatest(File rootDir) {
            try {
                return Files.readFirstLine(new File(rootDir, "_LATEST"), Charsets.UTF_8);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public List<String> getTableFilesFromStash(String table) {
            File tableDir = new File(_stashDir, encodeStashTable(table));
            if (tableDir.exists() && tableDir.isDirectory()) {
                File[] tableFiles = tableDir.listFiles(File::isFile);
                if (tableFiles != null) {
                    return Arrays.stream(tableFiles).map(File::getName).collect(Collectors.toList());
                }
            }
            return ImmutableList.of();
        }

        @Override
        public void copyTableFile(StashWriter toStash, String table, String file) {
            if (toStash instanceof LocalFileStashIO) {
                LocalFileStashIO localDest = (LocalFileStashIO) toStash;
                String encodedTable = encodeStashTable(table);
                File fromFile = new File(new File(_stashDir, encodedTable), file);
                File toFile = new File(new File(localDest._stashDir, encodedTable), file);

                if (fromFile.exists()) {
                    try {
                        Files.createParentDirs(toFile);
                        Files.copy(fromFile, toFile);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            } else {
                throw new UnsupportedOperationException("Copying between different Stash schemes is currently unsupported");
            }
        }

        @Override
        public Iterator<String> readStashTableFile(String table, String file) {
            String encodedTable = encodeStashTable(table);
            File fromFile = new File(new File(_stashDir, encodedTable), file);

            try {
                return readJsonLines(new FileInputStream(fromFile), file);
            } catch (FileNotFoundException e) {
                return Iterators.emptyIterator();
            }
        }

        @Override
        public void writeStashTableFile(String table, String suffix, Iterator<String> jsonLines) {
            // Base the file suffix on a hash of the first entry's key
            String encodedTable = encodeStashTable(table);
            String fileName = String.format("%s-%s.gz", table, suffix);
            File toFile = new File(new File(_stashDir, encodedTable), fileName);

            try {
                Files.createParentDirs(toFile);
                writeJsonLines(new FileOutputStream(toFile), jsonLines);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String getStashDirectory() {
            return _stashDir.getName();
        }
    }
}
