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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static com.bazaarvoice.emodb.stash.emr.generator.StashNaming.encodeStashTable;
import static com.bazaarvoice.emodb.stash.emr.json.JsonUtil.parseJson;

/**
 * Implementation for read/write access to Stash.
 */
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

    abstract protected InputStream getFileInputStream(String table, String fileName);

    private Iterator<String> readFileLines(InputStream inputStream, String fileName) {
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
                            return endOfData();
                        }
                        if (!line.isEmpty()) {
                            return line;
                        }
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    private <T> Iterator<Tuple2<Integer, T>> countingIterator(final Iterator<T> iterator) {
        return new AbstractIterator<Tuple2<Integer, T>>() {
            int num = 1;

            @Override
            protected Tuple2<Integer, T> computeNext() {
                if (iterator.hasNext()) {
                    return new Tuple2<>(num++, iterator.next());
                }
                return endOfData();
            }
        };
    }

    @Override
    public CloseableIterator<Tuple2<Integer, String>> readStashTableFileJson(String table, String file) {
        InputStream inputStream = getFileInputStream(table, file);
        return new DelegateCloseableIterator<>(countingIterator(readFileLines(inputStream, file)), inputStream);
    }

    @Override
    public CloseableIterator<Tuple2<Integer, DocumentMetadata>> readStashTableFileMetadata(String table, String file) {
        InputStream inputStream = getFileInputStream(table, file);
        return new DelegateCloseableIterator<>(
                countingIterator(Iterators.transform(readFileLines(inputStream, file), line -> parseJson(line, DocumentMetadata.class))),
                inputStream);
    }

    /**
     * S3 implementation.
     */
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
        protected InputStream getFileInputStream(String table, String file) {
            String encodedTable = encodeStashTable(table);
            String s3File = String.format("%s/%s/%s", _stashPath, encodedTable, file);

            try {
                S3Object s3Object = s3().getObject(_bucket, s3File);
                return s3Object.getObjectContent();
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    return new ByteArrayInputStream(new byte[0]);
                }
                throw e;
            }
        }

        @Override
        public StashFileWriter writeStashTableFile(String table, String suffix) {
            String encodedTable = encodeStashTable(table);
            String s3File = String.format("%s/%s/%s-%s.gz", _stashPath, encodedTable, encodedTable, suffix);

            OutputStream s3out = null;
            try {
                s3out = new S3OutputStream(s3(), _bucket, s3File, BUFFER_POOL);
                return new StashFileWriter(s3out);
            } catch (IOException e) {
                try {
                    Closeables.close(s3out, true);
                } catch (IOException ignore) {
                    // Already managed
                }
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String getStashDirectory() {
            return _stashPath.substring(_stashPath.lastIndexOf('/') + 1);
        }

        @Override
        public void updateLatestFile() {
            String key = String.format("%s/_LATEST", _stashPath.substring(0, _stashPath.lastIndexOf('/')));
            byte[] content = getStashDirectory().getBytes(Charsets.UTF_8);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("application/text");
            objectMetadata.setContentLength(content.length);

            // Add retries for writing the _LATEST file since this typically happens on the driver and therefore is not
            // automatically retried by Spark on failure.
            RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetry(10, 250, TimeUnit.MILLISECONDS);
            AmazonS3 s3 = (AmazonS3) RetryProxy.create(AmazonS3.class, s3(), retryPolicy);
            s3.putObject(new PutObjectRequest(_bucket, key, new ByteArrayInputStream(content), objectMetadata));
        }
    }

    /**
     * Local file system implementation.  Useful for unit testing.
     */
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
        protected InputStream getFileInputStream(String table, String file) {
            String encodedTable = encodeStashTable(table);
            File fromFile = new File(new File(_stashDir, encodedTable), file);

            try {
                return new FileInputStream(fromFile);
            } catch (FileNotFoundException e) {
                return new ByteArrayInputStream(new byte[0]);
            }
        }

        @Override
        public StashFileWriter writeStashTableFile(String table, String suffix) {
            // Base the file suffix on a hash of the first entry's key
            String encodedTable = encodeStashTable(table);
            String fileName = String.format("%s-%s.gz", encodedTable, suffix);
            File toFile = new File(new File(_stashDir, encodedTable), fileName);

            OutputStream fileOut = null;
            try {
                Files.createParentDirs(toFile);
                fileOut = new FileOutputStream(toFile);
                return new StashFileWriter(fileOut);
            } catch (IOException e) {
                try {
                    Closeables.close(fileOut, true);
                } catch (IOException ignore) {
                    // Already managed
                }
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String getStashDirectory() {
            return _stashDir.getName();
        }

        @Override
        public void updateLatestFile() {
            File latestFile = new File(_stashDir.getParentFile(), "_LATEST");
            try (FileOutputStream out = new FileOutputStream(latestFile)) {
                out.write(getStashDirectory().getBytes(Charsets.UTF_8));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
