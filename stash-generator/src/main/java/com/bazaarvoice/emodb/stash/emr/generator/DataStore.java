package com.bazaarvoice.emodb.stash.emr.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.bazaarvoice.emodb.stash.emr.json.JsonUtil.parseJson;

/**
 * Thin client for accessing EmoDB DataStore.  Only those methods actually used by the Stash generator are implemented.
 */
public class DataStore implements Serializable, Closeable {

    private final static Logger _log = LoggerFactory.getLogger(DataStore.class);
    private final static TypeReference<List<TableEntry>> _tableEntriesType = new TypeReference<List<TableEntry>>() {};
    private final DataStoreDiscovery.Builder _dataStoreDiscoveryBuilder;
    private final String _apiKey;

    private transient volatile DataStoreDiscovery _dataStoreDiscovery;
    private transient volatile JerseyClient _client;

    public DataStore(DataStoreDiscovery.Builder dataStoreDiscoveryBuilder, String apiKey) {
        _dataStoreDiscoveryBuilder = dataStoreDiscoveryBuilder;
        _apiKey = apiKey;
    }

    /**
     * Interface {@link IDataStoreClient} and it's implementation {@link DataStoreClient} are present only to satisfy
     * using Spark's {@link RetryProxy} for retrying low-level client API calls.
     */
    private interface IDataStoreClient {
        List<TableEntry> getTableEntries(@Nullable String from) throws Exception;
    }

    private class DataStoreClient implements IDataStoreClient {
        @Override
        public List<TableEntry> getTableEntries(@Nullable String from) throws Exception {
            Response response = null;
            try {
                UriBuilder uriBuilder = UriBuilder.fromUri(getDataStoreDiscovery().getBaseUri())
                        .path("sor")
                        .path("1")
                        .path("_table")
                        .queryParam("limit", 100);

                if (from != null) {
                    uriBuilder = uriBuilder.queryParam("from", from);
                }

                response = _client.target(uriBuilder.build()).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .get();

                if (response.getStatus() != HttpStatus.SC_OK) {
                    _log.error("Failed to read table names from DataStore: status={}, message={}",
                            response.getStatus(), response.readEntity(String.class));
                    throw new IOException("Failed to read table names");
                }

                return parseJson(response.readEntity(String.class), _tableEntriesType);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
    }

    public Iterator<String> getTableNames() {
        RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetry(15, 100, TimeUnit.MILLISECONDS);
        final IDataStoreClient dataStore = (IDataStoreClient) RetryProxy.create(IDataStoreClient.class, new DataStoreClient(), retryPolicy);

        return new AbstractIterator<String>() {
            private Iterator<String> _batch = Iterators.emptyIterator();
            private String _from = null;

            @Override
            protected String computeNext() {
                if (!_batch.hasNext()) {
                    try {
                        List<TableEntry> tableEntries = dataStore.getTableEntries(_from);
                        if (tableEntries.isEmpty()) {
                            return endOfData();
                        }

                        _batch = tableEntries.stream()
                                .filter(TableEntry::isAvailable)
                                .map(TableEntry::getName)
                                .iterator();

                        _from = tableEntries.get(tableEntries.size() - 1).name;
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }

                return _batch.next();
            }
        };
    }

    private DataStoreDiscovery getDataStoreDiscovery() {
        DataStoreDiscovery dataStoreDiscovery = _dataStoreDiscovery;
        if (dataStoreDiscovery == null) {
            synchronized (this) {
                dataStoreDiscovery = _dataStoreDiscovery;
                if (dataStoreDiscovery == null) {
                    dataStoreDiscovery = _dataStoreDiscoveryBuilder.build();
                    ListenableFuture<Service.State> future = dataStoreDiscovery.start();

                    try {
                        future.get(30, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        _log.error("DataStore discovery did not start in a reasonable time");
                        throw Throwables.propagate(e);
                    } catch (Exception e) {
                        _log.error("DataStore discovery startup failed", e);
                    }

                    _client = JerseyClientBuilder.createClient(new ClientConfig()
                            .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofSeconds(5).toMillis())
                            .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofSeconds(10).toMillis()));

                    _dataStoreDiscovery = dataStoreDiscovery;
                }
            }
        }
        return dataStoreDiscovery;
    }

    @Override
    synchronized public void close() throws IOException {
        if (_dataStoreDiscovery != null) {
            _dataStoreDiscovery.stop();
            _dataStoreDiscovery = null;
        }
        if (_client != null) {
            _client.close();
            _client = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * Simple POJO class for parsing the JSON for an EmoDB table definition.  Only those attributes we actually care
     * about are present.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final static class TableEntry {
        @JsonProperty
        private String name;
        @JsonProperty
        private Availability availability = Availability.UNAVAILABLE;

        public String getName() {
            return name;
        }

        boolean isAvailable() {
            return availability != null && availability.available;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(TableEntry.class)
                    .add("name", name)
                    .add("available", isAvailable())
                    .toString();
        }
    }

    /**
     * The list of tables returned by {@link #getTableNames()} should ignore tables which aren't available locally, such
     * as when generating a Stash in us-east-1 for a table which is only replicated to eu-west-1.   Tables which are not
     * locally available have an availability attribute of <code>null</code>.  Available tables have a struct with details
     * about local storage which frankly we don't are about.  So availability and it's serializer are determined by whether
     * the availability attribute exists and the value is a non-null struct.
     */
    @JsonDeserialize(using = AvailabilityDeserializer.class)
    final static class Availability {
        final static Availability AVAILABLE = new Availability(true);
        final static Availability UNAVAILABLE = new Availability(false);

        public Availability(boolean available) {
            this.available = available;
        }

        boolean available;
    }

    final static class AvailabilityDeserializer extends JsonDeserializer<Availability> {
        @Override
        public Availability deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            Availability availability = Availability.UNAVAILABLE;
            if (p.getCurrentToken().isStructStart()) {
                availability = Availability.AVAILABLE;
                p.skipChildren();
            }
            return availability;
        }
    }
}
