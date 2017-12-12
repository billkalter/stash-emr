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
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class DataStore implements Serializable, Closeable {

    private final static Logger _log = LoggerFactory.getLogger(DataStore.class);
    private final static TypeReference<List<TableEntry>> _tableEntriesType = new TypeReference<List<TableEntry>>() {};
    private final DataStoreDiscovery.Builder _dataStoreDiscoveryBuilder;
    private final String _apiKey;

    private volatile DataStoreDiscovery _dataStoreDiscovery;
    private volatile JerseyClient _client;

    public DataStore(DataStoreDiscovery.Builder dataStoreDiscoveryBuilder, String apiKey) {
        _dataStoreDiscoveryBuilder = dataStoreDiscoveryBuilder;
        _apiKey = apiKey;
    }

    public Iterator<String> getTableNames() {
        return new AbstractIterator<String>() {
            private Iterator<String> _batch = Iterators.emptyIterator();
            private String _from = null;

            @Override
            protected String computeNext() {
                if (!_batch.hasNext()) {
                    Response response = null;
                    try {
                        UriBuilder uriBuilder = UriBuilder.fromUri(_dataStoreDiscovery.getBaseUri())
                                .path("sor")
                                .path("1")
                                .path("_table")
                                .queryParam("limit", 100);

                        if (_from != null) {
                            uriBuilder = uriBuilder.queryParam("from", _from);
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

                        List<TableEntry> tableEntries = parseJson(response.readEntity(String.class), _tableEntriesType);

                        if (tableEntries.isEmpty()) {
                            return endOfData();
                        }

                        _batch = tableEntries.stream()
                                .filter(TableEntry::isAvailable)
                                .map(TableEntry::getName)
                                .iterator();

                        _from = tableEntries.get(tableEntries.size() - 1).name;
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    } finally {
                        if (response != null) {
                            response.close();
                        }
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
                    _dataStoreDiscovery = dataStoreDiscovery;
                    _dataStoreDiscovery.startAsync();

                    try {
                        _dataStoreDiscovery.awaitRunning(30, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        _log.error("DataStore discovery did not start in a reasonable time");
                        throw Throwables.propagate(e);
                    }
                    _client = JerseyClientBuilder.createClient(new ClientConfig()
                            .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofSeconds(5).toMillis())
                            .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofSeconds(10).toMillis()));
                }
            }
        }
        return dataStoreDiscovery;
    }

    @Override
    synchronized public void close() throws IOException {
        if (_dataStoreDiscovery != null) {
            _dataStoreDiscovery.stopAsync();
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
