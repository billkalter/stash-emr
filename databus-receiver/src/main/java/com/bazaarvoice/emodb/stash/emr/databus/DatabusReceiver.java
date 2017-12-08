package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.http.HttpStatus;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatabusReceiver extends Receiver<Tuple2<DocumentMetadata, String>> {

    private final static Logger _log = LoggerFactory.getLogger(DatabusReceiver.class);
    private final static ObjectMapper _objectMapper = new ObjectMapper();

    private final DatabusDiscovery.Builder _databusDiscoveryBuilder;
    private final String _subscription;
    private final String _condition;
    private final String _apiKey;

    private volatile DatabusDiscovery _databusDiscovery;
    private volatile JerseyClient _client;
    private volatile ScheduledExecutorService _service;
    private volatile ReentrantLock _lock = new ReentrantLock();
    private volatile Condition _receiverStopped = _lock.newCondition();

    public DatabusReceiver(DatabusDiscovery.Builder databusDiscoveryBuilder, String subscription, String condition, String apiKey) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        _databusDiscoveryBuilder = checkNotNull(databusDiscoveryBuilder, "Databus discovery builder is required");
        checkArgument(!Strings.isNullOrEmpty(subscription), "Valid subscription is required");
        _subscription = subscription;
        checkArgument(!Strings.isNullOrEmpty(condition), "Valid condition is required");
        _condition = condition;
        checkArgument(!Strings.isNullOrEmpty(apiKey), "Valid API key is required");
        _apiKey = apiKey;
    }

    @Override
    public void onStart() {
        startDatabus();

        _service = Executors.newScheduledThreadPool(2);

        // Subscribe
        subscribe();

        // Start a thread to resubscribe every 4 hours starting at a random offset in the future
        int fourHoursMs = (int) TimeUnit.HOURS.toMillis(4);
        _service.scheduleAtFixedRate(this::subscribe, new Random().nextInt(fourHoursMs), fourHoursMs, TimeUnit.MILLISECONDS);

        // Start a thread to continuously poll
        _service.submit(this::receiveDatabusEvents);
    }

    @Override
    public void onStop() {
        try {
            if (_lock.tryLock(2, TimeUnit.SECONDS)) {
                _receiverStopped.signalAll();
                _lock.unlock();
            }
        } catch (InterruptedException e) {
            _log.warn("Unexpected interrupt during receiver stop", e);
        } finally {
            stopDatabus();
            _service.shutdown();
            try {
                if (!_service.awaitTermination(10, TimeUnit.SECONDS)) {
                    _log.warn("Service taking unusually long to shut down");
                }
            } catch (InterruptedException e) {
                _log.warn("Thread interrupted waiting for services to shut down");
            } finally {
                _service = null;
            }
        }
    }
    
    private void startDatabus() {
        _databusDiscovery = _databusDiscoveryBuilder.withSubscription(_subscription).build();
        _databusDiscovery.startAsync();
        try {
            _databusDiscovery.awaitRunning(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            _log.error("Databus discovery did not start in a reasonable time");
            throw Throwables.propagate(e);
        }

        _client = JerseyClientBuilder.createClient(new ClientConfig()
                .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofSeconds(5).toMillis())
                .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofSeconds(10).toMillis()));
    }

    private void stopDatabus() {
        _databusDiscovery.stopAsync();
        _databusDiscovery = null;
        _client.close();
        _client = null;
    }


    private void receiveDatabusEvents() {
        try {
            List<String> eventKeys = Lists.newArrayListWithCapacity(50);
            boolean databusEmpty = false;


            while (!isStopped() && !databusEmpty) {

                URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                        .path("bus")
                        .path("1")
                        .path(_subscription)
                        .path("poll")
                        .queryParam("ttl", 30)
                        .queryParam("limit", 50)
                        .build();
                Response response = _client.target(uri).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .get();

                List<Event> events;
                try {
                    if (response.getStatus() != HttpStatus.SC_OK) {
                        throw new PollFailedException(_subscription, response.getStatus(), response.readEntity(String.class));
                    }

                    String databusEmptyHeader = response.getHeaderString("X-BV-Databus-Empty");
                    if (databusEmptyHeader != null) {
                        databusEmpty = Boolean.parseBoolean(databusEmptyHeader);
                    }

                    InputStream entity = response.readEntity(InputStream.class);
                    events = _objectMapper.readValue(entity, new TypeReference<List<Event>>() {});
                } finally {
                    response.close();
                }

                Iterator<Tuple2<DocumentMetadata, String>> documents = Iterators.transform(events.iterator(), event -> {
                    // Lazily build the list of event keys to ack
                    eventKeys.add(event.eventKey);
                    return new Tuple2<>(event.documentMetadata, event.content);

                });

                if (documents.hasNext()) {
                    store(documents);

                    URI ackUri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                            .path("bus")
                            .path("1")
                            .path(_subscription)
                            .path("ack")
                            .build();

                    response = _client.target(ackUri).request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header("X-BV-API-Key", _apiKey)
                            .post(Entity.json(_objectMapper.writeValueAsString(eventKeys)));

                    if (response.getStatus() != HttpStatus.SC_OK) {
                        _log.warn("Events ack failed with response: code={}, entity={}", response.getStatus(), response.readEntity(String.class));
                    }

                    response.close();
                    eventKeys.clear();
                }
            }

            if (!isStopped()) {
                // Pause polling for one second then start again
                _service.schedule(this::receiveDatabusEvents, 1, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            restart("Databus poll failed unexpectedly", e);
        }
    }

    private void subscribe() {
        Response response = null;
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path("bus")
                    .path("1")
                    .path(_subscription)
                    .queryParam("ttl", Duration.ofDays(7).getSeconds())
                    .queryParam("eventTtl", Duration.ofDays(2).getSeconds())
                    .queryParam("includeDefaultJoinFilter", "false")
                    .build();

            response = _client.target(uri).request()
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header("X-BV-API-Key", _apiKey)
                    .put(Entity.entity(_condition, "application/x.json-condition"));

            if (response.getStatus() != HttpStatus.SC_OK) {
                throw new SubscribeFailedException(_subscription, response.getStatus(), response.readEntity(String.class));
            }
        } catch (Exception e) {
            _log.warn("Failed to subscribe to {}", _subscription, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @JsonDeserialize(using = EventDeserializer.class)
    private static class Event {
        String eventKey;
        String content;
        DocumentMetadata documentMetadata;

        public Event(String eventKey, String content) throws IOException {
            this.eventKey = eventKey;
            this.content = content;
            this.documentMetadata = _objectMapper.readValue(content, DocumentMetadata.class);
        }
    }

    private static class EventDeserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(JsonParser parser, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonToken token = parser.getCurrentToken();
            if (token != JsonToken.START_OBJECT) {
                throw ctxt.wrongTokenException(parser, JsonToken.START_OBJECT, "Map expected");
            }

            String eventKey = null;
            String content = null;

            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                assert token == JsonToken.FIELD_NAME;

                String fieldName = parser.getText();
                parser.nextToken();
                
                if ("eventKey".equals(fieldName)) {
                    eventKey = parser.getValueAsString();
                } else if ("content".equals(fieldName)) {
                    content = parser.readValueAs(JsonNode.class).toString();
                } else {
                    parser.skipChildren();
                }
            }

            return new Event(eventKey, content);
        }
    }
}
