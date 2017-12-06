package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.databus.client.DatabusFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.DocumentVersion;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import io.dropwizard.client.HttpClientConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DatabusReceiver extends Receiver<Tuple2<DocumentMetadata, String>> {

    private final static Logger _log = LoggerFactory.getLogger(DatabusReceiver.class);

    private final String _subscriptionName;
    private final String _subscriptionConditionString;
    private final String _apiKey;
    private final String _cluster;
    private final String _zooKeeperConnectString;
    private final String _zooKeeperNamespace;
    private final String _emoUrl;

    private volatile Databus _databus;
    private volatile Closer _databusCloser;
    private volatile com.bazaarvoice.emodb.sor.condition.Condition _subscriptionCondition;
    private volatile ScheduledExecutorService _service;
    private volatile ReentrantLock _lock = new ReentrantLock();
    private volatile Condition _receiverStopped = _lock.newCondition();

    public static DatabusReceiver fromHostAndPort(String subscriptionName, String subscriptionCondition, String apiKey, String cluster, URI baseUri) {
        return new DatabusReceiver(subscriptionName, subscriptionCondition, apiKey, cluster, null, null, baseUri.toString());
    }

    public static DatabusReceiver fromHostDiscovery(String subscriptionName,  String subscriptionCondition, String apiKey, String cluster,
                                                    String zooKeeperConnectString, String zooKeeperNamespace) {
        return new DatabusReceiver(subscriptionName, subscriptionCondition, apiKey, cluster, zooKeeperConnectString, zooKeeperNamespace, null);
    }

    private DatabusReceiver(String subscriptionName,  String subscriptionCondition, String apiKey, String cluster,
                            String zooKeeperConnectString, String zooKeeperNamespace, String emoUrl) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        _subscriptionName = subscriptionName;
        _subscriptionConditionString = subscriptionCondition;
        _apiKey = apiKey;
        _cluster = cluster;
        _zooKeeperConnectString = zooKeeperConnectString;
        _zooKeeperNamespace = zooKeeperNamespace;
        _emoUrl = emoUrl;
    }

    @Override
    public void onStart() {
        startDatabus();

        _service = Executors.newScheduledThreadPool(2);

        // Subscribe
        _subscriptionCondition = Conditions.fromString(_subscriptionConditionString);
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
                _databus = null;
                _databusCloser = null;
                _service = null;
            }
        }
    }

    private void receiveDatabusEvents() {
        try {
            List<String> eventKeys = Lists.newArrayListWithCapacity(50);
            boolean pausePolling = false;

            while (!isStopped() && !pausePolling) {
                PollResult pollResult = _databus.poll(_subscriptionName, Duration.standardSeconds(30), 50);

                Iterator<Tuple2<DocumentMetadata, String>> documents = Iterators.transform(pollResult.getEventIterator(), event -> {
                    // Lazily build the list of event keys to ack
                    eventKeys.add(event.getEventKey());
                    return toDocumentTuple(event.getContent());

                });

                if (documents.hasNext()) {
                    store(documents);
                    _databus.acknowledge(_subscriptionName, eventKeys);
                    eventKeys.clear();
                }

                pausePolling = !pollResult.hasMoreEvents();
            }

            if (!isStopped()) {
                // Pause polling for one second then start again
                _service.schedule(this::receiveDatabusEvents, 1, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            restart("Databus poll failed unexpectedly", e);
        }
    }

    private void startDatabus() {
        _databusCloser = Closer.create();

        MetricRegistry metricRegistry = new MetricRegistry();
        HttpClientConfiguration clientConfiguration = new HttpClientConfiguration();
        clientConfiguration.setConnectionTimeout(io.dropwizard.util.Duration.seconds(5));
        clientConfiguration.setTimeout(io.dropwizard.util.Duration.seconds(10));

        ServiceFactory<Databus> databusFactory =
                DatabusClientFactory.forClusterAndHttpConfiguration(_cluster, clientConfiguration, metricRegistry)
                        .usingCredentials(_apiKey);

        ServicePoolBuilder<Databus> servicePoolBuilder = ServicePoolBuilder.create(Databus.class)
                .withServiceFactory(databusFactory);

        if (_emoUrl != null) {
            servicePoolBuilder = servicePoolBuilder.withHostDiscoverySource(new DatabusFixedHostDiscoverySource(_emoUrl));
        } else {
            ZooKeeperConfiguration zkConfig = new ZooKeeperConfiguration();
            zkConfig.setNamespace(_zooKeeperNamespace);
            zkConfig.setConnectString(_zooKeeperConnectString);
            zkConfig.setRetryPolicy(new RetryNTimes(3, 50));
            CuratorFramework curator = zkConfig.newCurator();
            curator.start();
            _databusCloser.register(curator);

            servicePoolBuilder = servicePoolBuilder.withHostDiscovery(new ZooKeeperHostDiscovery(curator, databusFactory.getServiceName(), metricRegistry));
        }

        _databus = servicePoolBuilder.buildProxy(new ExponentialBackoffRetry(3, 10, 100, TimeUnit.MILLISECONDS));
        _databusCloser.register(() -> ServicePoolProxies.close(_databus));
    }

    private void stopDatabus() {
        try {
            _databusCloser.close();
        } catch (IOException e) {
            _log.warn("Failed to close databus connection", e);
        }
    }

    private void subscribe() {
        try {
            _databus.subscribe(_subscriptionName, _subscriptionCondition, Duration.standardDays(1), Duration.standardSeconds(30), false);
        } catch (Exception e) {
            _log.warn("Failed to subscribe to {}", _subscriptionName, e);
        }
    }

    private Tuple2<DocumentMetadata, String> toDocumentTuple(Map<String, Object> map) {
        String table = Intrinsic.getTable(map);
        String key = Intrinsic.getId(map);
        DocumentId documentId = new DocumentId(table, key);

        long version = Intrinsic.getVersion(map);
        long lastUpdateTs = Intrinsic.getLastUpdateAt(map).getTime();
        DocumentVersion documentVersion = new DocumentVersion(version, lastUpdateTs);

        boolean deleted = Intrinsic.isDeleted(map);
        String json = JsonHelper.asJson(map);

        return new Tuple2<>(new DocumentMetadata(documentId, documentVersion, deleted), json);
    }
}
