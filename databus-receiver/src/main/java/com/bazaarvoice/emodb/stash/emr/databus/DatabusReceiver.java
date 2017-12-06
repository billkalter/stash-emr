package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.databus.client.DatabusFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.DocumentVersion;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import io.dropwizard.client.HttpClientConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DatabusReceiver extends Receiver<Tuple2<DocumentMetadata, String>> {

    private final static Logger _log = LoggerFactory.getLogger(DatabusReceiver.class);

    private final String _subscription;
    private final String _apiKey;
    private final String _cluster;
    private final String _zooKeeperConnectString;
    private final String _zooKeeperNamespace;
    private final String _emoUrl;

    private volatile ReentrantLock _lock = new ReentrantLock();
    private volatile Condition _receiverStopped = _lock.newCondition();

    public static DatabusReceiver fromHostAndPort(String subscription, String apiKey, String cluster, URI baseUri) {
        return new DatabusReceiver(subscription, apiKey, cluster, null, null, baseUri.toString());
    }

    public static DatabusReceiver fromHostDiscovery(String subscription, String apiKey, String cluster,
                                                    String zooKeeperConnectString, String zooKeeperNamespace) {
        return new DatabusReceiver(subscription, apiKey, cluster, zooKeeperConnectString, zooKeeperNamespace, null);
    }

    private DatabusReceiver(String subscription, String apiKey, String cluster, String zooKeeperConnectString,
                            String zooKeeperNamespace, String emoUrl) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        _subscription = subscription;
        _apiKey = apiKey;
        _cluster = cluster;
        _zooKeeperConnectString = zooKeeperConnectString;
        _zooKeeperNamespace = zooKeeperNamespace;
        _emoUrl = emoUrl;
    }

    @Override
    public void onStart() {
         new Thread(this::receiveDatabusEvents).start();
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
        }
    }

    private void receiveDatabusEvents() {
        Tuple2<Databus, List<Closeable>> t = createDatabusClient();

        try {
            Databus databus = t._1;
            List<String> eventKeys = Lists.newArrayListWithCapacity(50);

            while (!isStopped()) {
                PollResult pollResult = databus.poll(_subscription, Duration.standardSeconds(30), 50);

                Iterator<Tuple2<DocumentMetadata, String>> documents = Iterators.transform(pollResult.getEventIterator(), event -> {
                    // Lazily build the list of event keys to ack
                    eventKeys.add(event.getEventKey());
                    return toDocumentTuple(event.getContent());

                });

                if (documents.hasNext()) {
                    store(documents);
                    databus.acknowledge(_subscription, eventKeys);
                    eventKeys.clear();
                }

                if (!pollResult.hasMoreEvents()) {
                    sleep();
                }
            }
        } catch (Exception e) {
            restart("Databus poll failed unexpectedly", e);
        } finally {
            try {
                for (Closeable closeable : t._2) {
                    Closeables.close(closeable, true);
                }
            } catch (IOException ignore) {
                // Won't happen, exception already caught
            }
        }
    }

    private Tuple2<Databus, List<Closeable>> createDatabusClient() {
        List<Closeable> closeables = Lists.newArrayListWithCapacity(2);

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
            closeables.add(curator);

            servicePoolBuilder = servicePoolBuilder.withHostDiscovery(new ZooKeeperHostDiscovery(curator, databusFactory.getServiceName(), metricRegistry));
        }

        Databus databus = servicePoolBuilder.buildProxy(new ExponentialBackoffRetry(3, 10, 100, TimeUnit.MILLISECONDS));
        closeables.add(0, () -> ServicePoolProxies.close(databus));

        return new Tuple2<>(databus, closeables);
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

    private void sleep() {
        boolean locked = false;
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            if (!isStopped() && _lock.tryLock(1, TimeUnit.SECONDS)) {
                locked = true;
                if (!isStopped()) {
                    long awaitMs = 1000 - stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    if (awaitMs > 0) {
                        _receiverStopped.await(awaitMs, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } catch (InterruptedException e) {
            if (!isStopped()) {
                _log.warn("Sleep unexpectedly interrupted");
            }
        } finally {
            if (locked) {
                _lock.unlock();
            }
        }

    }
}
