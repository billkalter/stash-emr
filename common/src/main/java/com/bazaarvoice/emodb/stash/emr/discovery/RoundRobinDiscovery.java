package com.bazaarvoice.emodb.stash.emr.discovery;

import com.google.common.collect.Iterators;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple ServiceDiscovery implementation which round-robins requests around all servers in the EmoDB cluster.  This is
 * not as sophisticated as the Ostrich client; it doesn't track bad hosts or perform automatic retries.  However, the
 * Spark services make limited calls directly to Emo and those which do have built-in retries, so favor simplicity
 * over unnecessary robustness.
 */
public class RoundRobinDiscovery extends EmoServiceDiscovery {

    private volatile Iterator<URI> _uris = Iterators.emptyIterator();

    public RoundRobinDiscovery(String zookeeperConnectionString, String zookeeperNamespace, String service, URI directUri) {
        super(zookeeperConnectionString, zookeeperNamespace, service, directUri);
    }

    @Override
    protected void hostsChanged(List<Host> sortedHosts) {
        _uris = Iterators.cycle(sortedHosts.stream().map(host -> host.baseUri).collect(Collectors.toList()));
    }

    @Override
    protected URI getBaseUriFromDiscovery() {
        Iterator<URI> uris = _uris;
        if (uris.hasNext()) {
            return uris.next();
        }
        return null;
    }
}
