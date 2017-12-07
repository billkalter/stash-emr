package com.bazaarvoice.emodb.stash.emr.discovery;

import com.google.common.collect.Iterators;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
