package com.bazaarvoice.emodb.stash.emr.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class EmoServiceDiscovery extends AbstractService implements Serializable {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String _zookeeperConnectionString;
    private final String _zookeeperNamespace;
    private final String _service;
    private final URI _directUri;

    private volatile CuratorFramework _rootCurator;
    private volatile CuratorFramework _curator;
    private volatile PathChildrenCache _pathCache;

    protected EmoServiceDiscovery(String zookeeperConnectionString, String zookeeperNamespace, String service,
                                  URI directUri) {
        _zookeeperConnectionString = zookeeperConnectionString;
        _zookeeperNamespace = zookeeperNamespace;
        _service = service;
        _directUri = directUri;
    }

    @Override
    protected void doStart() {
        if (_zookeeperConnectionString != null) {
            try {
                _rootCurator = CuratorPool.getStartedCurator(_zookeeperConnectionString);
                _curator = _rootCurator.usingNamespace(_zookeeperNamespace);

                startNodeListener();
            } catch (Exception e) {
                doStop();
                throw Throwables.propagate(e);
            }
        }

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            if (_pathCache != null) {
                Closeables.close(_pathCache, true);
                _pathCache = null;
            }
            if (_rootCurator != null) {
                Closeables.close(_rootCurator, true);
                _rootCurator = _curator = null;
            }
        } catch (IOException ignore) {
            // Already managed
        }

        notifyStopped();
    }

    private void startNodeListener() throws Exception {
        String path = ZKPaths.makePath( "/ostrich", _service);

        _pathCache = new PathChildrenCache(_curator, path, true);
        _pathCache.getListenable().addListener((curator, event) -> rebuildHosts());
        _pathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        rebuildHosts();
    }

    public URI getBaseUri() throws UnknownHostException {
        URI uri = getBaseUriFromDiscovery();
        if (uri == null) {
            if (_directUri == null) {
                throw new UnknownHostException("No hosts discovered");
            }
            uri = _directUri;
        }
        return uri;
    }

    private void rebuildHosts() throws IOException {
        List<ChildData> currentData = _pathCache.getCurrentData();
        List<Host> hosts = Lists.newArrayListWithCapacity(currentData.size());
        for (ChildData childData : currentData) {
            RegistrationData registrationData = OBJECT_MAPPER.readValue(childData.getData(), RegistrationData.class);
            PayloadData payloadData = OBJECT_MAPPER.readValue(registrationData.payload, PayloadData.class);
            URI baseUri = UriBuilder.fromUri(payloadData.serviceUrl).replacePath(null).build();
            hosts.add(new Host(registrationData.id, baseUri));
        }
        Collections.sort(hosts);
        hostsChanged(hosts);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class RegistrationData {
        public String id;
        public String payload;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PayloadData {
        public String serviceUrl;
    }

    protected static class Host implements Comparable<Host> {
        public String id;
        public URI baseUri;

        public Host(String id, URI baseUri) {
            this.id = id;
            this.baseUri = baseUri;
        }

        @Override
        public int compareTo(Host o) {
            return id.compareTo(o.id);
        }
    }

    abstract protected void hostsChanged(List<Host> sortedHosts);

    abstract protected URI getBaseUriFromDiscovery();

    abstract protected static class Builder implements Serializable {
        private String _service;
        private String _zookeeperConnectionString;
        private String _zookeeperNamespace;
        private URI _directUri;

        public Builder(String service) {
            _service = checkNotNull(service, "Service name is required");
        }

        public Builder withZookeeperDiscovery(String zookeeperConnectionString, String zookeeperNamespace) {
            checkArgument(zookeeperConnectionString != null || zookeeperNamespace == null, "Connection string is required");
            _zookeeperConnectionString = zookeeperConnectionString;
            _zookeeperNamespace = zookeeperNamespace;
            return this;
        }

        public Builder withDirectUri(URI directUri) {
            _directUri = directUri;
            return this;
        }

        protected String getService() {
            return _service;
        }

        protected String getZookeeperConnectionString() {
            return _zookeeperConnectionString;
        }

        protected String getZookeeperNamespace() {
            return _zookeeperNamespace;
        }

        protected URI getDirectUri() {
            return _directUri;
        }

        protected void validate() {
            if (_zookeeperConnectionString == null && _directUri == null) {
                throw new IllegalStateException("At least one service discovery method is required");
            }
        }
    }
}
