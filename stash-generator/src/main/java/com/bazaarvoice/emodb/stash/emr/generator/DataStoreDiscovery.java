package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.discovery.EmoServiceDiscovery;
import com.bazaarvoice.emodb.stash.emr.discovery.RoundRobinDiscovery;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link RoundRobinDiscovery} implementation for discovering EmoDB DataStore servers.
 */
public class DataStoreDiscovery extends RoundRobinDiscovery {

    DataStoreDiscovery(String zookeeperConnectionString, String zookeeperNamespace, String service, URI directUri) {
        super(zookeeperConnectionString, zookeeperNamespace, service, directUri);
    }

    public static Builder builder(String cluster) {
        return new Builder(cluster);
    }

    public static class Builder extends EmoServiceDiscovery.Builder {
        Builder(String cluster) {
            super(checkNotNull(cluster, "Cluster is required") + "-emodb-sor-1");
        }

        @Override
        public Builder withZookeeperDiscovery(String zookeeperConnectionString, String zookeeperNamespace) {
            super.withZookeeperDiscovery(zookeeperConnectionString, zookeeperNamespace);
            return this;
        }

        @Override
        public Builder withDirectUri(URI directUri) {
            super.withDirectUri(directUri);
            return this;
        }

        public DataStoreDiscovery build() {
            validate();
            return new DataStoreDiscovery(
                    getZookeeperConnectionString(), getZookeeperNamespace(), getService(), getDirectUri());
        }
    }
}
