package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.discovery.EmoServiceDiscovery;
import com.bazaarvoice.emodb.stash.emr.discovery.PartitionedDiscovery;
import com.google.common.base.Strings;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link PartitionedDiscovery} implementation for discovering EmoDB databus servers.
 */
public class DatabusDiscovery extends PartitionedDiscovery {

    DatabusDiscovery(String zookeeperConnectionString, String zookeeperNamespace, String service, String partitionKey, URI directUri) {
        super(zookeeperConnectionString, zookeeperNamespace, service, partitionKey, directUri);
    }

    public static Builder builder(String cluster) {
        return new Builder(cluster);
    }

    public static class Builder extends EmoServiceDiscovery.Builder {
        private String _subscription;

        Builder(String cluster) {
            super(checkNotNull(cluster, "Cluster is required") + "-emodb-bus-1");
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

        public Builder withSubscription(String subscription) {
            _subscription = subscription;
            return this;
        }

        public void validate() {
            super.validate();
            if (Strings.isNullOrEmpty(_subscription)) {
                throw new IllegalStateException("A valid subscription is required");
            }
        }

        public DatabusDiscovery build() {
            validate();
            return new DatabusDiscovery(
                    getZookeeperConnectionString(), getZookeeperNamespace(), getService(), _subscription, getDirectUri());
        }
    }
}
