package com.bazaarvoice.emodb.stash.emr.discovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CuratorPool {

    private final static LoadingCache<String, CuratorFramework> _curators = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, CuratorFramework>() {
                @Override
                public CuratorFramework load(String connectionString) throws Exception {
                    return new RefCountCurator(connectionString);
                }
            });

    synchronized public static CuratorFramework getStartedCurator(String connectionString) {
        CuratorFramework curator = _curators.getUnchecked(connectionString);
        curator.start();
        return curator;
        
    }

    private static class RefCountCurator implements CuratorFramework {
        private final String _connectionString;
        private volatile CuratorFramework _curator;
        private volatile int _refCount = 0;

        RefCountCurator(String connectionString) {
            _connectionString = connectionString;
        }

        @Override
        synchronized public void start() {
            if (_refCount == 0) {
                ThreadFactory threadFactory = new ThreadFactoryBuilder()
                        .setNameFormat("Curator-" + _connectionString + "-%d")
                        .setDaemon(true)
                        .build();

                _curator = CuratorFrameworkFactory.builder()
                        .connectString(_connectionString)
                        .retryPolicy(new RetryNTimes(5, 100))
                        .threadFactory(threadFactory)
                        .build();
                
                _curator.start();
            }
            _refCount += 1;
        }

        @Override
        synchronized public void close() {
            _refCount -= 1;
            if (_refCount == 0) {
                _curator.close();
                _curator = null;
            }
        }

        @Override
        public CuratorFrameworkState getState() {
            return _curator.getState();
        }

        @Override
        public boolean isStarted() {
            return _curator.isStarted();
        }

        @Override
        public CreateBuilder create() {
            return _curator.create();
        }

        @Override
        public DeleteBuilder delete() {
            return _curator.delete();
        }

        @Override
        public ExistsBuilder checkExists() {
            return _curator.checkExists();
        }

        @Override
        public GetDataBuilder getData() {
            return _curator.getData();
        }

        @Override
        public SetDataBuilder setData() {
            return _curator.setData();
        }

        @Override
        public GetChildrenBuilder getChildren() {
            return _curator.getChildren();
        }

        @Override
        public GetACLBuilder getACL() {
            return _curator.getACL();
        }

        @Override
        public SetACLBuilder setACL() {
            return _curator.setACL();
        }

        @Override
        public CuratorTransaction inTransaction() {
            return _curator.inTransaction();
        }

        @Override
        public void sync(String path, Object backgroundContextObject) {
            _curator.sync(path, backgroundContextObject);
        }

        @Override
        public SyncBuilder sync() {
            return _curator.sync();
        }

        @Override
        public Listenable<ConnectionStateListener> getConnectionStateListenable() {
            return _curator.getConnectionStateListenable();
        }

        @Override
        public Listenable<CuratorListener> getCuratorListenable() {
            return _curator.getCuratorListenable();
        }

        @Override
        public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
            return _curator.getUnhandledErrorListenable();
        }

        @Override
        public CuratorFramework nonNamespaceView() {
            return _curator.nonNamespaceView();
        }

        @Override
        public CuratorFramework usingNamespace(String newNamespace) {
            return _curator.usingNamespace(newNamespace);
        }

        @Override
        public String getNamespace() {
            return _curator.getNamespace();
        }

        @Override
        public CuratorZookeeperClient getZookeeperClient() {
            return _curator.getZookeeperClient();
        }

        @Override
        public EnsurePath newNamespaceAwareEnsurePath(String path) {
            return _curator.newNamespaceAwareEnsurePath(path);
        }

        @Override
        public void clearWatcherReferences(Watcher watcher) {
            _curator.clearWatcherReferences(watcher);
        }

        @Override
        public void blockUntilConnected() throws InterruptedException {
            _curator.blockUntilConnected();
        }

        @Override
        public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
            return _curator.blockUntilConnected(maxWaitTime, units);
        }
    }
}
