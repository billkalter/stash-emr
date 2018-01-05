package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;

public class BroadcastRegistry {

    private final List<Broadcast<?>> _broadcasts = Lists.newArrayList();

    public <T> Broadcast<T> register(Broadcast<T> broadcast) {
        _broadcasts.add(broadcast);
        return broadcast;
    }

    public void destroyAll() {
        Iterators.consumingIterator(_broadcasts.iterator()).forEachRemaining(broadcast -> broadcast.destroy(false));
    }
}
