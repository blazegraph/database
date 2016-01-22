package com.bigdata.bop.fed.shards;

import com.bigdata.bop.IBindingSet;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * @todo Implement {@link Algorithm_LowShardCount}.
 *       <p>
 *       When the #of index partitions is small, it is efficient to locally
 *       materialize the {@link IMetadataIndex} using a locator scan and then
 *       assign the {@link Bundle}s to the partitions.
 *       <p>
 *       Rather than worrying about multiple locator scans against the remote
 *       {@link IMetadataIndex}, we can run a relatively naive (or more
 *       sophisticated) scan against a cached {@link IMetadataIndex}.
 *       <p>
 *       This could be done either by explicit caching of the
 *       {@link IMetadataIndex} or by materialization of a
 *       {@link PartitionLocator}[] and either scanning it or using a binary
 *       search.
 *       <p>
 *       Note that the {@link IMetadataIndex} might already be cached in the low
 *       shard count condition, in which case we could use any of the general
 *       purpose {@link Algorithm_NestedLocatorScan}.
 */
class Algorithm_LowShardCount<E extends IBindingSet, F> implements
        IShardMapper<E, F> {

    public void mapOverShards(Bundle<F>[] bundles) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}
