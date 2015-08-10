package com.bigdata.bop.fed.shards;

import com.bigdata.bop.IBindingSet;
import com.bigdata.mdi.PartitionLocator;

/**
 * FIXME Implement {@link Algorithm_GroupByLocatorScan}.
 * <p>
 * Each asBound predicate can read on several shards and the mappings from the
 * asBound predicate onto the shards can overlap. This can happen any time the
 * asBound predicate is not fully bound. This algorithm handles this case
 * efficiently when many asBound predicates share a common fromShard (the first
 * shard they will read on).
 * <p>
 * The asBound predicates are organized into groups based on the first partition
 * into which their fromKey would be mapped. These groups are lazily discovered
 * beginning with the fromKey which is ordered first when the {@link Bundle}s
 * are sorted. {@link Bundle}s are assigned to the same group if their fromKey
 * would select the same {@link PartitionLocator} (the test is first partition
 * whose fromKey is GTE the probe key and can be verified by ensuring that the
 * probe key in question is GTE the partition's fromKey and LT its toKey). Since
 * the {@link Bundle}s are sorted, we can quickly assign them into the current
 * group. We then do one locator scan for that group (unless the toKeys are also
 * in the same partition, in which case we are done with the group).
 * <p>
 * This can be improved on by reuse of the locators visited for the last group
 * when considering the next group. If fromKey for the first asBound predicate
 * in the next group is GTE the toKey of the last visited
 * {@link PartitionLocator} then no reuse is possible. Otherwise, the
 * {@link PartitionLocator}s can be reused though a new locator scan may be
 * required to visit {@link PartitionLocator}s for keys GT the probe key for the
 * last asBound predicate in the previous group. This reuse can extend across
 * groups as well.
 */
class Algorithm_GroupByLocatorScan<E extends IBindingSet, F> implements
        IShardMapper<E, F> {

    public void mapOverShards(Bundle<F>[] bundles) {

        // final byte[] fromKey = bundles[0].fromKey;
        //
        // final byte[] toKey = bundles[bundles.length - 1].toKey;

        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}
