package com.bigdata.bop.fed.shards;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;

/**
 * Interface for mapping {@link Bundle}s over shards.
 * 
 * @param <F>
 *            The generic type of the elements for the target
 *            {@link IPredicate}.
 * 
 * @todo Optimize locator lookup by caching and look at the locator scan
 *       code path.
 *       <p>
 *       For reads, we are permitted to cache the locators just as much as
 *       we like (but indirection would be introduced by a shared disk
 *       architecture).
 *       <p>
 *       For writes (or in a shard disk architecture) it is possible that
 *       the target shard will have moved by the time the receiver has
 *       notice of the intent to write on that shard or once the receiver
 *       has accepted the binding sets for that shard. The logic which moves
 *       the binding sets around will have to handle such 'stale locator'
 *       exceptions automatically.
 */
interface IShardMapper<E extends IBindingSet, F> {

    void mapOverShards(Bundle<F>[] bundles);

}