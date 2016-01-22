package com.bigdata.bop.fed.shards;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * FIXME Implement {@link Algorithm_AsGivenPredicate}.
 * <p>
 * This algorithm should be applied if the asBound predicate is known to be
 * either fully unbound (no variables are bound). In this case we know that we
 * will map each source binding set across all shards. All the implementation
 * needs to do is copy the references for the source binding sets onto an
 * {@link IBuffer} for each shard in that scale-out index.
 * <p>
 * A refinement of this would handle the case where the source binding sets will
 * not bind any variables for the target predicate. Here, instead of mapping
 * each source binding set across all shards we map each source binding set
 * across all shards having the constant prefix associated with the "as given"
 * {@link IPredicate} (since no variables from the source binding sets become
 * bound on the target predicate, all binding sets will be sent to all shards
 * spanned by that target predicate).
 * <p>
 * This could be hugely more efficient when we being with a predicate which is
 * relatively unselective.
 */
class Algorithm_AsGivenPredicate<E extends IBindingSet, F> implements
        IShardMapper<E, F> {

    public void mapOverShards(Bundle<F>[] bundles) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}