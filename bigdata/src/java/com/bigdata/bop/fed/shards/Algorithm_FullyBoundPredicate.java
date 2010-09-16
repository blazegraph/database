package com.bigdata.bop.fed.shards;

import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.AbstractSplitter;
import com.bigdata.service.ndx.ISplitter;

/**
 * When the asBound predicates are known to be fully bound, then the
 * {@link AbstractSplitter} can be used. This approach is quite efficient.
 */
class Algorithm_FullyBoundPredicate<E extends IBindingSet, F> implements
        IShardMapper<E, F> {

    static transient private final Logger log = Logger
            .getLogger(Algorithm_FullyBoundPredicate.class);

    private final MapBindingSetsOverShardsBuffer<E, F> op;

    private final ISplitter splitter;

    public Algorithm_FullyBoundPredicate(
            final MapBindingSetsOverShardsBuffer<E, F> op) {

        this.op = op;

        this.splitter = new Splitter(op.mdi);

    }

    public void mapOverShards(final Bundle<F>[] bundles) {

        /*
         * Construct a byte[][] out of the sorted fromKeys and then generate
         * slices (Splits) which group the binding sets based on the target
         * shard.
         */
        final LinkedList<Split> splits;
        {

            final byte[][] keys = new byte[bundles.length][];

            for (int i = 0; i < bundles.length; i++) {

                keys[i] = bundles[i].fromKey;

            }

            splits = splitter.splitKeys(op.timestamp, 0/* fromIndex */,
                    bundles.length/* toIndex */, keys);

        }

        if (log.isTraceEnabled())
            log.trace("nsplits=" + splits.size() + ", pred=" + op.pred);

        /*
         * For each split, write the binding sets in that split onto the
         * corresponding buffer.
         */
        for (Split split : splits) {

            // Note: pmd is a PartitionLocator, so this cast is valid.
            final IBuffer<IBindingSet[]> sink = op
                    .getBuffer((PartitionLocator) split.pmd);

            final IBindingSet[] slice = new IBindingSet[split.ntuples];

            for (int j = 0, i = split.fromIndex; i < split.toIndex; i++, j++) {

                final IBindingSet bset = bundles[i].bindingSet;

                slice[j] = bset;

                if (log.isTraceEnabled())
                    log.trace("Mapping: keyOrder=" + op.keyOrder + ", bset="
                            + bset + " onto partitionId="
                            + split.pmd.getPartitionId());

            }

            sink.add(slice);

        }

    }

}
