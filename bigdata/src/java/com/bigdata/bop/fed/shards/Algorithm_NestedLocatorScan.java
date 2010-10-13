package com.bigdata.bop.fed.shards;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * This does a locatorScan for each binding set. This is a general purpose
 * technique, but it will issue one query to the {@link IMetadataIndex} per
 * source {@link IBindingSet}.
 */
class Algorithm_NestedLocatorScan<E extends IBindingSet, F> implements
        IShardMapper<E, F> {

    static transient private final Logger log = Logger
            .getLogger(Algorithm_NestedLocatorScan.class);

    private final MapBindingSetsOverShardsBuffer<E, F> op;

    public Algorithm_NestedLocatorScan(
            final MapBindingSetsOverShardsBuffer<E, F> op) {

        this.op = op;

    }

    public void mapOverShards(final Bundle<F>[] bundles) {

        for (Bundle<F> bundle : bundles) {

            /*
             * Locator scan for the index partitions for that predicate as
             * bound.
             */
            final Iterator<PartitionLocator> itr = op.locatorScan(
                    bundle.keyOrder, bundle.fromKey, bundle.toKey);

            while (itr.hasNext()) {

                final PartitionLocator locator = itr.next();

                if (log.isTraceEnabled())
					log.trace("adding bindingSet to buffer" + ": partitionId="
							+ locator.getPartitionId() + "dataService="
							+ locator.getDataServiceUUID() + ", bindingSet="
							+ bundle.bindingSet);

                final IBuffer<IBindingSet[]> sink = op.getBuffer(locator);

                sink.add(new IBindingSet[] { bundle.bindingSet });

            }

        }
    }

}
