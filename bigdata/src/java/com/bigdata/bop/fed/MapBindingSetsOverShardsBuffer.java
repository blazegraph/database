package com.bigdata.bop.fed;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.AbstractSplitter;
import com.bigdata.striterator.IKeyOrder;

/**
 * Unsynchronized (non-thread safe) buffer maps the {@link IBindingSet}s across
 * the index partition(s) associated with an {@link IPredicate} and
 * {@link IKeyOrder}. For each source chunk, "as bound" versions of the target
 * {@link IPredicate} are constructed and the {@link IBindingSet}s in the chunk
 * are reordered based on {@link IKeyOrder#getFromKey(IKeyBuilder, IPredicate)}
 * for each asBound predicate. The {@link PartitionLocator}s are discovered for
 * each fromKey using an ordered locator scan and the binding sets are output
 * onto a shard or node specific {@link IBuffer} created by a concrete subclass.
 * The subclass is responsible for getting the binding sets from this node onto
 * the node associated with each output buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: UnsyncDistributedOutputBuffer.java 3448 2010-08-18 20:55:58Z
 *          thompsonbry $
 * @param <E>
 *            The generic type of the elements in the buffer.
 * @param <F>
 *            The generic type of the elements in the relation associated with
 *            the {@link IPredicate}.
 * 
 * @todo This could be refactored such that it no longer implemented
 *       {@link IBuffer} but instead was a {@link BOp} with binding sets
 *       streaming in from its source. However, unlike a normal {@link BOp} it
 *       would have a compound sink and it would have to be tightly integrated
 *       with the {@link QueryEngine} to be used.
 * 
 * @todo Figure out how we will combine binding set streams emerging from
 *       concurrent tasks executing on a given node destined for the same
 *       shard/node. (There is code in the {@link DistributedJoinTask} which
 *       does this for the same shard, but it does it on the receiver side.) Pay
 *       attention to the #of threads running in the join, the potential
 *       concurrency of threads targeting the same (bopId,shardId) and how to
 *       best combine their data together.
 * 
 * @todo Optimize locator lookup by caching in {@link AbstractSplitter} and look
 *       at the code path for obtaining {@link PartitionLocator}s from the MDI.
 *       <p>
 *       For reads, we are permitted to cache the locators just as much as we
 *       like (but indirection would be introduced by a shared disk
 *       architecture).
 *       <p>
 *       For writes (or in a shard disk architecture) it is possible that the
 *       target shard will have moved by the time the receiver has notice of the
 *       intent to write on that shard or once the receiver has accepted the
 *       binding sets for that shard. The logic which moves the binding sets
 *       around will have to handle such 'stale locator' exceptions
 *       automatically.
 * 
 * @todo This is not tracking the #of output chunks or the fanOut (#of
 *       shards/nodes which will receive binding sets). Given that the query
 *       engine will be managing the buffers on which the data are written, it
 *       might also update the appropriate statistics.
 */
public abstract class MapBindingSetsOverShardsBuffer<E extends IBindingSet, F>
        extends AbstractUnsynchronizedArrayBuffer<E> {

    private static transient final Logger log = Logger.getLogger(MapBindingSetsOverShardsBuffer.class);
    
    /**
     * The predicate from which we generate the asBound binding sets. This
     * predicate and the {@link IKeyOrder} together determine the required
     * access path. 
     */
    private final IPredicate<F> pred;
    
    /**
     * Identifies the index for the access path required by the {@link #pred
     * predicate}.
     */
    private final IKeyOrder<F> keyOrder;

    /**
     * The timestamp associated with the operation on the target access path. If
     * the binding sets will be used to read on the shards of the target access
     * path, then this is the read timestamp. If they will be used to write on
     * the target access path, then this is the write timestamp.
     */
    private final long timestamp;
    
    /**
     * The {@link IKeyBuilder} for the index associated with the access path
     * required by the predicate. 
     */
    private final IKeyBuilder keyBuilder;
    
    /**
     * Used to efficient assign binding sets to index partitions.
     */
    private final Splitter splitter;

//    /**
//     */
//    private final BOpStats stats;

    /**
     * @param fed
     *            The federation.
     * @param pred
     *            The predicate associated with the target operator. The
     *            predicate identifies which variables and/or constants form the
     *            key for the access path and hence selects the shards on which
     *            the target operator must read or write. For example, when the
     *            target operator is a JOIN, this is the {@link IPredicate}
     *            associated with the right hand operator of the join.
     * @param keyOrder
     *            Identifies the access path for the target predicate.
     * @param timestamp
     *            The timestamp associated with the operation on the target
     *            access path. If the binding sets will be used to read on the
     *            shards of the target access path, then this is the read
     *            timestamp. If they will be used to write on the target access
     *            path, then this is the write timestamp.
     * @param capacity
     *            The capacity of this buffer.
     */
    public MapBindingSetsOverShardsBuffer(
            final IBigdataFederation<?> fed,//
            final IPredicate<F> pred, //
            final IKeyOrder<F> keyOrder,//
            final long timestamp,//
            final int capacity) {

        super(capacity);

        if (fed == null)
            throw new IllegalArgumentException();

        if (pred == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

//        this.context = context;

        this.pred = pred;

        this.keyOrder = keyOrder;

        this.timestamp = timestamp;

        /*
         * Note: we can use the read view of the relation to get the IKeyBuilder
         * even if we will be writing on the relation since the IKeyBuilder
         * semantics can not be readily changed once an index has been created.
         */
        {

            @SuppressWarnings("unchecked")
            final IRelation<F> relation = (IRelation<F>) fed
                    .getResourceLocator().locate(pred.getOnlyRelationName(),
                            timestamp);

            final IIndex index = relation.getIndex(keyOrder);

            this.keyBuilder = index.getIndexMetadata().getKeyBuilder();
            
        }

        /*
         * Resolve a scale-out view of the metadata index for the target
         * predicate.
         */
        {

            final String namespace = pred.getOnlyRelationName();

            final IMetadataIndex mdi = fed.getMetadataIndex(namespace + "."
                    + keyOrder.getIndexName(), timestamp);

            if (mdi == null) {

                throw new NoSuchIndexException("name=" + namespace
                        + ", timestamp=" + TimestampUtility.toString(timestamp));

            }

            this.splitter = new Splitter(mdi);
            
        }
        
//        this.stats = context.getStats();

    }

    /**
     * Helper class efficiently splits an array of sorted keys into groups
     * associated with a specific index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class Splitter extends AbstractSplitter {
        
        private final IMetadataIndex mdi;
        
        public Splitter(final IMetadataIndex mdi) {

            if (mdi == null)
                throw new IllegalArgumentException();
            
            this.mdi = mdi;

        }
        
        @Override
        protected IMetadataIndex getMetadataIndex(long ts) {
            
            return mdi;
            
        }
        
    }

    /**
     * Helper class used to place the binding sets into order based on the
     * {@link #fromKey} associated with the {@link #asBound} predicate.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class Bundle implements Comparable<Bundle> {

        /** The binding set. */
        final IBindingSet bindingSet;

        /** The asBound predicate. */
        final IPredicate<F> asBound;

        /** The fromKey generated from that asBound predicate. */
        final byte[] fromKey;

        public Bundle(final IBindingSet bindingSet) {

            this.bindingSet = bindingSet;
            
            this.asBound = pred.asBound(bindingSet);
            
            this.fromKey = keyOrder.getFromKey(keyBuilder, asBound);
            
        }

        /**
         * Imposes an unsigned byte[] order on the {@link #fromKey}.
         */
        public int compareTo(final Bundle o) {

            return BytesUtil.compareBytes(this.fromKey, o.fromKey);
            
        }

        /**
         * Implemented to shut up findbugs, but not used.
         */
        @SuppressWarnings("unchecked")
        public boolean equals(final Object o) {

            if (this == o)
                return true;

            if (!(o instanceof MapBindingSetsOverShardsBuffer.Bundle))
                return false;

            final MapBindingSetsOverShardsBuffer.Bundle t = (MapBindingSetsOverShardsBuffer.Bundle) o;

            if (compareTo(t) != 0)
                return false;

            if (!bindingSet.equals(t.bindingSet))
                return false;

            if (!asBound.equals(t.asBound))
                return false;

            return true;

        }
        
        /**
         * Implemented to shut up find bugs.
         */
        public int hashCode() {

            if (hash == 0) {

                hash = Arrays.hashCode(fromKey);
                
            }
          
            return hash;
            
        }
        private int hash = 0;
        
    }

    /**
     * Maps the chunk of {@link IBindingSet}s across the index partition(s) for
     * the sink join dimension.
     * 
     * @param a
     *            A chunk of {@link IBindingSet}s.
     */
    protected void handleChunk(final E[] chunk) {

        @SuppressWarnings("unchecked")
        final Bundle[] bundles = new MapBindingSetsOverShardsBuffer.Bundle[chunk.length];

        /*
         * Create the asBound version of the predicate and the associated
         * fromKey for each bindingSet in the chunk.
         */
        for (int i = 0; i < chunk.length; i++) {

            bundles[i] = new Bundle(chunk[i]);
            
        }

        /*
         * Sort the binding sets in the chunk by the fromKey associated with
         * each asBound predicate.
         */
        Arrays.sort(bundles);

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

            splits = splitter.splitKeys(timestamp, 0/* fromIndex */,
                    bundles.length/* toIndex */, keys);

        }

        if (log.isTraceEnabled())
            log.trace("nsplits=" + splits.size() + ", pred=" + pred);
        
        /*
         * For each split, write the binding sets in that split onto the
         * corresponding buffer.
         */
        for (Split split : splits) {

            // Note: pmd is a PartitionLocator, so this cast is valid.
            final IBuffer<IBindingSet[]> sink = getBuffer((PartitionLocator) split.pmd);

            final IBindingSet[] slice = new IBindingSet[split.ntuples];

            for (int j = 0, i = split.fromIndex; i < split.toIndex; i++, j++) {
                
                final IBindingSet bset = bundles[i].bindingSet;
                
                slice[j] = bset;

                if (log.isTraceEnabled())
                    log
                            .trace("Mapping: keyOrder=" + keyOrder + ",bset="
                                    + bset + " onto partitionId="
                                    + split.pmd.getPartitionId());

            }

//            for (int i = split.fromIndex; i < split.toIndex; i++) {
//
//                final Bundle bundle = bundles[i];
//
//                sink.add(bundle.bindingSet);
//
////                stats.unitsOut.increment();
//
//            }
            
            sink.add(slice);
            
        }

    }

    /**
     * Extended to flush each buffer which targets a specific index partition as
     * well.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public long flush() {

        final long n = super.flush();

        for (IBuffer<IBindingSet[]> sink : sinks.values()) {

            if (!sink.isEmpty())
                sink.flush();

        }

        return n;

    }

    /**
     * The allocated sinks.
     * <p>
     * Note: Since the collection is not thread-safe, synchronization is
     * required when adding to the collection and when visiting the elements of
     * the collection. However, the {@link MapBindingSetsOverShardsBuffer} is not
     * thread-safe either so this should be Ok.
     */
    private final LinkedHashMap<PartitionLocator, IBuffer<IBindingSet[]>/* sink */> sinks = new LinkedHashMap<PartitionLocator, IBuffer<IBindingSet[]>>();

    /**
     * An immutable view of the sinks.
     */
    protected Map<PartitionLocator, IBuffer<IBindingSet[]>/* sink */> getSinks() {

        return Collections.unmodifiableMap(sinks);

    }

    /**
     * Return the buffer used to absorb binding sets which target the specified
     * index partition.
     * 
     * @param locator
     *            The locator for the target index partition.
     * 
     * @return The buffer.
     */
    private IBuffer<IBindingSet[]> getBuffer(final PartitionLocator locator) {

        IBuffer<IBindingSet[]> sink = sinks.get(locator);

        if (sink == null) {

            sinks.put(locator, sink = newBuffer(locator));

        }

        return sink;

    }

    /**
     * Return a buffer onto which binding sets will be written which are
     * destined for the specified shard.
     * <p>
     * Note: The concrete implementation may chose whether to associate buffers
     * with the target shard or the target node.
     * 
     * @param locator
     *            The locator for the target index partition.
     * 
     * @return The buffer.
     */
    abstract IBuffer<IBindingSet[]> newBuffer(PartitionLocator locator);

}
