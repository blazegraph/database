package com.bigdata.bop.fed.shards;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.solutions.SortOp;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IKeyOrder;

/**
 * A stream of {@link IBindingSet} are mapped across the shards which will have
 * the data for the {@link IPredicate#asBound(IBindingSet)} {@link IPredicate}.
 * <p>
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
 *       <p>
 *       In fact, this is pretty much just doing a join against the metadata
 *       index. However, it presumes that there are far fewer index partitions
 *       than tuples flowing through the system and that it is better to read
 *       remotely from the {@link IMetadataIndex} and cache than do use the
 *       general purpose pipeline join, which would cause all binding sets to be
 *       routed through the centralized {@link IMetadataIndex}.
 */
public abstract class MapBindingSetsOverShardsBuffer<E extends IBindingSet, F>
        extends AbstractUnsynchronizedArrayBuffer<E> {

//    static transient private final Logger log = Logger.getLogger(MapBindingSetsOverShardsBuffer.class);
    
    protected final AbstractScaleOutFederation<?> fed;
    
    /**
     * The predicate from which we generate the asBound binding sets. This
     * predicate and the {@link IKeyOrder} together determine the required
     * access path. 
     */
    protected final IPredicate<F> pred;

//    /**
//     * Identifies the index for the access path required by the {@link #pred
//     * predicate}.
//     */
//    protected final IKeyOrder<F> keyOrder;

    /**
     * The timestamp associated with the operation on the target access path. If
     * the binding sets will be used to read on the shards of the target access
     * path, then this is the read timestamp. If they will be used to write on
     * the target access path, then this is the write timestamp.
     */
    protected final long timestamp;
    
//    /**
//     * The name of the scale-out index associated with the {@link #pred
//     * predicate}, including both the relation name and the {@link IKeyOrder}
//     * components of the index name.
//     */
//    protected final String namespace;
//
//    /**
//     * The associated {@link IMetadataIndex}.
//     * 
//     * @todo might be moved into the {@link IShardMapper} constructors for
//     *       efficiency so only materialized when necessary. Alternatively, we
//     *       might get the {@link IKeyBuilder} from the index metadata template
//     *       on the {@link IMetadataIndex} and thus avoid lookup of the
//     *       {@link IRelation}.
//     */
//    protected final IMetadataIndex mdi;
//    
//    /**
//     * The {@link IKeyBuilder} for the index associated with the access path
//     * required by the predicate. 
//     */
//    protected final IKeyBuilder keyBuilder;

    /**
     * A scale-out view of the target relation.
     */
    protected final IRelation<F> relation;
    
    /**
     * The implementation class for the algorithm which will be used to map the
     * {@link IBindingSet}s over the shards.
     */
    private final IShardMapper<E,F> algorithm;
    
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
     * @param timestamp
     *            The timestamp associated with the operation on the target
     *            access path. If the binding sets will be used to read on the
     *            shards of the target access path, then this is the read
     *            timestamp. If they will be used to write on the target access
     *            path, then this is the write timestamp.
     * @param capacity
     *            The capacity of this buffer.
     */
//    * @param keyOrder
//    *            Identifies the access path for the target predicate.
    public MapBindingSetsOverShardsBuffer(
            final IBigdataFederation<?> fed,//
            final IPredicate<F> pred, //
//            final IKeyOrder<F> keyOrder,//
            final long timestamp,//
            final int capacity) {

        super(capacity);

        if (fed == null)
            throw new IllegalArgumentException();

        if (pred == null)
            throw new IllegalArgumentException();

//        /*
//         * This class was reworked to target the index which is
//         * selected dynamically rather than based on a static given IKeyOrder.
//         * Just use relation.getKeyOrder(predicate) for each asBound predicate
//         * and then obtain the IKeyBuilder from the scale-out view of the
//         * associated index.
//         */
//        if (keyOrder == null)
//            throw new IllegalArgumentException();

        this.fed = (AbstractScaleOutFederation<?>) fed;
        
        this.pred = pred;

//        this.namespace = pred.getOnlyRelationName() + "."
//                + keyOrder.getIndexName();
//
//        this.keyOrder = keyOrder;

        this.timestamp = timestamp;

//        /*
//         * Note: we can use the read view of the relation to get the IKeyBuilder
//         * even if we will be writing on the relation since the IKeyBuilder
//         * semantics can not be readily changed once an index has been created.
//         */
        {

            final String namespace = pred.getOnlyRelationName();
            
//            @SuppressWarnings("unchecked")
            this.relation = (IRelation<F>) fed.getResourceLocator().locate(
                    namespace, timestamp);

            if (relation == null)
                throw new RuntimeException("Not found: relation=" + namespace
                        + "@" + TimestampUtility.toString(timestamp));
            
//            final IIndex index = relation.getIndex(keyOrder);
//
//            this.keyBuilder = index.getIndexMetadata().getKeyBuilder();
            
        }
//
//        /*
//         * Resolve a scale-out view of the metadata index for the target
//         * predicate.
//         */
//        {
//
//            mdi = fed.getMetadataIndex(namespace, timestamp);
//
//            if (mdi == null) {
//
//                throw new NoSuchIndexException("name=" + namespace
//                        + ", timestamp=" + TimestampUtility.toString(timestamp));
//
//            }
//            
//        }

        /*
         * @todo Conditionally choose the best algorithm. I am not sure if we
         * might want to do via an annotation (of the target predicate), if this
         * is something that we determine automatically, or if there is some
         * combination of the two which will work best. (For example, the query
         * optimizer will know if the target predicate will be fully bound for
         * all inputs but we do not have that information available locally).
         * 
         * @todo From the perspective of the unit tests, it is important to have
         * this be declarative so we can test each of the different algorithms
         * independently.
         */
        final boolean predicateWillBeFullyBound = false;
        if (predicateWillBeFullyBound) {

            /*
             * Uses ISplitter, but requires keys based on fully bound predicate.
             * 
             * Note: This can not be used when there are optional joins other
             * other conditionals which could result in the predicate not being
             * fully bound for some evaluation paths.
             */
            
            /*
             * The key order which will be used for that relation for a fully
             * bound predicate.
             */
            final IKeyOrder<F> keyOrder = null;
            
            algorithm = new Algorithm_FullyBoundPredicate<E, F>(this, keyOrder);
            
        } else {

            // general purpose.
            algorithm = new Algorithm_NestedLocatorScan<E, F>(this);

        }

    }

    /**
     * Resolve a scale-out view of the metadata index for the target predicate.
     */
    protected IMetadataIndex getMetadataIndex(final IKeyOrder<F> keyOrder) {

        final String namespace = relation.getFQN(keyOrder);

        final IMetadataIndex mdi = fed.getMetadataIndex(namespace, timestamp);

        if (mdi == null) {

            throw new NoSuchIndexException("name=" + namespace + ", timestamp="
                    + TimestampUtility.toString(timestamp));

        }
        
        return mdi;

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
        final Bundle<F>[] bundles = new Bundle[chunk.length];

		/*
		 * Create the asBound version of the predicate and the associated
		 * fromKey for each bindingSet in the chunk.
		 */
		for (int i = 0; i < chunk.length; i++) {

			// an intermediate solution.
			final IBindingSet bindingSet = chunk[i];

			// the asBound version of the predicate.
			final IPredicate<F> asBound = pred.asBound(bindingSet);

			// the index which will be used for that asBound predicate.
			final IKeyOrder<F> keyOrder = relation.getKeyOrder(asBound);

			// the key builder associated with that index.
			final IKeyBuilder keyBuilder = relation.getIndex(keyOrder)
					.getIndexMetadata().getKeyBuilder();

			// save the bundle for processing.
			bundles[i] = new Bundle<F>(keyBuilder, asBound, keyOrder,
					bindingSet);

		}

//        /*
//         * Sort the binding sets in the chunk by the fromKey associated with
//         * each asBound predicate. [Sort is moved into the implementation.]
//         */
//        Arrays.sort(bundles);

        /*
         * Map the bundles over the shards.
         */
        algorithm.mapOverShards(bundles);

    }

    /**
     * Locator scan for the index partitions for that predicate as bound.
     */
    protected Iterator<PartitionLocator> locatorScan(
            final IKeyOrder<F> keyOrder, final byte[] fromKey,
            final byte[] toKey) {

        final String name = relation.getFQN(keyOrder);

        return fed
                .locatorScan(name, timestamp, fromKey, toKey, false/* reverse */);

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
     * 
     * @todo Rather than exposing all sinks and requiring that all sinks be
     *       fully buffered, it would be better to hook the production of the
     *       binding sets for a given {@link PartitionLocator} since many of the
     *       {@link IShardMapper}s can know when they will not see another
     *       binding set for a given {@link PartitionLocator} and hence the data
     *       can be immediately flushed to that target.
     *       <p>
     *       A similar scaling concern with very large numbers of source binding
     *       sets is that we may be better off applying a {@link SortOp} to the
     *       binding sets, which allows us to use external merge sorts or even
     *       hash partitioned distributed merge sorts. This suggests that we
     *       should really unpack this class as a general purpose operator with
     *       special integration into the query engine.
     */
    public Map<PartitionLocator, IBuffer<IBindingSet[]>/* sink */> getSinks() {

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
    protected IBuffer<IBindingSet[]> getBuffer(final PartitionLocator locator) {

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
    abstract protected IBuffer<IBindingSet[]> newBuffer(PartitionLocator locator);

}
