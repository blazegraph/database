package com.bigdata.rdf.magic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

public class MagicRelation extends AbstractRelation<IMagicTuple> {

    protected static final Logger log = Logger.getLogger(MagicRelation.class);

    private final int arity;
    
    private final Set<String> indexNames;
    
    private transient MagicKeyOrder[] keyOrders;
//    private transient IKeyOrder<IMagicTuple>[] keyOrders;
    
    public MagicRelation(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        final String arity = properties.getProperty(MagicSchema.ARITY); 
        if (arity == null) {
            throw new IllegalArgumentException(
                "you must specify the arity for this relation");
        }
        this.arity = Integer.valueOf(arity);
        
        // @todo should be pregenerated and immutable, not deferred until create().
        this.indexNames = new HashSet<String>();
        
    }
    
    public int getArity() {
        return arity;
    }
    
    @Override
    public void create() {
        
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            // create the relation declaration metadata.
            super.create();

            final IIndexManager indexManager = getIndexManager();
            
            this.keyOrders = MagicKeyOrderStrategy.calculateKeyOrders(arity);

            // store the key orders somewhere
            // indexManager.getGlobalRowStore().write(RelationSchema.INSTANCE, propertySet);

            for (IKeyOrder<IMagicTuple> keyOrder : keyOrders) {
            
                if (log.isInfoEnabled()) {
                    log.info("creating index: " + getFQN(keyOrder));
                }
                
                final IndexMetadata indexMetadata = 
                    getMagicTupleIndexMetadata(keyOrder);

                indexManager.registerIndex(indexMetadata);
                
                indexNames.add(getFQN(keyOrder));
                
            }

        } finally {

            unlock(resourceLock);

        }
        
    }
    
    @Override
    public void destroy() {
        
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            final IIndexManager indexManager = getIndexManager();

//            final MagicKeyOrder[] keyOrders = getKeyOrders(); 
            
            for (IKeyOrder<IMagicTuple> keyOrder : keyOrders) {
                
                if (log.isInfoEnabled()) {
                    log.info("destroying index: " + getFQN(keyOrder));
                }
                
                indexManager.dropIndex(getFQN(keyOrder));
                
            }

            // destroy the relation declaration metadata.
            super.destroy();

        } finally {

            unlock(resourceLock);

        }
        
    }
    
    /**
     * Really need to keep this in the global row store or something to avoid
     * re-calculating all the time.
     * 
     * @return
     */
    public Iterator<IKeyOrder<IMagicTuple>> getKeyOrders() {
        
        if (keyOrders == null) {
            
            keyOrders = MagicKeyOrderStrategy.calculateKeyOrders(arity);
            
        }
        
        return Arrays.asList((IKeyOrder<IMagicTuple>[])keyOrders).iterator();

    }

    protected IndexMetadata getMagicTupleIndexMetadata(
            final IKeyOrder<IMagicTuple> keyOrder) {

        final IndexMetadata metadata = newIndexMetadata(getFQN(keyOrder));

        metadata.setTupleSerializer(new MagicTupleSerializer(keyOrder));

        if (false) { // if (bloomFilter && keyOrder.isPrimary()) {
            
            final BloomFilterFactory factory = BloomFilterFactory.DEFAULT;
            
            if (log.isInfoEnabled())
                log.info("Enabling bloom filter for SPO index: " + factory);
            
            metadata.setBloomFilterFactory( factory );
            
        }
        
        return metadata;

    }

    //@todo get rid of this and inherit the base class behavior.
    public long delete(IChunkedOrderedIterator<IMagicTuple> itr) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    //@todo get rid of this and inherit the base class behavior.
    public long insert(IChunkedOrderedIterator<IMagicTuple> itr) {
        
        try {
            long n = 0;
            while (itr.hasNext()) {
                final IMagicTuple[] chunk = itr.nextChunk();
                n += insert(chunk, chunk.length);
            }
            return n;
        } finally {
            itr.close();
        }
        
    }

    //@todo get rid of this and inherit the base class behavior.
    public long insert(final IMagicTuple[] tuples, final int numTuples) {

        if (tuples == null)
            throw new IllegalArgumentException();
        
        if (numTuples > tuples.length)
            throw new IllegalArgumentException();

        for (IMagicTuple tuple : tuples) {
            if (tuple.getTermCount() != arity) {
                throw new IllegalArgumentException(
                    "bad tuple, incorrect arity: " + tuple.toString());
            }
        }
        
        if (numTuples == 0)
            return 0L;

        final long begin = System.currentTimeMillis();

        if(log.isDebugEnabled()) {
            
            log.debug("indexManager="+getIndexManager());
            
        }
        
        // time to sort the statements.
        final AtomicLong sortTime = new AtomicLong(0);

        // time to generate the keys and load the statements into the
        // indices.
        final AtomicLong insertTime = new AtomicLong(0);

        final AtomicLong mutationCount = new AtomicLong(0);
        
        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        for (IKeyOrder<IMagicTuple> keyOrder : keyOrders) {
            
            tasks.add(new MagicIndexWriter(this, tuples, numTuples,
                    false/* clone */, (MagicKeyOrder) keyOrder,
                    null/* filter */, sortTime, insertTime, mutationCount));
            
        }

        final List<Future<Long>> futures;
        final long[] elapsedPerIndex = new long[tasks.size()];

        try {

            futures = getExecutorService().invokeAll(tasks);

            for (int i = 0; i < elapsedPerIndex.length; i++) {
                elapsedPerIndex[i] = futures.get(i).get();
            }

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && numTuples > 1000) {

            log.info("Wrote " + numTuples + " statements (mutationCount="
                    + mutationCount + ") in " + elapsed + "ms" //
                    + "; sort=" + sortTime + "ms" //
                    + ", keyGen+insert=" + insertTime + "ms" //
//                    + "; spo=" + elapsed_SPO + "ms" //
//                    + ", pos=" + elapsed_POS + "ms" //
//                    + ", osp=" + elapsed_OSP + "ms" //
            );

        }

        return mutationCount.get();
        
    }

    public IAccessPath<IMagicTuple> getAccessPath(
            final IPredicate<IMagicTuple> predicate) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        checkPredicate(predicate);
        
        return _getAccessPath(predicate);
        
    }

    /**
     * Isolates the logic for selecting the {@link SPOKeyOrder} from the
     * {@link SPOPredicate} and then delegates to
     * {@link #getAccessPath(IKeyOrder, IPredicate)}.
     */
    final private IAccessPath<IMagicTuple> _getAccessPath(
            final IPredicate<IMagicTuple> predicate) {

        final MagicKeyOrder keyOrder = getKeyOrder(predicate);

        final IAccessPath<IMagicTuple> accessPath = getAccessPath(keyOrder,
                predicate);

        if (log.isDebugEnabled())
            log.debug(accessPath.toString());

        return accessPath;

    }

    public IKeyOrder<IMagicTuple> getPrimaryKeyOrder() {
        
        return keyOrders[0];
        
    }
    
    @SuppressWarnings("unchecked")
    public IAccessPath<IMagicTuple> getAccessPath(
            final IKeyOrder<IMagicTuple> keyOrder) {
        
        final IVariableOrConstant<IV>[] terms = 
            new IVariableOrConstant[arity];
        
        for (int i = 0; i < terms.length; i++) {
            
            terms[i] = Var.var("v"+i);
            
        }

        return getAccessPath(keyOrder, new MagicPredicate(getNamespace(),
                terms));
        
    }

    private void checkPredicate(final IPredicate<IMagicTuple> predicate) {
        
        if (predicate.arity() != this.arity) {
            final StringBuilder sb = new StringBuilder();
            sb.append("bad predicate:\n");
            sb.append("relation: " + getNamespace()).append("\n");
            sb.append("arity: " + getArity()).append("\n");
            sb.append("predicate: " + predicate).append("\n");
            sb.append("predicate arity: " + predicate.arity());
            throw new IllegalArgumentException(sb.toString());
        }
        
    }
    
    /**
     * Return the {@link MagicKeyOrder} for the given predicate.
     * 
     * @param predicate
     *            The predicate.
     *            
     * @return The {@link MagicKeyOrder}
     */
    public MagicKeyOrder getKeyOrder(final IPredicate<IMagicTuple> predicate) {

        checkPredicate(predicate);
        
        int numBound = 0;
        int[] bound = new int[predicate.arity()];
        for (int i = 0; i < predicate.arity(); i++) {
            if (predicate.get(i).isVar() == false) {
                bound[numBound++] = i;
            }
        }
        int[] compact = new int[numBound];
        System.arraycopy(bound, 0, compact, 0, numBound);
        bound = compact;
//        MagicKeyOrder[] keyOrders = getKeyOrders();
        if (numBound == 0 || numBound == arity) {
            return keyOrders[0];
        }
        for (MagicKeyOrder keyOrder : keyOrders) {
            if (keyOrder.canService(bound)) {
                return keyOrder;
            }
        }
        
        throw new IllegalStateException();
        
    }
    
    /**
     * Core impl.
     * <p>
     * Note: This method is NOT cached. See {@link #getAccessPath(IPredicate)}.
     * 
     * @param keyOrder
     *            The natural order of the selected index (this identifies the
     *            index).
     * @param predicate
     *            The predicate specifying the query constraint on the access
     *            path.
     * 
     * @return The access path.
     * 
     * FIXME This does not touch the cache. Track down the callers. I imagine
     * that these are mostly SPO access path scans, but they could also be scans
     * on the POS or OSP indices. What we really want is a method with the
     * signature <code>getAccessPath(keyOrder,filter)</code>, where the
     * filter is optional. This method should cache by the keyOrder, which
     * implies that we want to either verify or layer on the filter if we will
     * be reusing the cached access path with different filter values.
     * <p>
     * The application SHOULD NOT specify both the predicate and the keyOrder
     * since they are less likely to make the right choice, but it is reasonable
     * to specify the keyOrder for bulk copy, dump, and some other modestly low
     * level things and when only a single access path is used, then of course
     * we need to specify that access path (several things use a temporary
     * triple store with only the SPO access path).
     */
    public IAccessPath<IMagicTuple> getAccessPath(final IKeyOrder<IMagicTuple> keyOrder,
            final IPredicate<IMagicTuple> predicate) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        checkPredicate(predicate);
        
        final IIndex ndx = getIndex(keyOrder);

        if (ndx == null) {
        
            throw new IllegalArgumentException("no index? relation="
                    + getNamespace() + ", timestamp=" + getTimestamp()
                    + ", keyOrder=" + keyOrder + ", pred=" + predicate
                    + ", indexManager=" + getIndexManager());
            
        }
        
        final int flags = IRangeQuery.KEYS
                | IRangeQuery.VALS
                | (TimestampUtility.isReadOnly(getTimestamp()) ? IRangeQuery.READONLY
                        : 0);
        
        final AbstractTripleStore container = getContainer();
        
        final int chunkOfChunksCapacity = container.getChunkOfChunksCapacity();

        final int chunkCapacity = container.getChunkCapacity();

        final int fullyBufferedReadThreshold = container.getFullyBufferedReadThreshold();
        
        return new AccessPath<IMagicTuple>(this, getIndexManager(),
                getTimestamp(), predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold).init();

    }
    
    /**
     * Strengthened return type.
     */
    public AbstractTripleStore getContainer() {

        return (AbstractTripleStore) super.getContainer();
        
    }

    public Set<String> getIndexNames() {
        return indexNames;
    }

//    public IMagicTuple newElement(IPredicate<IMagicTuple> predicate,
//            IBindingSet bindingSet) {
//
//        if (predicate == null)
//            throw new IllegalArgumentException();
//        
//        if (bindingSet == null)
//            throw new IllegalArgumentException();
//        
//        final IV[] terms = new IV[arity];
//        for (int i = 0; i < arity; i++) {
//            terms[i] = asBound(predicate, i, bindingSet);
//        }
//        
//        final MagicTuple magicTuple = new MagicTuple(terms);
//        
//        return magicTuple;
//        
//    }

    @SuppressWarnings("unchecked")
    public IMagicTuple newElement(final List<BOp> a,
            final IBindingSet bindingSet) {

        if (a == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IV[] terms = new IV[arity];

        final Iterator<BOp> itr = a.iterator();
        
        for (int i = 0; i < arity; i++) {
        
            terms[i] = (IV) ((IVariableOrConstant<?>) itr.next())
                    .get(bindingSet);
            
        }
        
        final MagicTuple magicTuple = new MagicTuple(terms);
        
        return magicTuple;
        
    }

    public Class<IMagicTuple> getElementClass() {
        
        return IMagicTuple.class;
        
    }

//    /**
//     * Extract the bound value from the predicate. When the predicate is not
//     * bound at that index, then extract its binding from the binding set.
//     * 
//     * @param pred
//     *            The predicate.
//     * @param index
//     *            The index into that predicate.
//     * @param bindingSet
//     *            The binding set.
//     *            
//     * @return The bound value.
//     */
//    private IV asBound(final IPredicate<IMagicTuple> predicate, 
//            final int index, final IBindingSet bindingSet) {
//
//        final IVariableOrConstant<IV> t = predicate.get(index);
//        final IConstant<IV> c;
//        if (t.isVar()) {
//            c = bindingSet.get((IVariable) t);
//        } else {
//            c = (IConstant<IV>) t;
//        }
//
//        return c.get();
//
//    }

}
