package com.bigdata.rdf.iris;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOIndexWriter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IChunkedOrderedIterator;

public class MagicRelation extends AbstractRelation<IMagicTuple> {

    protected static final Logger log = Logger.getLogger(MagicRelation.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    private final int arity;
    
    private final Set<String> indexNames;
    
    private transient MagicKeyOrder[] keyOrders;
    
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

            for (MagicKeyOrder keyOrder : keyOrders) {
            
                if (INFO) {
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

            final MagicKeyOrder[] keyOrders = getKeyOrders(); 
            
            for (MagicKeyOrder keyOrder : keyOrders) {
                
                if (INFO) {
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
    protected MagicKeyOrder[] getKeyOrders() {
        
        if (keyOrders == null) {
            
            keyOrders = MagicKeyOrderStrategy.calculateKeyOrders(arity);
            
        }
        
        return keyOrders;

    }
    
    protected IndexMetadata getMagicTupleIndexMetadata(
            final MagicKeyOrder keyOrder) {

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

    public long delete(IChunkedOrderedIterator<IMagicTuple> itr) {
        throw new UnsupportedOperationException("not implemented yet");
    }

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

    public long insert(final IMagicTuple[] tuples, final int numTuples) {

        if (tuples == null)
            throw new IllegalArgumentException();
        
        if (numTuples > tuples.length)
            throw new IllegalArgumentException();
        
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

        for (MagicKeyOrder keyOrder : getKeyOrders()) {
            
            tasks.add(new MagicIndexWriter(this, tuples, numTuples, 
                false/*clone*/, keyOrder, null/*filter*/, sortTime, insertTime, 
                mutationCount));
            
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

    public IAccessPath<IMagicTuple> getAccessPath(IPredicate<IMagicTuple> predicate) {
        // TODO Auto-generated method stub
        return null;
    }

    public Set<String> getIndexNames() {
        return indexNames;
    }

    public IMagicTuple newElement(IPredicate<IMagicTuple> predicate,
            IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final long[] terms = new long[arity];
        for (int i = 0; i < arity; i++) {
            terms[i] = asBound(predicate, i, bindingSet);
        }
        
        final MagicTuple magicTuple = new MagicTuple(terms);
        
        return magicTuple;
        
    }

    /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    @SuppressWarnings("unchecked")
    private long asBound(final IPredicate<IMagicTuple> predicate, 
            final int index, final IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = predicate.get(index);
        final IConstant<Long> c;
        if (t.isVar()) {
            c = bindingSet.get((IVariable) t);
        } else {
            c = (IConstant<Long>) t;
        }

        return c.get().longValue();

    }

}
