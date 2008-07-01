package com.bigdata.relation.rdf;

import java.util.concurrent.ExecutorService;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IKeyOrder;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOAccessPath extends AbstractAccessPath<SPO> {

    // @todo IRawTripleStore when re-factored back to the rdf module.
    private transient final long NULL = 0L;

    private SPOTupleSerializer tupleSer;
    
    /**
     * Set by the ctor.
     */
    protected final long s, p, o;
    
    /**
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    public SPOAccessPath(ExecutorService service, IPredicate<SPO> predicate,
            IKeyOrder<SPO> keyOrder, IIndex ndx, int flags) {

        super(service, predicate, keyOrder, ndx, flags);

        {

            final IVariableOrConstant<Long> t = predicate.get(0);

            s = t.isVar() ? NULL : t.get();

        }

        {

            final IVariableOrConstant<Long> t = predicate.get(1);

            p = t.isVar() ? NULL : t.get();

        }
        
        {

            final IVariableOrConstant<Long> t = predicate.get(2);

            o = t.isVar() ? NULL : t.get();

        }

    }
    
    protected SPOTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
    public SPOAccessPath init() {

        final SPOTupleSerializer tupleSer = getTupleSerializer();
        
        final byte[] fromKey;
        final byte[] toKey;
        
        if (s != NULL && p != NULL && o != NULL) {
            
            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, p, o);

            toKey = tupleSer.statement2Key(s, p, o + 1);

        } else if (s != NULL && p != NULL) {

            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, p, NULL);

            toKey = tupleSer.statement2Key(s, p + 1, NULL);

        } else if (s != NULL && o != NULL) {

            assert keyOrder == SPOKeyOrder.OSP;
            
            fromKey = tupleSer.statement2Key(o, s, NULL);

            toKey = tupleSer.statement2Key(o, s + 1, NULL);

        } else if (p != NULL && o != NULL) {

            assert keyOrder == SPOKeyOrder.POS;
            
            fromKey = tupleSer.statement2Key(p, o, NULL);

            toKey = tupleSer.statement2Key(p, o + 1, NULL);

        } else if (s != NULL) {

            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, NULL, NULL);

            toKey = tupleSer.statement2Key(s + 1, NULL, NULL);

        } else if (p != NULL) {

            assert keyOrder == SPOKeyOrder.POS;
            
            fromKey = tupleSer.statement2Key(p, NULL, NULL);

            toKey = tupleSer.statement2Key(p + 1, NULL, NULL);

        } else if (o != NULL) {

            assert keyOrder == SPOKeyOrder.OSP;
            
            fromKey = tupleSer.statement2Key(o, NULL, NULL);

            toKey = tupleSer.statement2Key(o + 1, NULL, NULL);

        } else {

            /*
             * Note: The KeyOrder does not matter when you are fully
             * unbound.
             */
            
            fromKey = toKey = null;

        }

        setFromKey(fromKey);

        setToKey(toKey);

        super.init();
    
        return this;
        
    }
    
    /**
     * The implementation uses a key scan to find the first term identifer for
     * the given index. It then forms a fromKey that starts at the next possible
     * term identifier and does another scan, thereby obtaining the 2nd distinct
     * term identifier for that position on that index. This process is repeated
     * iteratively until the key scan no longer identifies a match. This
     * approach skips quickly over regions of the index which have many
     * statements for the same term and makes N+1 queries to identify N distinct
     * terms. Note that there is no way to pre-compute the #of distinct terms
     * that will be identified short of running the queries.
     * 
     * FIXME write a scale-out version.
     * <p>
     * This can be implemented using ICursor by seeking to the next possible key
     * after each SPO visited. This is relatively efficient and can scale-out
     * once the ICursor support is driven through the system. Another approach
     * is to write a parallelizable task that returns an Iterator and to have a
     * special aggregator. The aggregator would have to buffer the first result
     * from each index partition in order to ensure that the term identifiers
     * were not duplicated when the subject position cross (one or more) index
     * partition boundaries.
     * <p>
     * I am not sure how best to get this specialized scan folded into the rule
     * execution. E.g., as an ITupleCursor layered over the access path's
     * iterator, in which case the SPO access path will automatically do the
     * right thing but the rule should specify that filter as an
     * IPredicateConstraint. Or as a custom method on the SPOAccessPath.
     * 
     * @todo If the indices are range partitioned and the iterator guarentees
     *       "distinct" and even locally ordered, but not globally ordered, then
     *       those steps can be parallelized. The only possibility for conflict
     *       is when the last distinct term identifier is read from one index
     *       before the right sibling index partition has reported its first
     *       distinct term identifier. We could withhold the first result from
     *       each partition until the partition that proceeds it in the metadata
     *       index has completed, which would give nearly full parallelism.
     *       <p>
     *       If the indices are range partitioned and distinct + fully ordered
     *       is required, then the operation can not be parallelized, or if it
     *       is parallelized then a merge sort must be done before returning the
     *       first result.
     *       <p>
     *       Likewise, if the indices are hash partitioned, then we can do
     *       parallel index scans and a merge sort but the caller will have to
     *       wait for the merge sort to complete before obtaining the 1st
     *       result.
     * 
     * @todo unit tests (refactor from RDF DB).
     */
    public IChunkedOrderedIterator<Long> distinctTermScan() {

        final int capacity = 10000;
        
        byte[] fromKey = null;
        
        final byte[] toKey = null;
        
        ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, capacity,
                IRangeQuery.KEYS, null/* filter */);
        
        final SPOTupleSerializer tupleSer = getTupleSerializer();
        
        final IBlockingBuffer<Long> buffer = new BlockingBuffer<Long>(10000,
                null/*keyOrder*/);
        
        long nterms = 0L;
        
        while(itr.hasNext()) {
            
            ITuple tuple = itr.next();
            
            final long id = KeyBuilder.decodeLong( tuple.getKeyBuffer().array(), 0);
            
            // add to the buffer.
            buffer.add(id);

//            log.debug(ids.size() + " : " + id + " : "+ toString(id));
            
            // restart scan at the next possible term id.
            final long nextId = id + 1;
            
            fromKey = tupleSer.statement2Key(nextId, NULL, NULL);
            
            // new iterator.
            itr = ndx.rangeIterator(fromKey, toKey, capacity,
                    IRangeQuery.KEYS, null/* filter */);
         
            nterms++;
            
        }
      
        if (log.isDebugEnabled()) {

            log.debug("Distinct key scan: KeyOrder=" + keyOrder + ", #terms="
                    + nterms);

        }

        return buffer.iterator();

    }

}
