package com.bigdata.rdf.iris;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

public class MagicAccessPath extends AbstractAccessPath<IMagicTuple> {
    
    private static transient final long NULL = IRawTripleStore.NULL;

    private MagicTupleSerializer tupleSer;
    
    /** Relation (resolved lazily if not specified to the ctor). */
    private MagicRelation relation;

    /**
     * Variant when the {@link SPORelation} has already been materialized.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the data service rather
     * than materializing the elements and then filtering them. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    @SuppressWarnings("unchecked")
    public MagicAccessPath(final MagicRelation relation,
            final IPredicate<IMagicTuple> predicate, final IKeyOrder<IMagicTuple> keyOrder,
            final IIndex ndx, final int flags, final int chunkOfChunksCapacity,
            final int chunkCapacity, final int fullyBufferedReadThreshold) {

        this(relation.getIndexManager(), relation.getTimestamp(), predicate,
                keyOrder, ndx, flags, chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);

        this.relation = relation;
        
    }

    /**
     * Variant does not require the {@link SPORelation} to have been
     * materialized. This is useful when you want an {@link IAccessPath} for a
     * specific index partition.
     * 
     * @param indexManager
     * @param timestamp
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     * @param chunkOfChunksCapacity
     * @param chunkCapacity
     * @param fullyBufferedReadThreshold
     */
    public MagicAccessPath(final IIndexManager indexManager,
            final long timestamp, final IPredicate<IMagicTuple> predicate,
            final IKeyOrder<IMagicTuple> keyOrder, final IIndex ndx, final int flags,
            final int chunkOfChunksCapacity, final int chunkCapacity,
            final int fullyBufferedReadThreshold) {

        super(indexManager, timestamp, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);
        
    }
    
    protected MagicTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (MagicTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
    public MagicAccessPath init() {

        final MagicTupleSerializer tupleSer = getTupleSerializer();
        
        /*
         * The minimum value that a term identifier may take on.
         */
        long MIN = Long.MIN_VALUE;
        
        final int arity = predicate.arity();
        
        final long[] fromTerms = new long[arity];
        
        final long[] toTerms = new long[arity];
        
        MagicKeyOrder keyOrder = (MagicKeyOrder) this.keyOrder;
        
        int[] keyMap = keyOrder.getKeyMap();
        
        boolean noneBound = true;
        
        for (int i = 0; i < arity; i++) {
            
            IVariableOrConstant<Long> term = predicate.get(keyMap[i]);
            
            if (term.isVar()) {
                
                fromTerms[i] = toTerms[i] = MIN;
                
            } else {
                
                fromTerms[i] = toTerms[i] = term.get();
                
                noneBound = false;
                
            }
            
        }
        
        for (int i = 0; i < arity; i++) {
            
            if (fromTerms[i] == MIN) {
                
                if (i > 0) {
                    
                    toTerms[i-1] = fromTerms[i-1] + 1;
                    
                    break;
                    
                }
                
            }
            
        }

        final byte[] fromKey = 
            noneBound ? null : tupleSer.magicTuple2Key(fromTerms);

        final byte[] toKey = 
            noneBound ? null : tupleSer.magicTuple2Key(toTerms);
        
        setFromKey(fromKey);

        setToKey(toKey);

        super.init();
    
        return this;
        
    }

    /**
     * Resolved lazily if not specified to the ctor.
     */
    synchronized
    public MagicRelation getRelation() {
        
        if (relation == null) {
            
            relation = (MagicRelation) indexManager.getResourceLocator().locate(
                    predicate.getOnlyRelationName(), timestamp);

        }

        return relation;
            
    }

}
