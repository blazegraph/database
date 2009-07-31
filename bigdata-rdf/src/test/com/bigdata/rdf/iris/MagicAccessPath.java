package com.bigdata.rdf.iris;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.striterator.IKeyOrder;

public class MagicAccessPath extends AbstractAccessPath<IMagicTuple> {
    
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
    
}
