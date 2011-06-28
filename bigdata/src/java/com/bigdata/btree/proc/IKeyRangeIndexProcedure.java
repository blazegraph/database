package com.bigdata.btree.proc;

/**
 * Interface for procedures that are mapped across one or more index
 * partitions based on a key range (fromKey, toKey).  The keys are
 * interpreted as variable length unsigned byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyRangeIndexProcedure extends IIndexProcedure {
    
    /**
     * Return the lowest key that will be visited (inclusive). When
     * <code>null</code> there is no lower bound.
     */
    public byte[] getFromKey();

    /**
     * Return the first key that will not be visited (exclusive). When
     * <code>null</code> there is no upper bound.
     */
    public byte[] getToKey();

}