package com.bigdata.service;

import java.util.UUID;

import com.bigdata.btree.IIndex;

/**
 * Interface to a bigdata federation.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBigdataFederation {

    /**
     * A constant that may be used as the transaction identifier when the
     * operation is <em>unisolated</em> (non-transactional).  The value of
     * this constant is ZERO (0L).
     */
    public static final long UNISOLATED = 0L;

    /**
     * Register a scale-out index with the federation.
     * 
     * @param name
     *            The index name.
     * 
     * @return The UUID for the scale-out index.
     */
    public UUID registerIndex(String name);
    
    /**
     * Create and statically partition a scale-out index.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param separatorKeys
     *            The array of separator keys. Each separator key is
     *            interpreted as an <em>unsigned byte[]</em>. The first
     *            entry MUST be an empty byte[]. The entries MUST be in
     *            sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition
     *            defined by a separator key will be mapped. The #of entries
     *            in this array MUST agree with the #of entries in the
     *            <i>separatorKeys</i> array.
     *            
     * @return The UUID of the scale-out index.
     */
    public UUID registerIndex(String name, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs);

    /**
     * Drop a scale-out index.
     * 
     * @param name
     *            The index name.
     */
    public void dropIndex(String name);
    
    /**
     * Obtain a view on a partitioned index.
     * 
     * @param tx
     *            The transaction identifier or zero(0L) iff the index will
     *            be unisolated.
     * 
     * @param name
     *            The index name.
     * 
     * @return The index or <code>null</code> if the index is not
     *         registered with the {@link MetadataService}.
     */
    public IIndex getIndex(long tx, String name);

}