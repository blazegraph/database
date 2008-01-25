package com.bigdata.btree;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.IDataService;

/**
 * An interface that knows how to create an instance of an index when it
 * is registered on an {@link IDataService}.
 * 
 * @see IDataService#registerIndex(String, UUID, IIndexConstructor)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexConstructor extends Serializable {
    
    /**
     * Create an instance of a class that extends {@link BTree}.
     * 
     * @param store
     *            The backing store on which the index will be created.
     * @param indexUUID
     *            The UUID to be assigned to the index.
     * @param pmd
     *            The partition metadata (optional, required only for
     *            partitioned indices).
     */
    public BTree newInstance(IRawStore store, UUID indexUUID, IPartitionMetadata pmd);
    
}