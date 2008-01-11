package com.bigdata.service;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.rawstore.IRawStore;

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
     */
    public BTree newInstance(IRawStore store, UUID indexUUID);
    
}