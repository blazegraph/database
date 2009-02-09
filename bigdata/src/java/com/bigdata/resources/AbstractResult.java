package com.bigdata.resources;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.service.DataService;

/**
 * Abstract base class for results when post-processing a named index
 * partition on the old journal after an overflow operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractResult {

    /**
     * The source index partition for the operation.
     * 
     * @see DataService#getIndexPartitionName(String, int)
     */
    public final String name;

    /**
     * The index metadata object for the source index partition.
     */
    public final IndexMetadata indexMetadata;

    /**
     * 
     * @param name
     *            The name of the source index partition.
     * @param indexMetadata
     *            The index metadata object for the source index partition.
     */
    public AbstractResult(final String name, final IndexMetadata indexMetadata) {

        if (name == null)
            throw new IllegalArgumentException();

        if (indexMetadata == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.indexMetadata = indexMetadata;

    }
    
    public String toString() {
        
        return super.toString()+"{name="+name+"}";
        
    }

}
