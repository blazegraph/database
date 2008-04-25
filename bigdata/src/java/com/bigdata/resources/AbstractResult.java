package com.bigdata.resources;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;

/**
 * Abstract base class for results when post-processing a named index
 * partition on the old journal after an overflow operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractResult {

    /**
     * The name under which the processed index partition was registered
     * (this is typically different from the name of the scale-out index).
     */
    public final String name;

    /**
     * The index metadata object for the processed index as of the timestamp
     * of the view from which the {@link IndexSegment} was generated.
     */
    public final IndexMetadata indexMetadata;

    /**
     * 
     * @param name
     *            The name under which the processed index partition was
     *            registered (this is typically different from the name of
     *            the scale-out index).
     * @param indexMetadata
     *            The index metadata object for the processed index as of
     *            the timestamp of the view from which the
     *            {@link IndexSegment} was generated.
     */
    public AbstractResult(String name, IndexMetadata indexMetadata) {

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
