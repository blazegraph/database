package com.bigdata.sparse;

import com.bigdata.btree.IReadOnlyOperation;

/**
 * Atomic read of the logical row associated with some {@link Schema} and
 * primary key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRowRead extends AbstractAtomicRowReadOrWrite implements
        IReadOnlyOperation {

    /**
     * 
     */
    private static final long serialVersionUID = 7240920229720302721L;

    /**
     * De-serialization ctor.
     */
    public AtomicRowRead() {
        
    }
    
    /**
     * Constructor for an atomic read operation.
     * 
     * @param schema
     *            The schema governing the property set.
     * @param primaryKey
     *            The value of the primary key (identifies the logical row
     *            to be read).
     * @param timestamp
     *            A timestamp to obtain the value for the named property
     *            whose timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most
     *            recent value for the property.
     * @param filter
     *            An optional filter used to restrict the property values
     *            that will be returned.
     */
    public AtomicRowRead(Schema schema, Object primaryKey, long timestamp,
            INameFilter filter) {
        
        super(schema, primaryKey, timestamp, filter);
        
    }
    
}
