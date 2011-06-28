package com.bigdata.sparse;

import com.bigdata.btree.IIndex;


/**
 * Atomic read of the logical row associated with some {@link Schema} and
 * primary key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRowRead extends AbstractAtomicRowReadOrWrite {

    /**
     * 
     */
    private static final long serialVersionUID = 7240920229720302721L;

    public final boolean isReadOnly() {
        
        return true;
        
    }
        
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
     * @param fromTime
     *            <em>During pre-condition and post-condition reads</em>, the
     *            first timestamp for which timestamped property values will be
     *            accepted.
     * @param toTime
     *            <em>During pre-condition and post-condition reads</em>, the
     *            first timestamp for which timestamped property values will NOT
     *            be accepted -or- {@link IRowStoreConstants#CURRENT_ROW} to
     *            accept only the most current binding whose timestamp is GTE
     *            <i>fromTime</i>.
     * @param filter
     *            An optional filter used to restrict the property values
     *            that will be returned.
     */
    public AtomicRowRead(final Schema schema, final Object primaryKey,
            final long fromTime, final long toTime, final INameFilter filter) {
        
        super(schema, primaryKey, fromTime, toTime, filter);
        
    }
    
    /**
     * Atomic read.
     * 
     * @return A {@link TPS} instance containing the selected data from the
     *         logical row identified by the {@link #primaryKey} -or-
     *         <code>null</code> iff the primary key was NOT FOUND in the
     *         index. I.e., iff there are NO entries for that primary key
     *         regardless of whether or not they were selected.
     */
    public TPS apply(final IIndex ndx) {
    
        return atomicRead(ndx, schema, primaryKey, fromTime, toTime,
                0L/* writeTime */, filter);
        
    }

}
