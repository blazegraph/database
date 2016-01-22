package com.bigdata.sparse;

/**
 * a Timestamped Property Value is a single {property, timestamp, value} tuple
 * for some schema as read from the {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITPV {

    /**
     * The {@link Schema}.
     */
    public Schema getSchema();
    
    /**
     * The timestamp.
     */
    public long getTimestamp();

    /**
     * The property name.
     */
    public String getName();

    /**
     * The property value.
     * 
     * @return The value of the property as of the indicated timestamp -or-
     *         <code>null</code> iff the property was NOT bound as of that
     *         timestamp (i.e., either a deleted property value or a property
     *         that was never bound).
     */
    public Object getValue();
    
}