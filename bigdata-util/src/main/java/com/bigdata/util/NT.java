package com.bigdata.util;

import java.io.Serializable;

/**
 * The name of an {@link com.bigdata.btree.IIndex} or an {@link com.bigdata.relation.IRelation} and a timestamp. This
 * is used as a key for a {@link com.bigdata.cache.WeakValueCache} to provide a canonicalizing
 * mapping for index views or relation views.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NT implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -2447755655295770390L;

    private final String name;

    private final long timestamp;

    private final int hashCode;

    final public String getName() {
        
        return name;
        
    }
    
    final public long getTimestamp() {
        
        return timestamp;
        
    }
    
    /**
     * 
     * @param name
     *            The name of an {@link com.bigdata.btree.IIndex} or an {@link com.bigdata.relation.IRelation}.
     * @param timestamp
     *            The timestamp associated with the view.
     */
    public NT(final String name, final long timestamp) {

        if (name == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.timestamp = timestamp;

        this.hashCode = name.hashCode() << 32 + (Long.valueOf(timestamp)
                .hashCode() >>> 32);

    }

    @Override
    public int hashCode() {

        return hashCode;

    }

    @Override
    public boolean equals(final Object o) {

        return equals((NT) o);

    }

    public boolean equals(final NT o) {

        if (o == null) {

            /*
             * Note: This handles a case where the other instance was a key in a
             * WeakHashMap and the reference for the key was cleared. This
             * arises with the NamedLock class.
             */
            
            return false;
            
        }
        
        if (this == o)
            return true;

        if (this.timestamp != o.timestamp)
            return false;

        if (!this.name.equals(o.name))
            return false;

        return true;

    }

    @Override
    public String toString() {

        return "NT{name=" + name + ",timestamp=" + timestamp + "}";

    }

}
