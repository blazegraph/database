package com.bigdata.util;

import java.io.Serializable;

import com.bigdata.btree.IIndex;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.relation.IRelation;

/**
 * The name of an {@link IIndex} or an {@link IRelation} and a timestamp. This
 * is used as a key for a {@link WeakValueCache} to provide a canonicalizing
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

    public String getName() {
        
        return name;
        
    }
    
    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    /**
     * 
     * @param name
     *            The name of an {@link IIndex} or an {@link IRelation}.
     * @param timestamp
     *            The timestamp associated with the view.
     */
    public NT(String name, long timestamp) {

        if (name == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.timestamp = timestamp;

        this.hashCode = name.hashCode() << 32 + (Long.valueOf(timestamp)
                .hashCode() >>> 32);

    }

    public int hashCode() {

        return hashCode;

    }

    public boolean equals(Object o) {

        return equals((NT) o);

    }

    public boolean equals(NT o) {

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

        if (!this.name.equals(o.name))
            return false;

        if (this.timestamp != o.timestamp)
            return false;

        return true;

    }

    public String toString() {

        return "NT{name=" + name + ",timestamp=" + timestamp + "}";

    }

}
