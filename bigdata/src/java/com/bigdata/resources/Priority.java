package com.bigdata.resources;

/**
 * Class used to place {@link ViewMetadata} objects into a total order based
 * on a formula used to prioritize them for some kind of operation such as a
 * compacting merge.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
class Priority<T> implements Comparable<Priority<T>> {

    public final double d;

    public final T v;

    public Priority(final double d, final T v) {

        this.d = d;

        this.v = v;
        
    }
    
    public int compareTo(final Priority<T> o) {

        return d < o.d ? -1 : d > o.d ? 1 : 0;
        
    }
    
}
