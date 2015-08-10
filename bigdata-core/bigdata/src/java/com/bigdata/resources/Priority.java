package com.bigdata.resources;


/**
 * Class used to place {@link ViewMetadata} objects into a total order based on
 * a formula used to prioritize them for some kind of operation such as a
 * compacting merge.
 * <p>
 * <strong> The natural order of [Priority] is DESCENDING (largest numerical
 * value to smallest numerical value)</strong>
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

    /**
     * Places {@link Priority}s into <strong>descending</strong> order.
     */
    public int compareTo(final Priority<T> o) {

        return d < o.d ? 1 : d > o.d ? -1 : 0;
        
    }

//    /**
//     * Comparator places {@link Priority}s into ascending order (smallest
//     * numerical value to largest numerical value).
//     */
//    public Comparator<Priority<T>> asc() {
//        
//        return new Comparator<Priority<T>>() {
//
//            public int compare(Priority<T> t, Priority<T> o) {
//
//                return t.d < o.d ? -1 : t.d > o.d ? 1 : 0;
//                
//            }
//    
//        };
//        
//    }
//    
//    /**
//     * Comparator places {@link Priority}s into descending order (largest
//     * numerical value to smallest numerical value).
//     */
//    public Comparator<Priority<T>> dsc() {
//        
//        return new Comparator<Priority<T>>() {
//
//            public int compare(Priority<T> t, Priority<T> o) {
//
//                return t.d < o.d ? 1 : t.d > o.d ? -1 : 0;
//                
//            }
//    
//        };
//        
//    }
    
}
