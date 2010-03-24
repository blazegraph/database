package com.bigdata.rdf.model;

import java.util.Comparator;

/**
 * Compares {@link Long}s used to represent term identifiers, placing them
 * into the same order as the <code>id:terms</code> index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermIdComparator2 implements Comparator<Long> {

    public static final transient Comparator<Long> INSTANCE =
        new TermIdComparator2();

    /**
     * Note: comparison avoids possible overflow of <code>long</code> by
     * not computing the difference directly.
     */
    public int compare(final Long term1, final Long term2) {

        final long id1 = term1.longValue();
        final long id2 = term2.longValue();
        
        if(id1 < id2) return -1;
        if(id1 > id2) return 1;
        return 0;

    }

}