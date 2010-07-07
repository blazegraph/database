package com.bigdata.rdf.model;

import java.util.Comparator;
import com.bigdata.rdf.internal.IV;

/**
 * Places {@link BigdataValue}s into an ordering determined by their assigned
 * {@link BigdataValue#getIV() term identifiers}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataValue#getIV()
 */
public class TermIdComparator implements Comparator<BigdataValue> {

    public static final transient Comparator<BigdataValue> INSTANCE =
        new TermIdComparator();

    /**
     * Note: comparison avoids possible overflow of <code>long</code> by
     * not computing the difference directly.
     */
    public int compare(BigdataValue term1, BigdataValue term2) {

        final IV id1 = term1.getIV();
        final IV id2 = term2.getIV();
        
        return id1.compareTo(id2);

    }

}