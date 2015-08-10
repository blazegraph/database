package com.bigdata.btree.keys;

import java.util.Locale;

/**
 * Interface allows us to encapsulate differences between the ICU and JDK
 * libraries for generating sort keys from Unicode strings.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface UnicodeSortKeyGenerator {
    
    /**
     * The {@link Locale} used to configure this object.
     */
    public Locale getLocale();

    /**
     * Append a Unicode sort key to the {@link KeyBuilder}.
     * 
     * @param keyBuilder
     *            The {@link KeyBuilder}.
     * @param s
     *            The Unicode string.
     */
    public void appendSortKey(KeyBuilder keyBuilder, String s);
    
}