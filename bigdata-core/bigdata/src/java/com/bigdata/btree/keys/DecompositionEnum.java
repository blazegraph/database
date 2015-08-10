package com.bigdata.btree.keys;

import java.text.Collator;

/**
 * Type safe enumeration for the decomposition mode.
 * <p>
 * Note: ICU and the JDK use different integer constants for the
 * decomposition modes!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum DecompositionEnum {
    
    /**
     * See {@link Collator#NO_DECOMPOSITION}.
     */
    None,
    /**
     * See {@link Collator#FULL_DECOMPOSITION}.
     */
    Full,
    /**
     * See {@link Collator#CANONICAL_DECOMPOSITION}.
     */
    Canonical;
    
}