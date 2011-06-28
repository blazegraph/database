package com.bigdata.btree;

/**
 * Typesafe enum used to indicate that an {@link ILeafCursor} should
 * seek to the first or last leaf in the B+Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public enum SeekEnum {

    /** Seek to the first leaf. */
    First,
    
    /** Seek to the last leaf. */
    Last;
    
}