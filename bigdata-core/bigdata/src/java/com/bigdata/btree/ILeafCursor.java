package com.bigdata.btree;

/**
 * Leaf cursor interface.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILeafCursor<L extends Leaf> extends Cloneable {

    /**
     * The backing B+Tree.
     */
    AbstractBTree getBTree();
    
    /**
     * The current leaf (always defined).
     */
    L leaf();
    
    /**
     * Return the first leaf.
     */
    L first();

    /**
     * Return the last leaf.
     */
    L last();
    
    /**
     * Find the leaf that would span the key.
     * 
     * @param key
     *            The key
     * 
     * @return The leaf which would span that key and never <code>null</code>.
     * 
     * @throws IllegalArgumentException
     *             if the <i>key</i> is <code>null</code>.
     */
    L seek(byte[] key);

    /**
     * Position this cursor on the same leaf as the given cursor.
     * 
     * @param src
     *            A cursor.
     *            
     * @return The leaf on which the given cursor was positioned.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the argument is a cursor for a different
     *             {@link AbstractBTree}.
     */
    L seek(ILeafCursor<L> src);
    
    /**
     * Clone the cursor.
     */
    ILeafCursor<L> clone();
    
    /**
     * Return the previous leaf in the natural order of the B+Tree. The
     * cursor position is unchanged if there is no prececessor.
     * 
     * @return The previous leaf -or- <code>null</code> iff there is no
     *         previous leaf.
     */
    L prior();
    
    /**
     * Return the next leaf in the natural order of the B+Tree. The cursor
     * position is unchanged if there is no successor.
     * 
     * @return The next leaf -or- <code>null</code> iff there is no next
     *         leaf.
     */
    L next();
    
}