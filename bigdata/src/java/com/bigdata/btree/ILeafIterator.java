package com.bigdata.btree;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Interface for an iterator that visits the leaves of a B+-Tree. The iterator
 * supports the concept of a <em>scan</em> order so that it may be used to
 * scan the leaves from last to first as well as methods that directly obtain
 * the previous or next leaf in the <em>key</em> order.
 * <p>
 * Note: a <em>foward</em> scan starts with the <em>first</em> leaf in the
 * key order and then proceeds in key order.
 * <p>
 * Note: a <em>reverse</em> scan starts with the <em>last</em> leaf in the
 * key order and then proceeds in reverse key order.
 * <p>
 * Note: The semantics of this interface differ from those of {@link Iterator},
 * which is why this interface does NOT extent {@link Iterator}. In particular,
 * the {@link #current()} leaf is ALWAYS defined. You can think of this has have
 * {@link ILeafIterator#next()} invoked before you receive the iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public interface ILeafIterator<T extends ILeafData> {

    /**
     * <code>true</code> if the scan is in ascending key order (left to
     * right).
     */
    public boolean isForwardScan();

    /**
     * <code>true</code> if the scan is in descending key order (right to
     * left).
     */
    public boolean isReverseScan();

    /**
     * Visits the next leaf in the key order regardless of the scan
     * direction.
     * <p>
     * Note: The {@link #current()} leaf is NOT modified if the iterator is
     * currently visiting the first leaf in the key order.
     * 
     * @return The next leaf in the key order -or- <code>null</code> iff
     *         the iterator is currently visiting the last leaf in the key
     *         order.
     */
    public T priorInKeyOrder();

    /**
     * Visits the next leaf in the key order regardless of the scan
     * direction.
     * <p>
     * Note: The {@link #current()} leaf is NOT modified if the iterator is
     * currently visiting the last leaf in the key order.
     * 
     * @return The next leaf in the key order -or- <code>null</code> iff
     *         the iterator is currently visiting the last leaf in the key
     *         order.
     */
    public T nextInKeyOrder();

    /**
     * <code>true</code> iff the next leaf in key order exists and is
     * allowed (by an optional <i>toKey</i> range constraint).
     */
    public boolean hasNextInKeyOrder();
    
    /**
     * <code>true</code> iff the previous leaf in key order exists and is
     * allowed (by an optional <i>fromKey</i> range constraint).
     */
    public boolean hasPriorInKeyOrder();
    
    /**
     * <code>true</code> if there is a leaf before the current leaf in 
     * the <em>scan</em> order.
     */
    public boolean hasPrior();

    /**
     * <code>true</code> if there is a leaf after the current leaf in the
     * <em>scan</em> order.
     */
    public boolean hasNext();

    /**
     * Visits the prior leaf in the <em>scan</em> order.
     */
    public T prior();

    /**
     * Visits the next leaf in the <em>scan</em> order.
     * 
     * @throws NoSuchElementException
     *             if there is no next leaf.
     */
    public T next();

    /**
     * Return the current leaf (always defined since there is always at
     * least one leaf).
     */
    public T current();
    
//    /**
//     * Return <code>true</code> iff there is another leaf to visit in the
//     * given scan direction.
//     * 
//     * @param forwardScan
//     *            The scan direction. When <code>true</code> the method
//     *            will return <code>true</code> iff there is another leaf
//     *            to scan in key order. When <code>false</code> the method
//     *            will return <code>true</code> iff there is another leaf
//     *            to scan in reverse key order.
//     *            
//     * @return If there is another leaf to scan in the indicated direction.
//     */

}
