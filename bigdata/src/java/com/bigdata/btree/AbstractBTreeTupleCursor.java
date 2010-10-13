/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Jun 9, 2008
 */

package com.bigdata.btree;

import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.Leaf.ILeafListener;
import com.bigdata.btree.isolation.IsolatedFusedView;
import com.bigdata.btree.view.FusedView;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * Class supporting random access to tuples and sequential tuple-based cursor
 * movement for an {@link AbstractBTree}.
 * <p>
 * The tuple position is defined in terms of the current key on which the tuple
 * "rests". If there is no tuple associated with that key in the index then you
 * will not be able to read the value or optional metadata (delete markers or
 * version timestamps) for the key. If the key is associated with a deleted
 * tuple then you can not read the value associated with the key, but you can
 * read the (optional) delete marker and version metadata if the cursor was
 * provisioned to visit deleted tuples.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeTupleCursor<I extends AbstractBTree, L extends Leaf, E>
        implements ITupleCursor2<E> {

    protected static final Logger log = Logger
            .getLogger(AbstractBTreeTupleCursor.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /*
     * Some frequently used log messages.
     */
    
    private static transient final String LOG_NO_CURSOR_POSITION = "No cursor position";

    private static transient final String LOG_NO_SUCCESSOR = "No successor";

    private static transient final String LOG_NO_PREDECESSOR = "No predecessor";

    private static transient final String LOG_CURSOR_POSITION_NOT_VISITABLE = "Cursor position is not visitable";
    
    /** From the ctor. */
    protected final I btree;
    
    /** From the ctor. */
    protected final Tuple<E> tuple;

    /** from the ctor. */
    protected final byte[] fromKey;
    
    /** from the ctor. */
    protected final byte[] toKey;

    /** true iff the cursor was provisioned to visit deleted tuples. */
    final protected boolean visitDeleted;

    final public I getIndex() {

        return btree;

    }

    /** From the ctor. */
    final public byte[] getFromKey() {
        
        return fromKey;
        
    }

    /** From the ctor. */
    final public byte[] getToKey() {
        
        return toKey;
        
    }
    
    final public boolean isDeletedTupleVisitor() {
        
        return visitDeleted;
        
    }
    
    /**
     * The current cursor position (initially <code>null</code>).
     */
    protected AbstractCursorPosition<L,E> currentPosition;

//    /**
//     * A temporary cursor position used by {@link #hasNext()} and
//     * {@link #hasPrior()}.
//     */
//    private AbstractCursorPosition<L,E> tempPosition;
    
    final public boolean isCursorPositionDefined() {

        return currentPosition != null;
        
    }

    /**
     * The cursor position is undefined until {@link #first(boolean)},
     * {@link #last(boolean)}, or {@link #seek(byte[])} is used to position the
     * cursor.
     * 
     * @throws IllegalStateException
     *             if the cursor position is not defined.
     */
    final protected void assertCursorPositionDefined() {

        if (!isCursorPositionDefined())
            throw new IllegalStateException();

    }

    /**
     * Create a new cursor.
     * 
     * @param btree
     *            The B+Tree whose tuples are visited by this cursor.
     * @param tuple
     *            The tuple into which the data will be copied.
     *            {@link IRangeQuery#KEYS} MUST be specified for that
     *            {@link Tuple}. The keys of the index will always be copied
     *            into the {@link Tuple}, but the <i>flags</i> on the
     *            {@link Tuple} will determine whether the values paired to the
     *            key will be copied and whether or not deleted tuples will be
     *            visited.
     * @param fromKey
     *            The optional inclusive lower bound.
     * @param toKey
     *            The optional exclusive upper bound.
     * 
     * @throws IllegalArgumentException
     *             if the <i>btree</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>tuple</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>tuple</i> is not associated with that <i>btree</i>.
     * @throws IllegalArgumentException
     *             if the <i>fromKey</i> is GTE the <i>toKey</i>.
     */
    public AbstractBTreeTupleCursor(final I btree, final Tuple<E> tuple,
            final byte[] fromKey, final byte[] toKey) {

        if (btree == null)
            throw new IllegalArgumentException();

        if (tuple == null)
            throw new IllegalArgumentException();

//        if (tuple.getBTree() != btree)
//            throw new IllegalArgumentException();
        
        if (fromKey != null) {

            btree.rangeCheck(fromKey, false/* allowUpperBound */);

        }

        if (toKey != null) {

            btree.rangeCheck(toKey, true/* allowUpperBound */);
            
        }

        if (fromKey != null && toKey != null) {

            if (BytesUtil.compareBytes(fromKey, toKey) > 0) {

                throw new IllegalArgumentException(
                        "toKey LT fromKey: fromKey="
                        + BytesUtil.toString(fromKey) + ", toKey="
                        + BytesUtil.toString(toKey));
                
            }
            
        }
        
        this.btree = btree;
        
        this.tuple = tuple;
        
        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
        this.visitDeleted = ((tuple.flags() & IRangeQuery.DELETED) != 0);

        // Note: the cursor position is NOT defined!
        currentPosition = null;
//        nextPosition = priorPosition = null;

//        /*
//         * Note: This is used to minimize the costs associated with testing for
//         * a prior/next visitable tuple. It is initially set to the first
//         * position, but this is an arbitrary choice. It's state is updated from
//         * the [currentPosition] each time before it is used.
//         */ 
//        tempPosition = firstPosition();
        
    }

    public String toString() {
        
        return "Cursor{fromKey=" + BytesUtil.toString(fromKey) + ", toKey="
                + BytesUtil.toString(toKey) + ", currentKey="
                + BytesUtil.toString(currentKey()) + ", visitDeleted="
                + visitDeleted + "}";
        
    }
    
    /**
     * Return <code>true</code> if the <i>key</i> lies inside of the optional
     * half-open range constraint.
     * 
     * @return <code>true</code> unless the <i>key</i> is LT [fromKey] or GTE
     *         [toKey].
     */
    final protected boolean rangeCheck(final byte[] key) {

    	return BytesUtil.rangeCheck(key, fromKey, toKey);
    	
//        if (fromKey == null && toKey == null) {
//
//            // no range constraint.
//            return true;
//            
//        }
//
//        if (fromKey != null) {
//
//            if (BytesUtil.compareBytes(key, fromKey) < 0) {
//
//                if (DEBUG) {
//
//                    log.debug("key=" + BytesUtil.toString(key) + " LT fromKey"
//                            + BytesUtil.toString(fromKey));
//
//                }
//                
//                // key is LT then the optional inclusive lower bound.
//                return false;
//
//            }
//
//        }
//
//        if (toKey != null) {
//
//            if (BytesUtil.compareBytes(key, toKey) >= 0) {
//
//                if (DEBUG) {
//
//                    log.debug("key=" + BytesUtil.toString(key) + " GTE toKey"
//                            + BytesUtil.toString(toKey));
//
//                }
//
//                // key is GTE the optional exclusive upper bound
//                return false;
//
//            }
//
//        }
//
//        return true;
        
    }

    /**
     * The optional inclusive lower bound. If the <i>fromKey</i> constraint was
     * specified when the {@link ITupleCursor} was created, then that value is
     * returned. Otherwise, if the index is an index partition then the
     * {@link LocalPartitionMetadata#getLeftSeparatorKey()} is returned.
     * Finally, <code>null</code> is returned if there is no inclusive lower
     * bound.
     */
    final protected byte[] getInclusiveLowerBound() {
        
        final byte[] fromKey;

        if (this.fromKey != null) {

            fromKey = this.fromKey;

        } else {

            IndexMetadata md = getIndex().getIndexMetadata();

            LocalPartitionMetadata pmd = md.getPartitionMetadata();

            if (pmd != null) {

                fromKey = pmd.getLeftSeparatorKey();

            } else {

                fromKey = null;

            }

        }
        
        return fromKey;
        
    }
    
    /**
     * The optional exclusive upper bound. If the <i>toKey</i> constraint was
     * specified when the {@link ITupleCursor} was created, then that value is
     * returned. Otherwise, if the index is an index partition then the
     * {@link LocalPartitionMetadata#getRightSeparatorKey()} is returned.
     * Finally, <code>null</code> is returned if there is no exclusive upper
     * bound.
     */
    final protected byte[] getExclusiveUpperBound() {
        
        final byte[] toKey;
        
        if(this.toKey!=null) {
            
            toKey = this.toKey;
            
        } else {
            
            IndexMetadata md = getIndex().getIndexMetadata();

            LocalPartitionMetadata pmd = md.getPartitionMetadata();

            if (pmd != null) {

                toKey = pmd.getRightSeparatorKey();

            } else {

                /*
                 * Note: unlike the inclusive lower bound, the exclusive upper
                 * bound can not be defined in the absence of explicit
                 * constraints since we allow variable length keys.
                 */
                toKey = null;

            }
            
        }
        
        return toKey;
        
    }

    /**
     * Return a new {@link ICursorPosition} from the <i>leafCursor</i>, tuple
     * <i>index</i>, and <i>key</i>
     * 
     * @param leafCursor
     *            The {@link ILeafCursor} (already positioned on the desired
     *            leaf).
     * @param index
     *            The index of the tuple corresponding to the <i>key</i> within
     *            the current leaf of the <i>leafCursor</i> -or- a negative
     *            integer representing the insertion point for the <i>key</i>
     *            if the <i>key</i> is spanned by that leaf but there is no
     *            tuple for that <i>key</i> in the <i>leaf</i>.
     * @param key
     *            The key.
     * 
     * @return The new {@link ICursorPosition}.
     * 
     * @throws IllegalArgumentException
     *             if <i>leafCursor</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>key</i> is <code>null</code>.
     */
    abstract protected AbstractCursorPosition<L, E> newPosition(ILeafCursor<L> leafCursor,
            int index, byte[] key);

    /**
     * Return a clone of the given {@link ICursorPosition} designed for use by
     * {@link #hasNext()} and {@link #hasPrior()} (temporary test without
     * side-effects).
     * 
     * @param p
     *            The cursor position.
     * 
     * @return A clone of that cursor position.
     * 
     * @deprecated This is never used.
     */
    abstract protected AbstractCursorPosition<L, E> newTemporaryPosition(ICursorPosition<L, E> p);
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on the
     * inclusive lower bound and on <code>new byte[]{}</code> if there is no
     * inclusive lower bound.
     */
    @SuppressWarnings("unchecked")
    final protected AbstractCursorPosition<L,E> firstPosition() {

        byte[] key = getInclusiveLowerBound();

        if (key == null) {

            key = BytesUtil.EMPTY;

        }
        
        final ILeafCursor<L> leafCursor = btree.newLeafCursor(key);
        
        final int index = leafCursor.leaf().getKeys().search(key);
        
        return newPosition(leafCursor, index, key);
        
    }
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on the
     * given <i>key</i>.
     * 
     * @param leaf
     *            A leaf (required).
     * @param key
     *            A key that is spanned by that leaf (required, but there is no
     *            requirement that a tuple corresponding to that key is present
     *            in the leaf).
     * 
     * @return The new {@link ICursorPosition}.
     * 
     * @throws IllegalArgumentException
     *             if the key is <code>null</code>.
     * @throws KeyOutOfRangeException
     *             if the key lies outside of the optional constrain on the
     *             {@link ITupleCursor}.
     */
    @SuppressWarnings("unchecked")
    final protected AbstractCursorPosition<L,E> newPosition(final byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();

        if (!rangeCheck(key))
            throw new KeyOutOfRangeException("key=" + BytesUtil.toString(key)
                    + ", fromKey=" + BytesUtil.toString(fromKey) + ", toKey="
                    + BytesUtil.toString(toKey));

        final ILeafCursor<L> leafCursor = btree.newLeafCursor(key);

        final int index = leafCursor.leaf().getKeys().search(key);

        return newPosition(leafCursor, index, key);
        
    }
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on the
     * last tuple in the key-range (does not skip over deleted tuples).
     * <p>
     * Note: If there is no exclusive upper bound and there are no tuples in the
     * index then the position is set to <code>new byte[]{}</code> (the same
     * behavior as first for an empty index).
     * 
     * @return The leaf spanning {@link #getExclusiveUpperBound()}.
     */
    @SuppressWarnings("unchecked")
    final protected AbstractCursorPosition<L,E> lastPosition() {

        byte[] key = getExclusiveUpperBound();
        
        final ILeafCursor<L> leafCursor;
        
        int index;
        
        if (key == null) {

            /*
             * Since there is no exclusive upper bound, use the last leaf in the
             * B+Tree.
             */
            
            leafCursor = btree.newLeafCursor(SeekEnum.Last);
            
            final L leaf = leafCursor.leaf();
            
            /*
             * Use the last key in the leaf.
             */

            index = leaf.getKeyCount() - 1;
            
            if (index < 0) {

                /*
                 * This is an insertion point which means that the leaf was
                 * empty. This case only occurs for an empty root leaf.
                 */
                
                key = BytesUtil.EMPTY;

            } else {

                /*
                 * Lookup the key in the leaf at that index.
                 */
                
                key = leaf.getKeys().get(index);
                
            }
            
        } else {

            /*
             * Since there is an exclusive upper bound, lookup the leaf spanning
             * that key.
             */
            
            leafCursor = btree.newLeafCursor(key);
            
            L leaf = leafCursor.leaf();
            
            /*
             * Find the position (or the insertion point) of the key in the
             * leaf.
             */

            index = leaf.getKeys().search(key);

            if (index == 0 || index == -1) {

                /*
                 * Either the key exists in at index ZERO (0) in the leaf -or-
                 * the insertion point is -1, which means that the key would 
                 * be inserted at index ZERO (0) in
                 * the leaf. Either way, we want to start the cursor at the
                 * last tuple in the previous leaf.
                 */

                // Note: Will be null if there is no prior leaf.
                leaf = leafCursor.prior();

                // Start at the last tuple in the prior leaf.
                index = leaf == null ? 0 : leaf.getKeyCount() - 1;

            } else {


                if (index > 0) {

                    /*
                     * The key exists in the leaf at some position GT the first
                     * index in the leaf. Since the toKey is an exclusive upper
                     * bound, we subtract one to start the cursor at the prior
                     * tuple in the leaf.
                     * 
                     * Note: The case where index == 0 was handled above.
                     */

                    index--;
                    
                } else {

                    /*
                     * Since index is an insertion point, it is one index beyond
                     * the last tuple that we should visit. Therefore we add one
                     * to the insertion point, which has the effect of shifting
                     * the index position down by one in the leaf.
                     * 
                     * Note: The case where index == -1 was handled above.
                     * Therefore index++ can not turn the insertion point into a
                     * valid index.
                     */

                    index++;

                }

            }

        }

        return newPosition(leafCursor, index, key);

    }

    public byte[] currentKey() {
        
        if(!isCursorPositionDefined()) {

            if (DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);

            // the cursor position is not defined.
            return null;
            
        }
        
        return currentPosition.getKey();
        
    }
    
    public ITuple<E> tuple() {
        
        if(!isCursorPositionDefined()) {
         
            if (DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);
            
            // the cursor position is not defined.
            return null;
            
        }

        /*
         * Copy out the state of the current tuple if the cursor position is on
         * a tuple and if that tuple is visitable.
         */
        
        return currentPosition.get(tuple);
        
    }
    
    public ITuple<E> first() {

//        // clear references since no longer valid.
//        nextPosition = priorPosition = null;
        
        // new position on the inclusive lower bound.
        currentPosition = firstPosition();

        // Scan until we reach the first visitable tuple.
        if (!currentPosition.forwardScan(false/*skipCurrent*/,false/*testOnly*/)) {
        
            // discard the current position since we have scanned beyond the end of the index / key range.
            currentPosition = null;
            
            if (DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);

            // Nothing visitable.
            return null;
            
        }

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);
        
    }
    
    public ITuple<E> last() {
        
//        // clear references since no longer valid.
//        nextPosition = priorPosition = null;

        // new position after all defined keys.
        currentPosition = lastPosition();
        
        // Scan backwards until we reach the first visitable tuple.
        if(!currentPosition.reverseScan(false/*skipCurrent*/,false/*testOnly*/)) {
        
            // discard the current position since we have scanned beyond the start of the index / key range.
            currentPosition = null;
            
            if (DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);

            // Nothing visitable.
            return null;
            
        }

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);
        
    }

    final public ITuple<E> seek(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        return seek(getIndex().getIndexMetadata().getTupleSerializer()
                .serializeKey(key));
        
    }
    
    public ITuple<E> seek(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        if (DEBUG)
            log.debug("key="+BytesUtil.toString(key));
        
//        // clear references since no longer valid.
//        nextPosition = priorPosition = null;

        // new position is that key.
        currentPosition = newPosition(key);

        // Copy the data into [tuple].
        return currentPosition.get(tuple);
        
    }

//    /**
//     * Scan to the next cursor position having a visitable tuple.
//     * 
//     * @param pos
//     *            A cursor position (required).
//     * @param skipCurrent
//     *            true if the tuple at the current cursor position should be
//     *            skipped over.
//     *            
//     * @return The next cursor position -or- <code>null</code> if there is no
//     *         next cursor position having a visitable tuple.
//     */
//    protected AbstractCursorPosition<L, E> nextPosition(
//            AbstractCursorPosition<L, E> pos, boolean skipCurrent) {
//
//        assert pos != null;
//
//        if (!pos.forwardScan(skipCurrent)) {
//
//            // no visitable predecessor.
//            if (DEBUG)
//                log.debug(LOG_NO_PREDECESSOR);
//
//            return null;
//
//        }
//
//        return pos;
//
//    }
//    
//    /**
//     * Scan to the prior cursor position having a visitable tuple.
//     * 
//     * @param pos
//     *            A cursor position (required).
//     * @param skipCurrent
//     *            true if the tuple at the current cursor position should be
//     *            skipped over.
//     *            
//     * @return The prior cursor position -or- <code>null</code> if there is no
//     *         prior cursor position having a visitable tuple.
//     */
//    protected AbstractCursorPosition<L, E> priorPosition(
//            AbstractCursorPosition<L, E> pos, boolean skipCurrent) {
//
//        assert pos != null;
//
//        if (!pos.reverseScan(skipCurrent)) {
//
//            // no visitable predecessor.
//            if (DEBUG)
//                log.debug(LOG_NO_PREDECESSOR);
//
//            return null;
//
//        }
//
//        return pos;
//
//    }
    
    /**
     * Note: This is lighter weight than {@link #hasNext()} and {@link #next()}
     * since it does not need to scan to verify that the next position exists
     * before visiting that tuple.
     */
    public ITuple<E> nextTuple() {
        
        if(!isCursorPositionDefined()) {
            
            // Cursor position is not defined.
            
            if(DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);
            
            return null;
            
        }
        
        if(!currentPosition.forwardScan(true/*skipCurrent*/,false/*testOnly*/)) {

//            // no visitable successor.
//            currentPosition = null;
            
            if (DEBUG)
                log.debug(LOG_NO_SUCCESSOR);

            return null;
            
        }
        
        return currentPosition.get(tuple);
        
    }
    
    public ITuple<E> next() {

        final ITuple<E> t;
        
        if (!isCursorPositionDefined()) {

            t = first();
            
        } else {
            
            t = nextTuple();
            
        }
        
        if( t == null) {
            
            throw new NoSuchElementException();
            
        }
        
        return t;
        
//        if (!hasNext())
//            throw new NoSuchElementException();
//
//        // update the current position.
//        currentPosition = nextPosition;
//
//        // clear references since no longer valid.
//        nextPosition = priorPosition = null;
//
//        // Copy the data into [tuple] and return [tuple].
//        return currentPosition.get(tuple);
        
    }

    public boolean hasNext() {
        
//        // the next position is already known.
//        if (nextPosition != null) return true;

        final boolean skipCurrent;
        
        final AbstractCursorPosition<L,E> pos;
        
        if (!isCursorPositionDefined()) {
            
            /*
             * Note: In order to have the standard iterator semantics we
             * start a new scan if the cursor position is undefined.
             */
            pos = firstPosition();

            /*
             * Don't skip the first position if it is visitable.
             */
            skipCurrent = false;
        
        } else {

            /*
             * The cursor position was already defined we will skip the current
             * tuple when scanning forward to the next visitable tuple.
             */
            skipCurrent = true;

//            if (tempPosition == null) {
//                
//                pos = tempPosition = newTemporaryPosition(currentPosition);
//            
//            } else {
//
//                pos = tempPosition;
//
//                pos.seek(currentPosition);
//
//            }

//            pos = newTemporaryPosition(currentPosition);
            
            pos = currentPosition;
            
        }
        
        if (!pos.forwardScan(skipCurrent,true/*testOnly*/)) {

            if (DEBUG)
                log.debug(LOG_NO_SUCCESSOR);

            // nothing visitable.
            return false;

        }

        if (DEBUG)
            log.debug(pos.toString());

        // found something visitable.
        return true;
        
    }

    /**
     * Note: This is lighter weight than {@link #hasNext()} and {@link #next()}
     * since it does not need to scan to verify that the prior position exists
     * before visiting that tuple.
     */
    public ITuple<E> priorTuple() {
        
        if(!isCursorPositionDefined()) {
            
            // Cursor position is not defined.
            
            if(DEBUG)
                log.debug(LOG_NO_CURSOR_POSITION);

            return null;
            
        }
        
        if(!currentPosition.reverseScan(true/*skipCurrent*/,false/*testOnly*/)) {
            
            if (DEBUG)
                log.debug(LOG_NO_PREDECESSOR);

            return null;
            
        }
        
        return currentPosition.get(tuple);
        
    }

    public ITuple<E> prior() {

        final ITuple<E> t;
        
        if (!isCursorPositionDefined()) {

            t = last();
            
        } else {

            t = priorTuple();

        }

        if (t == null) {
            
            throw new NoSuchElementException();
            
        }
        
        return t;
        
//        if(!hasPrior()) throw new NoSuchElementException();
//
//        // update the current position.
//        currentPosition = priorPosition;
//        
//        // clear references since no longer valid.
//        nextPosition = priorPosition = null;
//
//        // Copy the data into [tuple] and return [tuple].
//        return currentPosition.get(tuple);

    }
    
    public boolean hasPrior() {
    
//        // the prior position is already known.
//        if (priorPosition != null) return true;

        final boolean skipCurrent;

        final AbstractCursorPosition<L,E> pos;
        
        if (!isCursorPositionDefined()) {

            /*
             * Note: This makes hasPrior() and prior() have semantics exactly
             * parallel to those of the standard iterator. See hasNext() for
             * details.
             */
            pos = lastPosition();

            /*
             * Don't skip the last position if it is a visitable tuple.
             */
            skipCurrent = false;

        } else {

            /*
             * The cursor position was already defined so scan backward
             * (skipping the current tuple) to the previous visitable tuple.
             */
            skipCurrent = true;

//            if (tempPosition == null) {
//
//                pos = tempPosition = newTemporaryPosition(currentPosition);
//                
//            } else {
//                
//                pos = tempPosition;
//
//                pos.seek(currentPosition);
//                
//            }
            
//            pos = newTemporaryPosition(currentPosition);

            pos = currentPosition;
            
        }

        if (!pos.reverseScan(skipCurrent,true/*testOnly*/)) {

            if (DEBUG)
                log.debug(LOG_NO_PREDECESSOR);

            // nothing visitable.
            return false;

        }

        if (DEBUG)
            log.debug(pos.toString());
        
        // found something visitable.
        return true;

    }

    /**
     * FIXME This needs to be overridden for the {@link IsolatedFusedView} in
     * order to correctly propagate the version timestamp onto the tuple. See
     * the insert() and remove() methods on that class. This will probably be
     * done inside of an implementation that extends the {@link FusedView}
     * cursor implementation, at which point this notice can be removed.
     */
    public void remove() {

        if (btree.isReadOnly())
            throw new UnsupportedOperationException();
        
        if (!isCursorPositionDefined()) {
            
            throw new IllegalStateException(LOG_NO_CURSOR_POSITION);
            
        }
        
        if (!currentPosition.isVisitableTuple()) {
         
            /*
             * The cursor position is defined, e.g., by seek(), but it is not on
             * a visitable tuple.
             */
            
            throw new IllegalStateException(LOG_CURSOR_POSITION_NOT_VISITABLE);
            
        }
        
        // the key for the last visited tuple.
        final byte[] key = currentKey();
        
        if (DEBUG)
            log.debug("key="+BytesUtil.toString(key));
        
        /*
         * Remove the last visited tuple.
         * 
         * Note: this will cause the tuples in the current leaf to have a gap
         * (at least logically) and the remaining tuples will be moved down to
         * cover that gap. This means that the [index] for the cursor position
         * needs to be corrected.  That is handled when the cursor position's
         * listener notices the mutation event.
         */
        
        btree.remove(key);
        
    }

    /**
     * A cursor position for an {@link ITupleCursor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <L>
     *            The generic type for the leaves of the B+Tree.
     * @param <E>
     *            The generic type for objects de-serialized from the values in
     *            the index.
     */
    static interface ICursorPosition<L extends Leaf,E> {
        
        /**
         * The cursor that owns this cursor position.
         */
        public ITupleCursor<E> getCursor();

        /**
         * The cursor used to navigate the leaves of the B+Tree.
         */
        public ILeafCursor<L> getLeafCursor();

        /**
         * The index of the current tuple in the current leaf.
         */
        public int getIndex();
            
        /**
         * Return the key corresponding to the cursor position.
         * 
         * @return The key.
         */
        public byte[] getKey();

        /**
         * Copy the data from the tuple at the {@link ICursorPosition} into the
         * caller's buffer.
         * <p>
         * Note: If the {@link ICursorPosition} is not on a visitable tuple then
         * this method will return <code>null</code> rather than the caller's
         * tuple. This situation can arise if the index is empty (e.g., a root
         * leaf with nothing in it) or if current cursor position does not
         * correspond to a visitable tuple in the index.
         * 
         * @param tuple
         *            The caller's buffer.
         * 
         * @return The caller's buffer -or- <code>null</code> if the
         *         {@link ICursorPosition} is not on a visitable tuple.
         */
        public Tuple<E> get(Tuple<E> tuple);

        /**
         * Return <code>true</code> iff the tuple corresponding to the cursor
         * position is visitable. A cursor position for which there is no
         * corresponding tuple in the index is not a visitable tuple. A cursor
         * position for which the corresponding tuple in the index is deleted is
         * visitable iff the {@link ITupleCursor} was provisioned to visit
         * deleted tuples.
         * 
         * @return <code>true</code> iff the cursor position corresponds to a
         * visitable tuple.
         */
        public boolean isVisitableTuple();
        
        /**
         * Scan forward to the next visitable tuple from the current position.
         * 
         * @param skipCurrent
         *            If the current tuple should be skipped over. when
         *            <code>false</code> position will not be advanced if the
         *            current tuple is "visitable".
         * @param testOnly
         *            When <code>true</code> the method will report whether or
         *            not a visitable successor of the cursor position exists
         *            but must not change the cursor position.
         *            
         * @return <code>true</code> if a visitable tuple was found.
         */
        public boolean forwardScan(boolean skipCurrent,boolean testOnly);
        
        /**
         * Scan backward to the previous visitable tuple from the current
         * position.
         * 
         * @param skipCurrent
         *            If the current tuple should be skipped over. when
         *            <code>false</code> position will not be advanced if the
         *            current tuple is "visitable".
         * @param testOnly
         *            When <code>true</code> the method will report whether or
         *            not a visitable predecessor of the cursor position exists
         *            but must not change the cursor position.
         * 
         * @return <code>true</code> if a visitable tuple was found.
         */
        public boolean reverseScan(boolean skipCurrent,boolean testOnly);
        
    }

    /**
     * Abstract base class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    abstract static class AbstractCursorPosition<L extends Leaf,E> implements ICursorPosition<L,E> {
        
        /** The owning {@link ITupleCursor2}. */
        final protected ITupleCursor2<E> cursor;

        /** Used for sequential and random access to the leaves of the B+Tree. */
        final protected ILeafCursor<L> leafCursor;
        
        /** The index of the tuple within that leaf. */
        protected int index;

        /**
         * The key corresponding to the current cursor position. This is update
         * each time we settle on a new tuple or when the cursor position is
         * directed to an explicit key using seek(). Having this state allows us
         * to unambiguously position the cursor in response to a seek() and also
         * allows us to unambiguously re-position the cursor if the current leaf
         * has been invalidated.
         */
        private final DataOutputBuffer kbuf;
        
        /**
         * When true the methods on this class will re-locate the leaf using the
         * key associated with the current tuple before doing anything else
         * (this flag is cleared as a side-effect once the leaf has been
         * re-located).
         * <p>
         * Note: This is used to support {@link ILeafListener}s by some derived
         * classes
         */
        protected boolean leafValid;

//        /**
//         * Return <code>true</code> iff the tuple position is the same as the
//         * given tuple position.
//         */
//        public boolean isSamePosition(AbstractCursorPosition<Leaf, E> pos) {
//
//            if(index != pos.index) return false;
//            
//            if(leafCursor.leaf()!=pos.leafCursor.leaf()) return false;
//            
//            return true;
//            
//        }
        
        public ITupleCursor<E> getCursor() {
            
            return cursor;
            
        }
        
        public ILeafCursor<L> getLeafCursor() {
            
            relocateLeaf();
            
            return leafCursor;
            
        }
        
        public int getIndex() {

            relocateLeaf(); 
            
            return index;
            
        }

        /**
         * <code>true</code> iff the class registers as an
         * {@link ILeafListener} and can therefore handle events that invalidate
         * the position by re-locating the appropriate leaf within the B+Tree.
         */
        public boolean isLeafListener() {
            
            return false;
            
        }
        
        /**
         * Create position on the specified tuple.
         * 
         * @param cursor
         *            The {@link ITupleCursor}.
         * @param leaf
         *            The leaf cursor (already positioned on the desired leaf).
         * @param index
         *            The index of the tuple in the <i>leaf</i> -or- the
         *            insertion point for the <i>key</i> if there is no tuple
         *            for that key in the leaf.
         * @param key
         *            The key (required).
         */
        protected AbstractCursorPosition(final ITupleCursor2<E> cursor,
                final ILeafCursor<L> leafCursor, final int index,
                final byte[] key) {

            if (cursor == null)
                throw new IllegalArgumentException();

            if (leafCursor == null)
                throw new IllegalArgumentException();

            if (key == null)
                throw new IllegalArgumentException();

//            /*
//             * Note: The <i>index</i> MAY be equal to the #of keys in the leaf,
//             * in which case the {@link CursorPosition} MUST be understood to
//             * lie in the next leaf (if it exists). The scanForward() method
//             * handles this case automatically.
//             */
//            if(index < 0 || index > leaf.getKeyCount()) {
//                
//                throw new IllegalArgumentException("index="+index+", nkeys="+leaf.getKeyCount());
//                
//            }
            // @todo the valid range on [index] also includes the insertion points (negative integers).

            this.leafValid = true;
            
            this.cursor = cursor;
            
            this.leafCursor = leafCursor;
            
            this.index = index;
            
            this.kbuf = new DataOutputBuffer(key.length);
            
            this.kbuf.put( key );
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public AbstractCursorPosition(final AbstractCursorPosition<L,E> p) {
           
            if (p == null)
                throw new IllegalArgumentException();
            
            // make sure that source position is valid.
            p.relocateLeaf();
            
            this.leafValid = true;
            
            this.cursor = p.cursor;
            
            this.index = p.index;

            this.kbuf = new DataOutputBuffer(p.kbuf.capacity());
            
            this.kbuf.copyAll(p.kbuf);
            
            this.leafCursor = p.leafCursor.clone();
            
        }
        
        public String toString() {
            
            return "CursorPosition{" + cursor + ", leafCursor=" + leafCursor + ", index="
                    + index + ", leafValid=" + leafValid + ", key="
                    + BytesUtil.toString(kbuf.toByteArray()) + "}";
            
        }

        /**
         * Seek to the given position.
         */
        public void seek(final AbstractCursorPosition<L, E> src) {

            if (src == null)
                throw new IllegalArgumentException();
            
            if (cursor != src.cursor)
                throw new IllegalArgumentException();
            
            // make sure the source position is valid.
            src.relocateLeaf();
            
            leafValid = true;
                        
            index = src.index;
            
            leafCursor.seek(src.leafCursor);
            
            kbuf.reset().copyAll(src.kbuf);
            
        }
        
        final public byte[] getKey() {
            
            return kbuf.toByteArray();
            
        }
        
        /**
         * <code>true</code> if the cursor position corresponds to a tuple in
         * the index.
         * <p>
         * Note: A cursor position can be initially constructed with
         * [index==nkeys], in which case the cursor is not "on" a tuple.
         * Normally the caller will scan forward / backward until they reached a
         * visitable tuple so as to avoid that fence post.
         * <p>
         * Note: The other cause the cursor position not being "on" a tuple is
         * an empty root leaf
         */
        protected boolean isOnTuple() {

            if (index >= 0 && index < leafCursor.leaf().getKeyCount()) {

                return true;
                
            }
            
            return false;

        }
        
        public boolean isVisitableTuple() {
            
            relocateLeaf();
            
            if(!isOnTuple()) {
                
                /*
                 * Note: This happens when [index] is an insertion point
                 * (negative) because there was no tuple in the index for the
                 * current key.
                 */
                
                return false;
                
            }
            
            /*
             * Either delete markers are not supported or we are visiting
             * deleted tuples or the tuple is not deleted.
             */
            
            final Leaf leaf = leafCursor.leaf();
            
            return !leaf.hasDeleteMarkers() || cursor.isDeletedTupleVisitor()
                    || !leaf.getDeleteMarker(index);

        }

        public Tuple<E> get(Tuple<E> tuple) {

            relocateLeaf();
            
            if(!isVisitableTuple()) {
                
                if (DEBUG)
                    log.debug(LOG_CURSOR_POSITION_NOT_VISITABLE);

                return null;
                
            }
            
            tuple.copy(index, leafCursor.leaf()); // Note: increments [tuple.nvisited] !!!

            if(DEBUG) {
                
                log.debug(tuple.toString());
                
            }
            
            return tuple;
            
        }
        
        /**
         * The contract for this method is to re-locate the {@link #leaf} and
         * then re-locate the {@link #index} of current tuple within that leaf
         * using the key reported by {@link #getKey()}. This method is
         * triggered when an {@link ILeafListener} is registered against a
         * mutable {@link BTree}).
         * <p>
         * The default implementation does nothing if {@link #leafValid} is
         * <code>true</code> (and it will always be true if the B+Tree is
         * read-only).
         * 
         * @return true unless if the leaf was re-located but the current tuple
         *         is no longer present in the leaf (e.g., it was deleted from
         *         the index). Note that the return is always <code>true</code>
         *         if delete markers are in effect since a remove() is actually
         *         an update in that case.
         * 
         * @throws UnsupportedOperationException
         *             if {@link #leafValid} is <code>false</code>
         * 
         * @todo the return value is never used so this could be declared to
         *       return <code>void</code>
         */
        protected boolean relocateLeaf() {
        
            if(leafValid) return true;
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Materialize the next leaf in the natural order of the index.
         * <p>
         * Note: This method is NOT required to validate that the next leaf lies
         * within the optional key-range constraint on the owning
         * {@link ITupleCursor}.
         * 
         * @return <code>true</code> if there is successor of the current
         *         leaf.
         */
        protected boolean nextLeaf(final ILeafCursor<L> leafCursor) {

            if (leafCursor.next() == null) {
            
                log.info("No right sibling.");

                return false;

            }

            return true;
            
        }

        /**
         * Materialize the prior leaf in the natural order of the index.
         * <p>
         * Note: This method is NOT required to validate that the prior leaf
         * lies within the optional key-range constraint on the owning
         * {@link ITupleCursor}.
         * 
         * @return <code>true</code> if there is a predecessor of the current
         *         leaf.
         */
        protected boolean priorLeaf(ILeafCursor<L> leafCursor) {
            
            if (leafCursor.prior() == null) {

                log.info("No left sibling.");

                return false;

            }
            
            return true;
            
        }

        /**
         * Return <code>true</code> if the key at the current {@link #index} in
         * the current {@link #leaf} lies inside of the optional half-open range
         * constraint.
         * 
         * @param leaf
         *            The current leaf.
         * @param index
         *            The index of the current tuple in that leaf.
         * @param forward
         *            <code>true</code> iff the cursor is moving in the forward
         *            direction (increasing key order) and <code>false</code>
         *            iff the cursor is moving in the reverse direction
         *            (decreasing key order). We only check the
         *            <code>fromKey</code> when moving in reverse order and the
         *            <code>toKey</code> when moving in forward order.
         * 
         * @return <code>true</code> unless the current key is LT [fromKey] or
         *         GTE [toKey].
         * 
         * @todo could only rangeCheck in the direction of the traversal, but
         *       this causes cursor test failures for some reason.
         * 
         * @todo for a read-only view, we could compute the leaf addr and the
         *       tuple index of the fromKey and/or to key and then just compare
         *       those with the given leaf and index to decide if we were within
         *       the necessary bounds.
         */
        private boolean rangeCheck(final L leaf, final int index) {//, boolean forward) {

            // optional inclusive lower bound (may be null).
            final byte[] fromKey = cursor.getFromKey();
//            final byte[] fromKey = forward ? null : cursor.getFromKey();

            // optional exclusive upper bound (may be null).
            final byte[] toKey = cursor.getToKey();
//            final byte[] toKey = forward ? cursor.getToKey() : null;

            if (fromKey == null && toKey == null) {

                // no range constraint.
                return true;
                
            }

            /*
             * The key for the cursor position.
             */
            if (true) {
                final byte[] key = leaf.getKeys().get(index);
                if (fromKey != null && BytesUtil.compareBytes(key, fromKey) < 0) {
                    // key is LT then the optional inclusive lower bound.
                    return false;
                }
                if (toKey != null && BytesUtil.compareBytes(key, toKey) >= 0) {
                    // key is GTE the optional exclusive upper bound
                    return false;

                }

            } else {

                /*
                 * FIXME This performance optimizes out the allocation of a copy
                 * of the key using an inline comparison. However, there is
                 * something wrong as this causes
                 * AbstractBTreeCursorTestCase#test_baseCase() to fail.
                 */

                if (tbuf == null) {
                    tbuf = new DataOutputBuffer(0);
                }
                leafCursor.leaf().getKeys().copy(index, tbuf);
                final int tlen = tbuf.limit();
                final byte[] a = tbuf.array();

                if (fromKey != null) {

                    if (BytesUtil.compareBytesWithLenAndOffset(0, tlen, a, 0,
                            fromKey.length, fromKey) < 0) {

                        if (DEBUG) {

                            log.debug("key=" + BytesUtil.toString(a, 0, tlen)
                                    + " LT fromKey" + BytesUtil.toString(fromKey));

                        }

                        // key is LT then the optional inclusive lower bound.
                        return false;

                    }

                }

                if (toKey != null) {

                    if (BytesUtil.compareBytesWithLenAndOffset(0, tlen, a, 0,
                            toKey.length, toKey) >= 0) {

                        if (DEBUG) {

                            log.debug("key=" + BytesUtil.toString(a, 0, tlen)
                                    + " GTE toKey" + BytesUtil.toString(toKey));

                        }

                        // key is GTE the optional exclusive upper bound
                        return false;

                    }

                }

            }
            return true;
            
        }
      /**
      * Buffer used to range check the current key before it is copied into
      * #kbuf.
      */
     private DataOutputBuffer tbuf;

//        private AbstractCursorPosition<L,E> priorPosition;
//        private AbstractCursorPosition<L,E> nextPosition;

        /**
         * Used to update the position once a visitable tuple has been found.
         * Both {@link #forwardScan(boolean, boolean)} and
         * {@link #reverseScan(boolean, boolean)} are written to have NO SIDE
         * EFFECT on the position unless this method is invoked.
         */
        private void updatePosition(final ILeafCursor<L> leafCursor,
                final int index) {
            
            // the tuple index.
            this.index = index;
            
            /*
             * The leaf cursor
             * 
             * Note: The way the forward and reverse scan methods are written we
             * use the live leaf cursor unless [testOnly == true] so when it
             * comes to update the position this should always be the live leaf
             * cursor!
             */
            assert this.leafCursor == leafCursor; 
//            if (this.leafCursor != leafCursor) {
//
//                this.leafCursor.seek(leafCursor);
//
//            }
         
            // copy the current key.
            kbuf.reset();
            leafCursor.leaf().getKeys().copy(index, kbuf);

        }
        
        final public boolean forwardScan(final boolean skipCurrent,
                final boolean testOnly) {

            relocateLeaf();

            /*
             * Note: The use of local variables for [index] and [leafCursor]
             * avoids side-effects until we find a visitable successor of the
             * current position.
             * 
             * Note: [index] is cloned immediately while [leafCursor] is cloned
             * lazily (iff we move onto another leaf and [testOnly == true]).
             */
            int index = this.index;
            ILeafCursor<L> leafCursor = this.leafCursor;
            
            if (index < 0) {

                // if it is an insert position then convert it to an index first.
                index = -index - 1;
                
            } else if (skipCurrent) {

                /*
                 * Note: don't skip the current tuple if we had an insertion
                 * point (and hence we were not actually on a tuple).
                 */

                index++;
                
            }
            
            /*
             * Scan until we find a visitable tuple.
             * 
             * Note: If [index >= nkeys] then the scan will immediately skip to
             * the next leaf (if any) in the index.
             */
            
            // set true iff the scan exceeds the optional exclusive upper bound.
            boolean done = false;
            
            while (!done) {

                final L leaf = leafCursor.leaf();
                
                final int nkeys = leaf.getKeyCount();

                /*
                 * Scan tuples in the current leaf until we find a visitable
                 * tuple.
                 */
                for (; index < nkeys; index++) {

                    if (!rangeCheck(leaf, index)) {//, true/* forward */)) {
                        
                        // tuple is LT [fromKey] or GTE [toKey].
                        
                        done = true;
                        
                        break;
                        
                    }
                    
                    if (!leaf.hasDeleteMarkers()
                            || cursor.isDeletedTupleVisitor()
                            || !leaf.getDeleteMarker(index)) {

                        // found visitable tuple.

                        if (!testOnly) {

                            updatePosition(leafCursor, index);

                        }
                        
                        return true;

                    }

                }

                if(testOnly) {
                    
                    // Clone the cursor to avoid side-effects.
                    leafCursor = leafCursor.clone();
                    
                }

                if(!nextLeaf(leafCursor)) {

                    // no more leaves.
                    break;

                }

                // always start at index ZERO(0) after the first leaf.
                index = 0;

            }
            
            if (INFO)
                log.info(LOG_NO_SUCCESSOR);
            
            // no visitable tuple.
            return false;
            
        }
        
        final public boolean reverseScan(final boolean skipCurrent,final boolean testOnly) {

            relocateLeaf();
            
            // Note: updated if we change to a prior leaf!
            int nkeys = leafCursor.leaf().getKeyCount();
            
            if (nkeys == 0) {

                /*
                 * This happens when there is an empty root leaf. We have to
                 * test for this case explicitly since the for() loop below
                 * allows index == 0.
                 */

                return false;
                
            }
            
            /*
             * Note: The use of local variables for [index] and [leafCursor]
             * avoids side-effects until we find a visitable predecessor of the
             * current position.
             * 
             * Note: [index] is cloned immediately while [leafCursor] is cloned
             * lazily (iff we move onto another leaf and [testOnly == true]).
             */
            int index = this.index;
            ILeafCursor<L> leafCursor = this.leafCursor;
            
            if (index < 0) {

                // if it is an insert position then convert it to an index first.
                index = -index - 1;

            }

            if (skipCurrent) {
                
                index--;

            }
            
            /*
             * Scan leafs until we find a visitable tuple.
             * 
             * Note: If [index < 0] then the scan will immediately skip to the
             * prior leaf (if any) in the index.
             */
            
            // set true iff the scan reaches or exceeds the optional inclusive lower bound.
            boolean done = false;
            
            while (!done) {

                /*
                 * Scan tuples in the current leaf until we find a visitable
                 * tuple.
                 */
                
                final L leaf = leafCursor.leaf();
                
                for (; index >= 0; index--) {

                    if (index == nkeys) continue;
                    
                    if (!rangeCheck(leaf, index)) {//, false/* forward */)) {
                        
                        // tuple is LT [fromKey] or GTE [toKey].
                        
                        done = true;
                        
                        break;
                        
                    }

                    if (!leaf.hasDeleteMarkers()
                            || cursor.isDeletedTupleVisitor()
                            || !leaf.getDeleteMarker(index)) {

                        // found visitable tuple.

                        if(!testOnly) {
                            
                            updatePosition(leafCursor, index);
                            
                        }
                        
                        return true;

                    }

                }

                if(testOnly) {
                    
                    // Clone the cursor to avoid side-effects.
                    leafCursor = leafCursor.clone();
                    
                }

                if(!priorLeaf(leafCursor)) {
                    
                    // no more leaves.
                    break;
                    
                }

                // set to the #of keys in the prior leaf.
                nkeys = leafCursor.leaf().getKeyCount();
                
                // always reset the index to [nkeys-1] when moving to a prior leaf.
                index = nkeys - 1;

            }
            
            if (INFO)
                log.info(LOG_NO_PREDECESSOR);
            
            // no visitable tuple.
            return false;
            
        }
        
    }
    
    /**
     * {@link ICursorPosition} for a read-only {@link BTree} (does not establish
     * listeners).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static private class ReadOnlyCursorPosition<E> extends
            AbstractCursorPosition<Leaf, E> {

        /**
         * @param cursor
         * @param leafCursor
         * @param index
         * @param key
         */
        public ReadOnlyCursorPosition(ITupleCursor2<E> cursor,
                ILeafCursor<Leaf> leafCursor, int index, byte[] key) {

            super(cursor, leafCursor, index, key);
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public ReadOnlyCursorPosition(ReadOnlyCursorPosition<E> p) {
            
            super( p );
            
        }

    }

    /**
     * {@link ICursorPosition} for a mutable {@link BTree} (establishes
     * listeners to determine when the current leaf and/or tuple index has been
     * invalidated).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static private class MutableCursorPosition<E> extends
            ReadOnlyCursorPosition<E> implements ILeafListener {
        
        /**
         * @param cursor
         * @param leafCursor
         * @param index
         */
        protected MutableCursorPosition(ITupleCursor2<E> cursor,
                ILeafCursor<Leaf> leafCursor, int index, byte[] key) {

            super(cursor, leafCursor, index, key);
            
            leafCursor.leaf().addLeafListener(this);
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public MutableCursorPosition(MutableCursorPosition<E> p) {
            
            super( p );

            leafCursor.leaf().addLeafListener(this);

        }

        /**
         * Note: rather than immediately re-locate the leaf this just notes that
         * the leaf is invalid and it will be re-located the next time we need
         * to do anything with the leaf.
         */
        public void invalidateLeaf() {

            leafValid = false;

        }

//        /**
//         * Note: This does NOT establish an {@link ILeafListener} because it is
//         * only used for the temporary position (vs the current position).
//         */
//        @Override
//        public void seek(AbstractCursorPosition<Leaf, E> src) {
//            
//            super.seek(src);
//            
//        }

        @Override
        final public boolean isLeafListener() {
            
            return true;
            
        }
        
        /**
         * Extended to register ourselves as a listener to the new leaf.
         */
        protected boolean priorLeaf(ILeafCursor<Leaf> leafCursor) {
            
            if(super.priorLeaf(leafCursor)) {
                
                leafCursor.leaf().addLeafListener(this);

                return true;
                
            }
            
            return false;
            
        }
        
        /**
         * Extended to register ourselves as a listener to the new leaf.
         */
        protected boolean nextLeaf(ILeafCursor<Leaf>leafCursor) {
            
            if(super.nextLeaf(leafCursor)) {
                
                leafCursor.leaf().addLeafListener(this);

                return true;
                
            }
            
            return false;
            
        }
        
        protected boolean relocateLeaf() {
            
            if(leafValid) return true;
            
            // the key corresponding to the cursor position.
            final byte[] key = getKey();

            if (INFO)
                log.info("Relocating leaf: key=" + BytesUtil.toString(key));
            
            /*
             * Re-locate the leaf and re-egister the cursor position as a
             * listener for the new leaf.
             */
            leafCursor.seek(key).addLeafListener(this);

            // Re-locate the tuple index for the key in that leaf.
            index = leafCursor.leaf().getKeys().search(key);
            
            final boolean tupleDeleted;
            
            if (index < 0) {
                
                tupleDeleted = true;
                
            } else {
                
                tupleDeleted = false;
                
            }

            leafValid = true;
            
            return tupleDeleted;
            
        }
        
    }
    
    /**
     * An {@link ITuple} that directly supports forward and reverse cursor
     * operations on a local {@link BTree}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class ReadOnlyBTreeTupleCursor<E> extends
            AbstractBTreeTupleCursor<BTree, Leaf, E> {

        public ReadOnlyBTreeTupleCursor(final BTree btree,
                final Tuple<E> tuple, final byte[] fromKey, final byte[] toKey) {

            super(btree, tuple, fromKey, toKey);

        }

        @Override
        protected ReadOnlyCursorPosition<E> newPosition(
                final ILeafCursor<Leaf> leafCursor, final int index,
                final byte[] key) {

            return new ReadOnlyCursorPosition<E>(this, leafCursor, index, key);

        }

        @Override
        protected ReadOnlyCursorPosition<E> newTemporaryPosition(
                final ICursorPosition<Leaf, E> p) {

            return new ReadOnlyCursorPosition<E>( (ReadOnlyCursorPosition<E>) p );

        }

    }

    /**
     * An {@link ITuple} that directly supports forward and reverse cursor
     * operations on a local mutable {@link BTree}.
     * <p>
     * This implementation supports concurrent modification of the {@link BTree}
     * but is NOT thread-safe. This means that you can interleave cursor-based
     * tuple operations with insert, update, or delete of tuples using the
     * {@link BTree}. However, the {@link BTree} itself is NOT thread-safe for
     * mutation and this class does not relax that limitation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class MutableBTreeTupleCursor<E> extends
            ReadOnlyBTreeTupleCursor<E> {

        public MutableBTreeTupleCursor(BTree btree, Tuple<E> tuple,
                byte[] fromKey, byte[] toKey) {

            super(btree, tuple, fromKey, toKey);

        }

        @Override
        protected MutableCursorPosition<E> newPosition(
                ILeafCursor<Leaf> leafCursor, int index, byte[] key) {
            
            return new MutableCursorPosition<E>(this, leafCursor, index, key);
            
        }

        /**
         * Note: This is only used by {@link #hasNext()} and {@link #hasPrior()}
         * for a temporary test without side-effects on the state of the
         * {@link ITupleCursor} and therefore we do NOTNOT register an
         * {@link ILeafListener} since that is just more overhead and it will
         * not be used.
         */
        @Override
        protected ReadOnlyCursorPosition<E> newTemporaryPosition(ICursorPosition<Leaf, E> p) {

            return new ReadOnlyCursorPosition<E>( (ReadOnlyCursorPosition<E>) p );

        }

    }

}
