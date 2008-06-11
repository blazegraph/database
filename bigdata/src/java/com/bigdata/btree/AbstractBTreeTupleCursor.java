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

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;
import com.bigdata.btree.Leaf.ILeafListener;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.ClientIndexView;

/**
 * Class supporting random access to tuples and sequential tuple-based cursor
 * movement for an {@link AbstractBTree}.
 * <p>
 * The tuple position is defined in terms of the current key on which the tuple
 * "rests". If there is no data associated with that key in the index then you
 * will not be able to read the value or optional metadata (delete markers or
 * version timestamps) for the key. If the key is associated with a deleted
 * tuple then you can not read the value associated with the key, but you can
 * read the (optional) delete marker and version metadata. If the value stored
 * under the key is changed either using this class or the {@link BTree} then
 * the new values will be visible via this class.
 * 
 * FIXME Rewrite FusedView's iterator to implement {@link ITupleCursor}.
 * 
 * FIXME Rewrite the iterators based on the {@link ResultSet} to implement
 * {@link ITupleCursor}.
 * 
 * FIXME Modify {@link AbstractBTree} to use an {@link ITupleCursor} without the
 * post-order striterator (but we still need post-order traversal for flushing
 * evicted nodes to the store!)
 * 
 * FIXME change the return type of
 * {@link AbstractBTree#rangeIterator(byte[], byte[], int, int, ITupleFilter)}
 * to {@link ITupleCursor} in order to allow access handle prior/next access.
 * 
 * FIXME The {@link ClientIndexView} will need be modified to defer request of
 * the initial result set until the caller uses first(), last(), seek(),
 * hasNext(), or hasPrior().
 * 
 * FIXME get rid of the {@link ChunkedLocalRangeIterator}. Make sure that the
 * {@link IRangeQuery#REMOVEALL} flag is correctly interpreted as not allowing
 * more than [capacity] tuples to be deleted.
 * 
 * FIXME Examine how REMOVEALL is used together with a capacity limit for atomic
 * deletes on the server and verify that we can handle those use cases for head
 * and tail deletes using the {@link ITupleCursor} instead.
 * 
 * FIXME See {@link AbstractTupleFilterator} for many, many notes about the new
 * interator constructs and things that can be changed once they are running,
 * including the implementation of the prefix scans, etc.
 * 
 * FIXME Consider {@link AbstractTupleFilterator}, whether the [nextPosition]
 * and [priorPosition] fields should be discarded and we just re-scan forward /
 * backward in prior() / next() (they should not invoke hasNext() and hasPrior()
 * in that case but a priorTuple() and nextTuple() that return null if there is
 * no such tuple in order to avoid scanning twice for each request. Those
 * methods can be placed on the {@link ITupleCursor} interface and they will be
 * more efficient: while((t = priorTuple())!=null) { ... }
 * <p>
 * The problem is how to scan forward/backward conditionally - without changing
 * the actual cursor position. That is why [nextPosition] and [priorPosition]
 * exist. However, those could be cached internally in the _current_ position
 * and invalidated if the current position was invalidated. This would let us
 * use only a single listener for the iterator and the listener would be moved
 * from leaf to leaf as we go (it could be explicitly unregistered).
 * <p>
 * It seems that the cursor position DOES need to track the current key
 * explicitly in its own buffer so that seek() to a non-existent key will work
 * correctly and so that we can re-establish the cursor position without fail if
 * the leaf is invalidated.
 * <p>
 * The cursor position could carry a flag if it becomes invalid so that we do
 * not have to re-allocate it, but that seems a minor cost since it should not
 * be commit.
 * 
 * FIXME finish layering of iterators/cursors to support logical row scans, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeTupleCursor<I extends AbstractBTree, L extends Leaf, E>
        implements ITupleCursor<E> {

    protected static final Logger log = Logger.getLogger(AbstractBTreeTupleCursor.class);
    
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
     * The cursor position is undefined until {@link #first(boolean)},
     * {@link #last(boolean)}, or {@link #seek(byte[])} is used to position the
     * cursor.
     * 
     * @throws IllegalStateException
     *             if the cursor position is not defined.
     */
    protected void assertCursorPositionDefined() {

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
    public AbstractBTreeTupleCursor(I btree, Tuple<E> tuple,
            byte[] fromKey, byte[] toKey) {

        if (btree == null)
            throw new IllegalArgumentException();

        if (tuple == null)
            throw new IllegalArgumentException();

        if (tuple.btree != btree)
            throw new IllegalArgumentException();
        
        if (fromKey != null) {

            btree.rangeCheck(fromKey, false/* allowUpperBound */);

        }

        if (toKey != null) {

            btree.rangeCheck(toKey, true/* allowUpperBound */);
            
        }

        if (fromKey != null && toKey != null) {

            if (BytesUtil.compareBytes(fromKey, toKey) >= 0) {

                throw new IllegalArgumentException(
                        "fromKey/toKey are out of order.");
                
            }
            
        }
        
        this.btree = btree;
        
        this.tuple = tuple;
        
        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
        this.visitDeleted = ((tuple.flags() & IRangeQuery.DELETED) != 0);

        // Note: the cursor position is NOT defined!
        currentPosition = nextPosition = priorPosition = null;
        
    }

    public String toString() {
        
        return "Cursor{fromKey=" + BytesUtil.toString(fromKey) + ", toKey="
                + BytesUtil.toString(toKey) + ", currentKey="
                + BytesUtil.toString(currentKey()) + ", visitDeleted="
                + visitDeleted + "}";
        
    }
    
    /**
     * The optional inclusive lower bound. If the <i>fromKey</i> constraint was
     * specified when the {@link ITupleCursor} was created, then that value is
     * returned. Otherwise, if the index is an index partition then the
     * {@link LocalPartitionMetadata#getLeftSeparatorKey()} is returned.
     * Finally, <code>null</code> is returned if there is no inclusive lower
     * bound.
     */
    protected byte[] getInclusiveLowerBound() {
        
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
    protected byte[] getExclusiveUpperBound() {
        
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

    final public ITuple<E> seek(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        return seek(getIndex().getIndexMetadata().getTupleSerializer()
                .serializeKey(key));
        
    }
    
    /**
     * The current cursor position.
     */
    protected AbstractCursorPosition<L,E> currentPosition;
    
    final public boolean isCursorPositionDefined() {

        return currentPosition != null;
        
    }

    /**
     * Used by {@link #hasNext()} to scan forward to the next visitable
     * tuple without a side-effect on the current cursor position and
     * cleared to <code>null</code> by any method that changes the current
     * cursor position.
     */
    protected AbstractCursorPosition<L,E> nextPosition;

    /**
     * Used by {@link #hasPrior()} to scan backward to the previous
     * visitable tuple without a side-effect on the current cursor position and
     * cleared to <code>null</code> by any method that changes the current
     * cursor position.
     */
    protected AbstractCursorPosition<L,E> priorPosition;

    /**
     * Return the leaf that spans the optional {@link #getFromKey()}
     * constraint and the first leaf if there is no {@link #getFromKey()}
     * constraint.
     * 
     * @return The leaf that spans the first tuple that can be visited by
     *         this cursor.
     * 
     * @see Leaf#getKeys()
     * @see IKeyBuffer#search(byte[])
     */
    abstract protected L getLeafSpanningFromKey();

    /**
     * Return the leaf that spans the optional {@link #getToKey()}
     * constraint and the last leaf if there is no {@link #getFromKey()}
     * constraint.
     * 
     * @return The leaf that spans the first tuple that can NOT be visited
     *         by this cursor (exclusive upper bound).
     * 
     * @see Leaf#getKeys()
     * @see IKeyBuffer#search(byte[])
     */
    abstract protected L getLeafSpanningToKey();
    
    /**
     * Return the leaf that spans the key.  The caller must check to see
     * whether the key actually exists in the leaf.
     * 
     * @param key
     *            The key.
     *            
     * @return The leaf spanning that key.
     */
    abstract protected L getLeafSpanningKey(byte[] key);
    
    /**
     * Return a new {@link ICursorPosition} from the leaf and the tuple index.
     * 
     * @param leaf
     *            The leaf.
     * @param index
     *            The tuple index within that <i>leaf</i> -or- a negative
     *            integer representing the insertion point for the <i>key</i>
     *            if the <i>key</i> is spanned by leaf but there is no tuple
     *            for that <i>key</i> in the <i>leaf</i>.
     * @param key
     *            The key.
     * 
     * @return The new {@link ICursorPosition}.
     * 
     * @throws IllegalArgumentException
     *             if <i>leaf</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>key</i> is <code>null</code>.
     */
    abstract protected AbstractCursorPosition<L, E> newPosition(L leaf,
            int index, byte[] key);

    /**
     * Return a clone of the given {@link ICursorPosition}.
     * 
     * @param p
     *            The cursor position.
     *            
     * @return A clone of that cursor position.
     * 
     * @todo if we drop [nextPosition] and [priorPosition] then we can also drop this method.
     */
    abstract protected AbstractCursorPosition<L, E> newPosition(ICursorPosition<L, E> p);
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on
     * the first tuple in the key-range (does not skip over deleted tuples).
     */
    protected AbstractCursorPosition<L,E> firstPosition() {

        byte[] key = getInclusiveLowerBound();

        if (key == null) {

            key = BytesUtil.EMPTY;

        }
        
        final L leaf = getLeafSpanningFromKey();
        
        final int index = leaf.getKeys().search(key);
        
        return newPosition(leaf, index, key);
        
    }
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on the
     * given <i>key</i> (does not skip over deleted tuples).
     * 
     * @param leaf
     *            A leaf.
     * @param key
     *            A key that is spanned by that leaf.
     * 
     * @return The new {@link ICursorPosition}.
     */
    protected AbstractCursorPosition<L,E> newPosition(L leaf, byte[] key) {
        
        assert leaf != null;
        
        assert key != null;
        
        /*
         * Find the starting tuple index for the key in that leaf.
         * 
         * Note: this will give us the insertion point if the key is not
         * found in the leaf, in which case we convert it into the index
         * of the first tuple ordered after the key.
         */

        final int index = leaf.getKeys().search(key);
        
//        if (key == null) {
//            
//            index = 0;
//            
//        } else {
//
//
//            if (log.isInfoEnabled())
//                log.info("position=" + index);
//            
//            if (index < 0) {
//
//                /*
//                 * Convert the insert position into the index of the
//                 * successor.
//                 * 
//                 * Note: This can result in index == nkeys, in which case
//                 * the first tuple of interest does not actually lie within
//                 * the starting leaf.
//                 */
//
//                index = -index - 1;
//
//            }
//
//        }

        if (log.isInfoEnabled())
            log.info("index=" + index);

        final AbstractCursorPosition<L,E> pos = newPosition(leaf, index, key);

        return pos;
        
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
    protected AbstractCursorPosition<L,E> lastPosition() {

        final L leaf = getLeafSpanningToKey();
        
        byte[] key = getExclusiveUpperBound();
        
        final int index;
        
        if (key == null) {
            
            /*
             * Use the last key in the leaf.
             * 
             * Note: This will be an insertion point if this this an empty root
             * leaf.
             */

            index = leaf.getKeyCount() - 1;
            
            key = BytesUtil.EMPTY;
            
        } else {

            // find the position of the key in the leaf.
            index = leaf.getKeys().search(key);

//            if (log.isInfoEnabled())
//                log.info("position=" + index);
//
//            if (index < 0) {
//
//                /*
//                 * Convert the insert position into the index of the
//                 * predecessor.
//                 */
//
//                index = -index - 1;
//
//            } else {
//
//                index = index - 1;
//                
//            }

        }

        if (log.isInfoEnabled())
            log.info("index=" + index);

        final AbstractCursorPosition<L,E> pos = newPosition(leaf, index, key);

        return pos;

    }

    public byte[] currentKey() {
        
        if(!isCursorPositionDefined()) {
            
            // the cursor position is not defined.
            return null;
            
        }
        
        return currentPosition.getKey();
        
    }
    
    public ITuple<E> tuple() {
        
        if(!isCursorPositionDefined()) {
            
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

        // clear references since no longer valid.
        nextPosition = priorPosition = null;
        
        // new position on the inclusive lower bound.
        currentPosition = firstPosition();

        // Scan until we reach the first visitable tuple.
        if (!currentPosition.forwardScan(false/*skipCurrent*/)) {
        
            // discard the current position since we have scanned beyond the end of the index / key range.
            currentPosition = null;
            
            // Nothing visitable.
            return null;
            
        }

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);
        
    }
    
    public ITuple<E> last() {
        
        // clear references since no longer valid.
        nextPosition = priorPosition = null;

        // new position after all defined keys.
        currentPosition = lastPosition();
        
        // Scan backwards until we reach the first visitable tuple.
        if(!currentPosition.reverseScan(false/*skipCurrent*/)) {
        
            // discard the current position since we have scanned beyond the start of the index / key range.
            currentPosition = null;
            
            // Nothing visitable.
            return null;
            
        }

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);
        
    }

    public ITuple<E> seek(byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();
        
        // clear references since no longer valid.
        nextPosition = priorPosition = null;

        // new position is that key.
        currentPosition = newPosition(getLeafSpanningKey(key), key);

//        // Scan until we reach the first visitable tuple.
//        if (!currentPosition.forwardScan(false/*skipCurrent*/)) {
//        
//            // discard the current position since we have seeked beyond the end of the index / key range.
//            currentPosition = null;
//            
//            // Nothing visitable.
//            return null;
//
//        }

        // Copy the data into [tuple].
        return currentPosition.get(tuple);

//        if (BytesUtil.compareBytes(key, currentPosition.getKey()) != 0) {
//            
//            // Found a successor of the probe key.
//            return null;
//            
//        }
//
//        // no visitable tuple for that key.
//        return null;
        
    }

    /**
     * FIXME This is a sketch of an alternative implementation that does not use
     * [nextPosition] and [priorPosition] and that returns null if the cursor
     * position is not defined or if there is no visitable successor.
     * 
     * @return
     */
    public ITuple<E> nextTuple() {
        
        if(!isCursorPositionDefined()) {
            
            // Cursor position is not defined.
            return null;
            
        }
        
        if(!currentPosition.forwardScan(true/*skipCurrent*/)) {

            // no visitable successor.
            currentPosition = null;
            
            return null;
            
        }
        
        return currentPosition.get(tuple);
        
    }
    
    /**
     * FIXME This is a sketch of an alternative implementation that does not use
     * [nextPosition] and [priorPosition] and that returns null if the cursor
     * position is not defined or if there is no visitable successor.
     * 
     * @return
     */
    public ITuple<E> priorTuple() {
        
        if(!isCursorPositionDefined()) {
            
            // Cursor position is not defined.
            return null;
            
        }
        
        if(!currentPosition.reverseScan(true/*skipCurrent*/)) {

            // no visitable predecessor.
            currentPosition = null;
            
            return null;
            
        }
        
        return currentPosition.get(tuple);
        
    }
    
    public boolean hasNext() {

        // the next position is already known.
        if (nextPosition != null) return true;

        if (!isCursorPositionDefined()) {
            
            /*
             * Note: In order to have the standard iterator semantics we
             * start a new scan if the cursor position is undefined.
             */
            
            currentPosition = firstPosition();
            
            /*
             * The cursor position is now defined so scan forward to the
             * next visitable tuple. If the current tuple is visitable then
             * we will use it rather than skipping over it.
             */

            nextPosition = newPosition(currentPosition);
            
            if(!nextPosition.forwardScan(false/*skipCurrent*/)) { 

                // we know that there is no next position.
                nextPosition = null;

                // nothing visitable.
                return false;
            
            }

        } else {
            
            /*
             * The cursor position is defined so scan forward (skipping the
             * current tuple) to the next visitable tuple.
             */

            nextPosition = newPosition(currentPosition);

            if (!nextPosition.forwardScan(true/*skipCurrent*/)) {

                // we know that there is no next position.
                nextPosition = null;

                // nothing visitable.
                return false;

            }

        }

        // found something visitable.
        return true;
        
    }

    public ITuple<E> next() {

        if(!hasNext()) throw new NoSuchElementException();

        // update the current position.
        currentPosition = nextPosition;
        
        // clear references since no longer valid.
        nextPosition = priorPosition = null;

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);
        
    }

    public boolean hasPrior() {
    
        // the prior position is already known.
        if (priorPosition != null) return true;

        if (!isCursorPositionDefined()) {
            
            /*
             * Note: This makes hasPrior() and prior() have semantics
             * exactly parallel to those of the standard iterator. See
             * hasNext() for details.
             */
            
            currentPosition = lastPosition();
            
            /*
             * The cursor position is now defined so scan backward to the
             * next visitable tuple. If the current tuple is visitable then
             * we will use it rather than skipping over it.
             */

            priorPosition = newPosition(currentPosition);
            
            if(!priorPosition.reverseScan(false/*skipCurrent*/)) { 
            
                // we know that there is no previous position.
                priorPosition = null;

                // nothing visitable.
                return false;
            
            }

        } else {
            
            /*
             * The cursor position is defined so scan backward (skipping the
             * current tuple) to the previous visitable tuple.
             */

            priorPosition = newPosition(currentPosition);
            
            if(!priorPosition.reverseScan(true/*skipCurrent*/)) { 

                // we know that there is no previous position.
                priorPosition = null;

                // nothing visitable.
                return false;
            
            }

        }

        // found something visitable.
        return true;

    }

    public ITuple<E> prior() {

        if(!hasPrior()) throw new NoSuchElementException();

        // update the current position.
        currentPosition = priorPosition;
        
        // clear references since no longer valid.
        nextPosition = priorPosition = null;

        // Copy the data into [tuple] and return [tuple].
        return currentPosition.get(tuple);

    }

    /**
     * FIXME This needs to be overriden for the {@link IsolatedFusedView} in
     * order to correctly propagate the version timestamp onto the tuple. See
     * the insert() and remove() methods on that class. This will probably be
     * done inside of an implementation that extends the {@link FusedView}
     * cursor implementation, at which point this notice can be removed.
     */
    public void remove() {

        if (btree.isReadOnly())
            throw new UnsupportedOperationException();
        
        if(!isCursorPositionDefined()) {
         
            throw new IllegalStateException("No cursor position.");
            
        }
        
        // the key for the last visited tuple.
        final byte[] key = tuple.getKey();
        
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

        /*
         * Note: The ILeafListener will notice that the tuple was deleted from
         * the index.
         */
        
    }

    public ITupleIterator<E> asReverseIterator() {
        
        return new ITupleIterator<E>() {

            public ITuple<E> next() {
                
                return prior();
                
            }

            public boolean hasNext() {
               
                return hasPrior();
                
            }

            public void remove() {
                
                AbstractBTreeTupleCursor.this.remove();
                
            }
            
        };
        
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
         * The current leaf.
         */
        public L getLeaf();

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
         * 
         * @return <code>true</code> if a visitable tuple was found.
         */
        public boolean forwardScan(boolean skipCurrent);
        
        /**
         * Scan backward to the previous visitable tuple from the current position.
         * 
         * @param skipCurrent
         *            If the current tuple should be skipped over. when
         *            <code>false</code> position will not be advanced if the
         *            current tuple is "visitable".
         * 
         * @return <code>true</code> if a visitable tuple was found.
         */
        public boolean reverseScan(boolean skipCurrent);
        
    }

    /**
     * Abstract base class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    abstract static class AbstractCursorPosition<L extends Leaf,E> implements ICursorPosition<L,E> {
        
        /** The {@link IndexSegmentTupleCursor}. */
        final protected ITupleCursor<E> cursor;
        
        /** The current leaf. */
        protected L leaf;
        
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

        public ITupleCursor<E> getCursor() {
            
            return cursor;
            
        }
        
        public L getLeaf() {
            
            relocateLeaf();
            
            return leaf;
            
        }
        
        public int getIndex() {

            relocateLeaf(); 
            
            return index;
            
        }
        
        /**
         * Create position on the specified tuple.
         * 
         * @param cursor
         *            The {@link ITupleCursor}.
         * @param leaf
         *            The current leaf.
         * @param index
         *            The index of the tuple in the <i>leaf</i> -or- the
         *            insertion point for the <i>key</i> if there is no tuple
         *            for that key in the leaf.
         * @param key
         *            The key (required).
         */
        protected AbstractCursorPosition(ITupleCursor<E> cursor, L leaf,
                int index, byte[] key) {
            
            if (cursor == null)
                throw new IllegalArgumentException();

            if (leaf == null)
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
            
            this.leaf = leaf;
            
            this.index = index;
            
            this.kbuf = new DataOutputBuffer(key.length);
            
            this.kbuf.put( key );
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public AbstractCursorPosition(ICursorPosition<L,E> p) {
           
            if (p == null)
                throw new IllegalArgumentException();
            
            // note: this will be true since p.getLeaf() will always return the valid leaf.
            this.leafValid = true;
            
            this.cursor = p.getCursor();
            
            this.leaf = p.getLeaf();
            
            this.index = p.getIndex();

            final byte[] key = p.getKey();
            
            this.kbuf = new DataOutputBuffer(key.length);
            
            this.kbuf.put( key );
            
        }
        
        public String toString() {
            
            return "CursorPosition{" + cursor + ", leaf=" + leaf + ", index="
                    + index + ", leafValid=" + leafValid + ", key="
                    + BytesUtil.toString(kbuf.toByteArray()) + "}";
            
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

            if (index>=0 && index < leaf.getKeyCount()) {

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
            
            return !leaf.hasDeleteMarkers() || cursor.isDeletedTupleVisitor()
                    || !leaf.getDeleteMarker(index);

        }

        public Tuple<E> get(Tuple<E> tuple) {

            relocateLeaf();
            
            if(!isVisitableTuple()) return null;
            
            tuple.copy(index, leaf); // Note: increments [tuple.nvisited] !!!

            return tuple;
            
        }
        
        /**
         * Return <code>true</code> if the key at the current {@link #index}
         * in the current {@link #leaf} lies inside of the optional half-open
         * range constraint.
         * 
         * @return <code>true</code> unless tuple is LT [fromKey] or GTE [toKey].
         */
        private boolean rangeCheck() {

            // optional inclusive lower bound (may be null).
            final byte[] fromKey = cursor.getFromKey();
            
            // optional exclusive upper bound (may be null).
            final byte[] toKey = cursor.getToKey();
            
            if (fromKey == null || toKey == null) {

                // no range constraint.
                return true;
                
            }

            // the key for the cursor position.
            final byte[] key = leaf.getKeys().getKey(index);

            assert key != null : "null key @ index="+index;
            
            if (fromKey != null) {

                if (BytesUtil.compareBytes(key, fromKey) < 0) {

                    // key is LT then the optional inclusive lower bound.
                    return false;

                }

            }

            if (toKey != null) {

                if (BytesUtil.compareBytes(key, toKey) >= 0) {

                    // key is GTE the optional exclusive upper bound
                    return false;

                }

            }

            return true;
            
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
         */
        protected boolean relocateLeaf() {
        
            if(leafValid) return true;
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Materialize the next leaf in the natural order of the index and set
         * the {@link #index} to the first tuple in the leaf
         * <code> index := 0 </code>.
         * <p>
         * Note: This method is NOT required to validate that the next leaf lies
         * within the optional key-range constraint on the owning
         * {@link ITupleCursor}.
         * 
         * @return The next leaf -or- <code>null</code> if there is no
         *         successor of the current {@link #leaf}.
         */
        abstract protected boolean nextLeaf();

        /**
         * Materialize the prior leaf in the natural order of the index and set
         * the {@link #index} to the last tuple in the leaf
         * <code> index := nkeys - 1 </code>.
         * <p>
         * Note: This method is NOT required to validate that the prior leaf
         * lies within the optional key-range constraint on the owning
         * {@link ITupleCursor}.
         * 
         * @return The prior leaf -or- <code>null</code> if there is no
         *         predecessor of the current {@link #leaf}.
         */
        abstract protected boolean priorLeaf();
        
        public boolean forwardScan(boolean skipCurrent) {

            relocateLeaf();
            
            int nleaves = 0;
            int ntuples = 0;

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
             * Scan leafs until we find a visitable tuple.
             * 
             * Note: If [index >= nkeys] then the scan will immediately skip to
             * the next leaf (if any) in the index.
             */
            
            // set true iff the scan exceeds the optional exclusive upper bound.
            boolean done = false;
            
            while (!done) {

                final int nkeys = leaf.getKeyCount();

                /*
                 * Scan tuples in the current leaf until we find a visitable
                 * tuple.
                 */
                for (; index < nkeys; index++, ntuples++) {

                    if(!rangeCheck()) {
                        
                        // tuple is LT [fromKey] or GTE [toKey].
                        
                        done = true;
                        
                        break;
                        
                    }
                    
                    // copy the current key.
                    kbuf.reset();
                    try {
                        leaf.getKeys().copyKey(index, kbuf);
                    } catch(IOException ex) {
                        // note: IOException never get thrown.
                        throw new RuntimeException(ex);
                    }

                    if (isVisitableTuple()) {

                        if (log.isInfoEnabled())
                            log.info("Found visitable tuple: leavesScanned="
                                    + nleaves + ", tuplesScanned=" + ntuples);

                        // found visitable tuple.
                        return true;

                    }

                }

                if(!nextLeaf()) {

                    // no more leaves.
                    break;

                }
                
                // #of leaves scanned.
                nleaves++;

            }
            
            if (log.isInfoEnabled())
                log.info("No visitable tuple: leavesScanned=" + nleaves
                        + ", tuplesScanned=" + ntuples);
            
            // no visitable tuple.
            return false;
            
        }
        
        // @todo skipCurrent can be dropped as a parameter.
        // @todo if the index is an insert position then we need to convert it when we start the scan.
        public boolean reverseScan(boolean skipCurrent) {

            relocateLeaf();
            
            final int nkeys = leaf.getKeyCount();
            
            if (nkeys == 0) {

                /*
                 * This happens when there is an empty root leaf. We have to
                 * test for this case explicitly since the for() loop below
                 * allows index == 0.
                 */

                return false;
                
            }
            
            int nleaves = 0;
            int ntuples = 0;
            
            if (index < 0) {

                // if it is an insert position then convert it to an index first.
                index = -index - 1;
                
//                if(index == nkeys) {
//                    
//                    /*
//                     * if the insertion point is on the right edge of the leaf
//                     * then we skip to the prior tuple index since there is no
//                     * tuple at leaf.keys[nkeys] - it is only used for transient
//                     * overflow during insert.
//                     */ 
//                    index--;
//                    
//                }

            }
//             else
            if (skipCurrent) {

//                /* 
//                 * Note: don't skip the current tuple if we had an insertion
//                 * point (and hence we were not actually on a tuple).
//                 */
                
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
                
                for (; index >= 0; index--, ntuples++) {

                    if(index==nkeys) continue;
                    
                    if(!rangeCheck()) {
                        
                        // tuple is LT [fromKey] or GTE [toKey].
                        
                        done = true;
                        
                        break;
                        
                    }

                    // copy the current key.
                    kbuf.reset();
                    try {
                        leaf.getKeys().copyKey(index, kbuf);
                    } catch(IOException ex) {
                        // note: IOException never get thrown.
                        throw new RuntimeException(ex);
                    }
                    
                    if (isVisitableTuple()) {

                        /*
                         * The tuple is either not deleted or we are visiting
                         * deleted tuples.
                         */
                        
                        if (log.isInfoEnabled())
                            log.info("Found visitable tuple: leavesScanned="
                                    + nleaves + ", tuplesScanned=" + ntuples);

                        // found visitable tuple.
                        return true;

                    }

                }

                if(!priorLeaf()) {
                    
                    // no more leaves.
                    break;
                    
                }

                // #of leaves scanned.
                nleaves++;

            }
            
            if (log.isInfoEnabled())
                log.info("No visitable tuple: leavesScanned=" + nleaves
                        + ", tuplesScanned=" + ntuples);
            
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
    static private class ReadOnlyCursorPosition<E> extends AbstractCursorPosition<Leaf,E> {

        /**
         * @param cursor
         * @param leaf
         * @param index
         * @param key
         */
        public ReadOnlyCursorPosition(ITupleCursor<E> cursor, Leaf leaf, int index,byte[] key) {

            super(cursor, leaf, index, key);
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public ReadOnlyCursorPosition(ReadOnlyCursorPosition<E> p) {
            
            super( p );
            
        }

        /**
         * Return the parent of the leaf (robust).
         * <p>
         * The leaf does not hold onto its parent using a hard reference. The
         * {@link AbstractBTree} maintains a hard reference cache so that the
         * parent (and children) will tend to remain strongly reachable while
         * they are in use. Further, the normal top-down navigation mechanisms
         * are recursive so that there is always a hard reference to the parent
         * on the stack. However, the {@link AbstractBTreeTupleCursor} does NOT
         * hold a hard reference to all parents of its
         * {@link AbstractCursorPosition}s (it has three - prior, current, and
         * next). Therefore the weak reference to the parent of the {@link Leaf}
         * associated with an {@link AbstractCursorPosition} will be cleared by
         * the JVM shortly after the parent falls off of the
         * {@link AbstractBTree}s hard reference queue. This method will
         * therefore re-establish the parent of the current leaf by top-down
         * navigation if the parent reference has been cleared (hence robust).
         * Due to the various caching effects this situation is unlikely to
         * arise, but it is nevertheless possible!
         * 
         * @return the parent of the leaf -or- <code>null</code> iff the leaf
         *         is the root leaf of the B+Tree (and hence does not have a
         *         parent).
         */
        protected Node getParent(Leaf leaf) {

            Node parent = leaf.getParent();
            
            if (parent == null) {

                final ReadOnlyBTreeTupleCursor<E> cursor = (ReadOnlyBTreeTupleCursor<E>) getCursor();
                
                // get the root node / leaf.
                final AbstractNode node = cursor.getIndex().getRoot();
                
                if(leaf == node) {
                
                    /*
                     * This is the root leaf and there is no parent.
                     */
                    
                    return null;
                    
                }

                // the key corresponding to the cursor position.
                final byte[] key = leaf.getKeys().getKey(index);

                // find the parent of the leaf containing that key (or its insertion point).
                parent = cursor.getParentOfLeafSpanningKey(key);

                assert parent != null;
                
            }
            
            return parent;

        }
        
        /**
         * Uses {@link Node#getRightSibling(AbstractNode, boolean)} on the
         * {@link #getParent(Leaf) parent} to materialize the next leaf.
         */
        @SuppressWarnings("unchecked")
        protected boolean nextLeaf() {

            final Node parent = getParent(leaf);
            
            if(parent == null) {
                
                log.info("Root leaf");
                
                return false;
                
            }

            final Leaf tmp = (Leaf) parent
                    .getRightSibling(leaf, true/* materialize */);

            if (tmp == null) {

                log.info("No right sibling.");

                return false;

            }

            leaf = tmp;
            
            // always start at index ZERO(0) after the first leaf.
            index = 0;

            return true;
            
        }
        
        /**
         * Uses {@link Node#getLeftSibling(AbstractNode, boolean)} on the
         * {@link #getParent(Leaf) parent} to materialize the prior leaf.
         */
        @SuppressWarnings("unchecked")
        protected boolean priorLeaf() {

            final Node parent = getParent(leaf);
            
            if (parent == null) {

                log.info("Root leaf");

                return false;

            }

            final Leaf tmp = (Leaf) parent
                    .getLeftSibling(leaf, true/* materialize */);

            if (tmp == null) {

                log.info("No left sibling.");

                return false;

            }

            leaf = tmp;
            
            // always start at index [nkeys-1] after the first leaf.
            index = leaf.getKeyCount() - 1;

            return true;
            
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
         * @param leaf
         * @param index
         */
        protected MutableCursorPosition(ITupleCursor<E> cursor, Leaf leaf, int index,byte[] key) {

            super(cursor, leaf, index, key);
            
            leaf.addLeafListener(this);
            
        }

        /**
         * Copy constructor.
         * 
         * @param p
         */
        public MutableCursorPosition(MutableCursorPosition<E> p) {
            
            super( p );

            leaf.addLeafListener(this);

        }

        /**
         * Note: rather than immediately re-locate the leaf this just notes that
         * the leaf is invalid and it will be re-located the next time we need
         * to do anything with the leaf.
         */
        public void invalidateLeaf() {

            leafValid = false;

        }

        /**
         * This cause the cursor to update the tuple state from the index
         * (synchronous).
         * 
         * @todo as an alternative this could cause the tuple state to be
         *       updated no later than the next time
         *       {@link ITupleCursor#tuple()} or any of the methods that change
         *       the cursor position is invoked.
         */
        public void invalidateTuple(int index) {

//            if (index == this.index) {
//
//                /*
//                 * Note: The tuple whose state was changed is the tuple
//                 * corresponding to this cursor position.
//                 */
//                
//                final MutableBTreeTupleCursor cursor = (MutableBTreeTupleCursor<E>)this.cursor;
//                
//                if(cursor.currentPosition == this) {
//                    
//                    /*
//                     * Note: This is the _current_ cursor position (vs the prior
//                     * or next cursor position) so update the state of the tuple
//                     * from the leaf.
//                     * 
//                     * FIXME rather than being synchronous, simply flag the
//                     * tuple state as changed in order to be faster. The flag
//                     * will be cleared if the cursor is moved to another tuple.
//                     * get(Tuple) can be modified or invoked conditionally if
//                     * the flag is set, or the Tuple can be added to the
//                     * _current_ position since that is the only tuple whose
//                     * state we ever report and that will remove the ambiguity
//                     * concerning which Tuple is updated in get(Tuple).
//                     */
//                    
//                    if (get(cursor.tuple) == null) {
//                        
//                        throw new AssertionError();
//                        
//                    }
//                    
//                }
//                
//            }
            
        }

        /**
         * Extended to register ourselves as a listener to the new leaf.
         */
        protected boolean priorLeaf() {
            
            if(super.priorLeaf()) {
                
                leaf.addLeafListener(this);

                return true;
                
            }
            
            return false;
            
        }
        
        /**
         * Extended to register ourselves as a listener to the new leaf.
         */
        protected boolean nextLeaf() {
            
            if(super.nextLeaf()) {
                
                leaf.addLeafListener(this);

                return true;
                
            }
            
            return false;
            
        }
        
        protected boolean relocateLeaf() {
            
            if(leafValid) return true;
            
            // the key corresponding to the cursor position.
            final byte[] key = getKey();

            if(log.isInfoEnabled()) log.info("Relocating leaf: key="+BytesUtil.toString(key));
            
            // re-locate the leaf.
            leaf = ((MutableBTreeTupleCursor<E>) cursor)
                    .getLeafSpanningKey(key);

            // register the cursor position as a listener for the new leaf.
            leaf.addLeafListener(this);
            
            // Re-locate the tuple index for the key in that leaf.
            index = leaf.getKeys().search(key);
            
            final boolean tupleDeleted;
            
            if (index < 0) {

//                /*
//                 * Note: We have re-located the leaf that spans the [key] but
//                 * there is no longer any tuple in the leaf for that [key].
//                 * 
//                 * Convert the insertion point into an index into the leaf.
//                 * 
////                 * Note: We choose the index in the leaf that corresponds to the
////                 * successor of the tuple. Since deleting the tuple created a
////                 * gap (at least logically) in the leaf, the successor is the
////                 * tuple that filled that gap. Since the successor was already
////                 * moved down the [-1] is commented out below in order to give
////                 * us the correct index for the successor.
//                 */
//
//                index = -index - 1; // @todo verify this.
//
//                /*
//                 * Since the tuple is not found we may need to mark the tuple on
//                 * the cursor as deleted (while this is synchronous just like
//                 * when the tuple state is updated, the [leafValid] flag itself
//                 * is NOT handled synchronously so the update will in fact be
//                 * delayed).
//                 */
//                
//                final MutableBTreeTupleCursor cursor = (MutableBTreeTupleCursor<E>)this.cursor;
//                
//                if(cursor.currentPosition == this) {
//
//                    /*
//                     * Note: The tuple whose state was changed is the tuple
//                     * corresponding to the _current_ cursor position so flag
//                     * that tuple as deleted now that it no longer appears in
//                     * the leaf.
//                     */
//
//                    cursor.tuple.markDeleted();
//                    
//                }
                
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

        public ReadOnlyBTreeTupleCursor(BTree btree, Tuple<E> tuple, byte[] fromKey,
                byte[] toKey) {

            super(btree, tuple, fromKey, toKey);

        }

        @Override
        protected Leaf getLeafSpanningFromKey() {
            
            final byte[] key = getInclusiveLowerBound();
            
            if (key == null) {
                
                /*
                 * Descend to the left-most leaf.
                 */
                
                AbstractNode node = btree.getRoot();
                
                while(!node.isLeaf()) {
                    
                    node = ((Node)node).getChild(0);
                    
                }

                return (Leaf)node;
                
            }
            
            return getLeafSpanningKey( key );
            
        }

        /**
         * Descend from the root node to the leaf spanning that key. Note that
         * the leaf may not actually contain the key, in which case it is the
         * leaf that contains the insertion point for the key.
         */
        @Override
        protected Leaf getLeafSpanningKey(byte[] key) {

            AbstractNode node = btree.getRoot();
            
            while(!node.isLeaf()) {
                
                final int index = ((Node)node).findChild(key);
                
                node = ((Node)node).getChild( index );
                
            }

            return (Leaf)node;            
        }

        /**
         * Descend from the root node to the parent of the leaf spanning that
         * key (or spanning its insertion point).
         * 
         * @param key
         *            The key.
         * 
         * @return The parent of the leaf spanning that key -or-
         *         <code>null</code> iff there is only a root leaf (and hence
         *         no parent).
         */
        protected Node getParentOfLeafSpanningKey(byte[] key) {

            Node parent = null;

            AbstractNode node = btree.getRoot();
            
            while(!node.isLeaf()) {
                
                final int index = ((Node)node).findChild(key);
                
                node = ((Node)node).getChild( index );
                
            }
            
            return parent;
            
        }
        
        /**
         * Descend from the root to the right-most leaf. 
         */
        @Override
        protected Leaf getLeafSpanningToKey() {
            
            AbstractNode node = btree.getRoot();
            
            while(!node.isLeaf()) {
                
                node = ((Node) node).getChild(node.getKeyCount() - 1);
                
            }

            return (Leaf)node;
            
        }

        @Override
        protected ReadOnlyCursorPosition<E> newPosition(Leaf leaf, int index, byte[] key) {
            
            return new ReadOnlyCursorPosition<E>(this, leaf, index, key);
            
        }

        @Override
        protected ReadOnlyCursorPosition<E> newPosition(ICursorPosition<Leaf, E> p) {

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
        protected MutableCursorPosition<E> newPosition(Leaf leaf, int index, byte[] key) {
            
            return new MutableCursorPosition<E>(this, leaf, index, key);
            
        }

        @Override
        protected MutableCursorPosition<E> newPosition(ICursorPosition<Leaf, E> p) {

            return new MutableCursorPosition<E>( (MutableCursorPosition<E>) p );

        }

    }

}
