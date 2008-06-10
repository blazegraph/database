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

import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;
import com.bigdata.btree.Leaf.ILeafListener;
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
 * FIXME Either modify the API for {@link AbstractBTree#lookup(byte[], Tuple)}
 * to return an {@link ITupleCursor} or add a method iterator(byte[] key, Tuple
 * tuple) that can be used to obtain a cursor initialized with the specified
 * starting key or just use {@link IRangeQuery#REVERSE} (rename as LAST_KEY?) to
 * pre-position the scan at the last key then let people use the strengthened
 * return type of {@link ITupleCursor} to access handle prior/next access. Note
 * that the {@link ClientIndexView} will need to be aware of this flag so that
 * it can process the index partitions in the reverse of their natural key
 * order.
 * 
 * FIXME See {@link AbstractTupleFilterator} for many, many notes about the new
 * interator constructs and things that can be changed once they are running,
 * including the implementation of the prefix scans, etc.
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

    final public ITuple<E> seek(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        return seek(getIndex().getIndexMetadata().getTupleSerializer()
                .serializeKey(key));
        
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
        
        this.btree = btree;
        
        this.tuple = tuple;
        
        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
        this.visitDeleted = ((tuple.flags() & IRangeQuery.DELETED) != 0);

        // Note: the cursor position is NOT defined!
        currentPosition = nextPosition = priorPosition = null;
        
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
     * The optional exclusive lower bound. If the <i>toKey</i> constraint was
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
     * Return a new {@link ICursorPosition} that is initially positioned on
     * the first tuple in the key-range (does not skip over deleted tuples).
     */
    protected AbstractCursorPosition<L,E> firstPosition() {

        final byte[] key = getInclusiveLowerBound();
        
        final L leaf = getLeafSpanningFromKey();
        
        return newPosition(leaf, key);
        
    }
    
    /**
     * Return a new {@link ICursorPosition} from the leaf and the tuple index.
     * 
     * @param leaf
     *            The leaf.
     * @param index
     *            The tuple index within that leaf.
     * 
     * @return The new {@link ICursorPosition}.
     */
    abstract protected AbstractCursorPosition<L, E> newPosition(L leaf, int index);

    /**
     * Return a clone of the given {@link ICursorPosition}.
     * 
     * @param p
     *            The cursor position.
     *            
     * @return A clone of that cursor position.
     */
    abstract protected AbstractCursorPosition<L, E> newPosition(ICursorPosition<L, E> p);
    
    /**
     * Return a new {@link ICursorPosition} that is initially positioned on
     * the first tuple found in the leaf whose key is GTE to the given
     * <i>key</i> (does not skip over deleted tuples).
     * 
     * @param leaf
     *            A leaf.
     * @param key
     *            A key that is spanned by that leaf (MAY be null in which
     *            case the first key in the index will be visited).
     * 
     * @return The new {@link ICursorPosition}.
     */
    protected AbstractCursorPosition<L,E> newPosition(L leaf, byte[] key) {
        
        assert leaf != null;
        
//        assert key != null;
        
        /*
         * Find the starting tuple index for the key in that leaf.
         * 
         * Note: this will give us the insertion point if the key is not
         * found in the leaf, in which case we convert it into the index
         * of the first tuple ordered after the key.
         */

        int index;
        
        if (key == null) {
            
            index = 0;
            
        } else {

            // find the position of the key in the leaf.
            index = leaf.getKeys().search(key);

            if (log.isInfoEnabled())
                log.info("position=" + index);
            
            if (index < 0) {

                /*
                 * Convert the insert position into the index of the
                 * successor.
                 * 
                 * Note: This can result in index == nkeys, in which case
                 * the first tuple of interest does not actually lie within
                 * the starting leaf.
                 */

                index = -index - 1;

            }

        }

        if (log.isInfoEnabled())
            log.info("index=" + index);

        final AbstractCursorPosition<L,E> pos = newPosition(leaf, index);

        return pos;
        
    }
    
    /**
     * Return a new {@link CursorPosition} that is initially positioned on
     * the last tuple in the key-range (does not skip over deleted tuples).
     * 
     * @return The leaf spanning {@link #getExclusiveUpperBound()}.
     */
    protected AbstractCursorPosition<L,E> lastPosition() {

        final L leaf = getLeafSpanningToKey();
        
        final byte[] key = getExclusiveUpperBound();
        
        int index;
        
        if (key == null) {
            
            if(leaf.getKeyCount() == 0) {

                // This is the root leaf and the B+Tree is empty.
                index = 0;
                
            } else {
            
                // use the last key in the leaf.
                index = leaf.getKeyCount() - 1;
                
            }
            
        } else {

            // find the position of the key in the leaf.
            index = leaf.getKeys().search(key);

            if (log.isInfoEnabled())
                log.info("position=" + index);

            if (index < 0) {

                /*
                 * Convert the insert position into the index of the
                 * predecessor.
                 */

                index = -index - 1;

            } else {

                index = index - 1;
                
            }

        }

        if (log.isInfoEnabled())
            log.info("index=" + index);

        final AbstractCursorPosition<L,E> pos = newPosition(leaf, index);

        return pos;

    }

    public ITuple<E> tuple() {
        
        if(!isCursorPositionDefined()) {
            
            // the cursor position is not defined.
            return null;
            
        }
        
        return tuple;
        
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

        // new position GTE that key.
        currentPosition = newPosition(getLeafSpanningKey(key), key);

        // Scan until we reach the first visitable tuple.
        if (!currentPosition.forwardScan(false/*skipCurrent*/)) {
        
            // discard the current position since we have seeked beyond the end of the index / key range.
            currentPosition = null;
            
            // Nothing visitable.
            return null;

        }

        // Copy the data into [tuple].
        currentPosition.get(tuple);

        if (BytesUtil.compareBytes(key, currentPosition.getKey()) != 0) {
            
            // Found a successor of the probe key.
            return null;
            
        }

        // Return [tuple].
        return tuple;
        
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
             * 
             * FIXME Verify this setup and then replicate to first(), last(),
             * seek(), tuple(), etc. It allows us to use the context of the
             * request to decide how to handle a tuple that is no longer in the
             * index, which could be different for a forward and reverse scan.
             */

            if(!currentPosition.leafValid) {
                
                currentPosition.relocateLeaf();

                nextPosition = currentPosition;
                
            } else {

                nextPosition = newPosition(currentPosition);

            }
        
            if (!nextPosition.forwardScan(true/* skipCurrent */)) {

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
         * Note: The ILeafListener will note that on the tuple that it was
         * deleted from the index.
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
         * Copy the data from the tuple at the {@link ICursorPosition} into the
         * caller's buffer.
         * <p>
         * Note: If the {@link ICursorPosition} is not on a tuple then this
         * method will return <code>null</code> rather than the caller's
         * tuple. This situation can arise if the index is empty (e.g., a root
         * leaf with nothing in it) or if the caller fails to scan to the
         * next/prior visitable tuple when establishing a new
         * {@link ICursorPosition}.
         * 
         * @param tuple
         *            The caller's buffer.
         * 
         * @return The caller's buffer -or- <code>null</code> if the
         *         {@link ICursorPosition} is not on a tuple.
         */
        public Tuple<E> get(Tuple<E> tuple);

        /**
         * Return the key corresponding to the cursor position.
         * 
         * @return The key -or- <code>null</code> if the
         *         {@link ICursorPosition} is not on a tuple.
         * 
         * @see #get(Tuple)
         */
        public byte[] getKey();

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
         * Create position on the specified key, or on the successor of the
         * specified key if that key is not found in the index.
         * 
         * @param cursor
         *            The {@link ITupleCursor}.
         * @param leaf
         *            The current leaf.
         * @param index
         *            The index of the tuple in the <i>leaf</i>.
         */
        protected AbstractCursorPosition(ITupleCursor<E> cursor,L leaf, int index) {
            
            if (cursor == null)
                throw new IllegalArgumentException();

            if (leaf == null)
                throw new IllegalArgumentException();

            /*
             * Note: The <i>index</i> MAY be equal to the #of keys in the leaf,
             * in which case the {@link CursorPosition} MUST be understood to
             * lie in the next leaf (if it exists). The scanForward() method
             * handles this case automatically.
             */
            if(index < 0 || index > leaf.getKeyCount()) {
                
                throw new IllegalArgumentException("index="+index+", nkeys="+leaf.getKeyCount());
                
            }

            this.leafValid = true;
            
            this.cursor = cursor;
            
            this.leaf = leaf;
            
            this.index = index;
            
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
            
        }
        
        public String toString() {
            
            return "CursorPosition{" + cursor + ", leaf=" + leaf + ", index="
                    + index + ", leafValid="+leafValid+"}";
            
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

            if (index == leaf.getKeyCount()) {

                return false;
                
            }
            
            return true;

        }
        
        public Tuple<E> get(Tuple<E> tuple) {

            relocateLeaf();
            
            if(!isOnTuple()) return null;
            
            tuple.copy(index, leaf); // Note: increments [tuple.nvisited] !!!

            return tuple;
            
        }

        public byte[] getKey() {
            
            relocateLeaf();
            
            if(!isOnTuple()) return null;
            
            return leaf.getKeys().getKey(index);
            
        }

        public boolean isVisitableTuple() {
            
            relocateLeaf();
            
            /*
             * Either delete markers are not supported or we are visiting
             * deleted tuples or the tuple is not deleted.
             */
            
            return !leaf.hasDeleteMarkers() || cursor.isDeletedTupleVisitor()
                    || !leaf.getDeleteMarker(index);

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
         * @throws UnsupportedOperationException
         *             if {@link #leafValid} is <code>false</code>
         */
        protected void relocateLeaf() {
        
            if(leafValid) return;
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Materialize the next leaf in the natural order of the index.
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
         * Materialize the prior leaf in the natural order of the index.
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
            
            /*
             * Scan leafs until we find a visitable tuple.
             * 
             * Note: If [index >= nkeys] then the scan will immediately skip to
             * the next leaf (if any) in the index.
             */
            
            if (skipCurrent) {

                index++;
                
            }
            
            // set true iff the scan exceeds the optional exclusive upper bound.
            boolean done = false;
            
            while (!done) {

                final int nkeys = leaf.getKeyCount();

                /*
                 * Scan tuples in the current leaf until we find a visitable
                 * tuple.
                 */
                for (; index < nkeys; index++, ntuples++) {

                    if (cursor.getToKey() != null) {

                        /*
                         * Done if GTE the toKey
                         */

                        final byte[] t = leaf.getKeys().getKey(index);

                        if (BytesUtil.compareBytes(cursor.getToKey(), t) >= 0) {

                            done = true;

                            if(log.isInfoEnabled()) 
                                log.info("Scan GTE optional exclusive upper bound: leavesScanned="
                                                + nleaves
                                                + ", tuplesScanned="
                                                + ntuples);
                            
                            break;
                            
                        }
                        
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
        
        public boolean reverseScan(boolean skipCurrent) {

            relocateLeaf();
            
            if (leaf.getKeyCount() == 0) {

                /*
                 * This happens when there is an empty root leaf. We have to
                 * test for this case explicitly since the for() loop below
                 * allows index == 0.
                 */

                return false;
                
            }
            
            int nleaves = 0;
            int ntuples = 0;
            
            /*
             * Scan leafs until we find a visitable tuple.
             * 
             * Note: If [index < 0] then the scan will immediately skip to the
             * prior leaf (if any) in the index.
             */
            
            if (skipCurrent) {

                index--;
                
            }
            
            // set true iff the scan reaches or exceeds the optional inclusive lower bound.
            boolean done = false;
            
            while (!done) {

                /*
                 * Scan tuples in the current leaf until we find a visitable
                 * tuple.
                 */
                
                for (; index >= 0; index--, ntuples++) {

                    final byte[] fromKey = cursor.getFromKey();
                    
                    if (fromKey != null) {

                        /*
                         * Done if LT then fromKey
                         */

                        final byte[] t = leaf.getKeys().getKey(index);

                        if (BytesUtil.compareBytes(fromKey, t) < 0) {

                            done = true;

                            if(log.isInfoEnabled()) 
                                log.info("Scan LE optional inclusive upper bound: leavesScanned="
                                                + nleaves
                                                + ", tuplesScanned="
                                                + ntuples);
                            
                            break;
                            
                        }
                        
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
         */
        public ReadOnlyCursorPosition(ITupleCursor<E> cursor, Leaf leaf, int index) {

            super(cursor, leaf, index);
            
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
        protected MutableCursorPosition(ITupleCursor<E> cursor, Leaf leaf, int index) {

            super(cursor, leaf, index);
            
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

            if (index == this.index) {

                /*
                 * Note: The tuple whose state was changed is the tuple
                 * corresponding to this cursor position.
                 */
                
                final MutableBTreeTupleCursor cursor = (MutableBTreeTupleCursor<E>)this.cursor;
                
                if(cursor.currentPosition == this) {
                    
                    /*
                     * Note: This is the _current_ cursor position (vs the prior
                     * or next cursor position) so update the state of the tuple
                     * from the leaf.
                     */
                    
                    if (get(cursor.tuple) == null) {
                        
                        throw new AssertionError();
                        
                    }
                    
                }
                
            }
            
        }

        protected void relocateLeaf() {
            
            if(leafValid) return;
            
            if (!isOnTuple()) {
                
                /*
                 * FIXME What do to if the cursor position was not on a tuple
                 * for the old leaf? If the btree is empty, then we can just use
                 * the root leaf. Otherwise I am not sure exactly what leaf
                 * needs to be located nor what tuple on that leaf should be
                 * located! Write some unit tests that trigger this condition
                 * and figure out what to do.
                 */

                throw new AssertionError("index="+index);
                
            }
            
            final byte[] key = leaf.getKeys().getKey(index);

            if(log.isInfoEnabled()) log.info("Relocating leaf: key="+BytesUtil.toString(key));
            
            // re-locate the leaf.
            leaf = ((MutableBTreeTupleCursor<E>) cursor)
                    .getLeafSpanningKey(key);

            // register the cursor position as a listener for the new leaf.
            leaf.addLeafListener(this);
            
            // Re-locate the tuple index for the key in that leaf.
            index = leaf.getKeys().search(key);
            
            if (index < 0) {

                /*
                 * Note: We have re-located the leaf that spans the [key] but
                 * there is no longer any tuple in the leaf for that [key].
                 * 
                 * Convert the insertion point into an index into the leaf.
                 * 
//                 * Note: We choose the index in the leaf that corresponds to the
//                 * successor of the tuple. Since deleting the tuple created a
//                 * gap (at least logically) in the leaf, the successor is the
//                 * tuple that filled that gap. Since the successor was already
//                 * moved down the [-1] is commented out below in order to give
//                 * us the correct index for the successor.
                 */

                index = -index - 1; // @todo verify this.

                /*
                 * Since the tuple is not found we may need to mark the tuple on
                 * the cursor as deleted (while this is synchronous just like
                 * when the tuple state is updated, the [leafValid] flag itself
                 * is NOT handled synchronously so the update will in fact be
                 * delayed).
                 */
                
                final MutableBTreeTupleCursor cursor = (MutableBTreeTupleCursor<E>)this.cursor;
                
                if(cursor.currentPosition == this) {

                    /*
                     * Note: The tuple whose state was changed is the tuple
                     * corresponding to the _current_ cursor position so flag
                     * that tuple as deleted now that it no longer appears in
                     * the leaf.
                     */

                    cursor.tuple.markDeleted();
                    
                }
                
            }

            leafValid = true;
            
        }
        
    }
    
    /**
     * An {@link ITuple} that directly supports forward and reverse cursor
     * operations on a local {@link BTree}.
     * 
     * FIXME Modify {@link AbstractBTree} to use an {@link ITupleCursor} without
     * the post-order striterator (but we still need post-order traversal for
     * flushing evicted nodes to the store!)
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
        protected ReadOnlyCursorPosition<E> newPosition(Leaf leaf, int index) {
            
            return new ReadOnlyCursorPosition<E>(this, leaf, index);
            
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
     * FIXME get rid of the {@link ChunkedLocalRangeIterator}. Make sure that
     * the {@link IRangeQuery#REMOVEALL} flag is correctly interpreted as not
     * allowing more than [capacity] tuples to be deleted.
     * 
     * @todo Support direct mutation of the tuple in the index corresponding to
     *       the current cursor position using a mutable {@link ITuple}? (You
     *       can always do an insert or remove on the index using
     *       {@link ITuple#getKey()} so this is only a convenience).
     * 
     * @todo The returned tuple is always the tuple supplied to the ctor so a
     *       concurrent modification will cause the state of the tuple already
     *       in the caller's hands to be immediately changed (the change will be
     *       synchronous). This "feature" should be documented and should be
     *       observable for the {@link FusedView}'s cursor as well (in fact it
     *       is only synchronous when a tuple is updated NOT when the leaf is
     *       invalidated).
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
        protected MutableCursorPosition<E> newPosition(Leaf leaf, int index) {
            
            return new MutableCursorPosition<E>(this, leaf, index);
            
        }

        @Override
        protected MutableCursorPosition<E> newPosition(ICursorPosition<Leaf, E> p) {

            return new MutableCursorPosition<E>( (MutableCursorPosition<E>) p );

        }

    }

}
