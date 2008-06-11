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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Interface for sequential and random-access cursor-based {@link ITuple}
 * operations on an index or index partition. The interface extends the standard
 * {@link Iterator} for forward sequential scans and also provides symmetric
 * methods for reverse sequential scans using {@link #hasPrior()} and
 * {@link #prior()}. Random access is supported using {@link #seek(byte[])}.
 * When {@link #first()}, {@link #last()}, or {@link #seek(byte[])} return
 * <code>null</code> this is an indication that there is no visitable tuple in
 * the index corresponding to that request. Likewise {@link #tuple()} will
 * return <code>null</code> if the cursor position does not correspond to a
 * tuple in the index or if the tuple at that cursor position has since been
 * deleted.
 * <p>
 * Note: Normally {@link ITupleCursor}s are provisioned such that deleted
 * tuples are not visited (they are skipped over if delete markers are used by
 * the index). This corresponds to the most common application requirement.
 * However, cursors MAY be provisioned using {@link IRangeQuery#DELETED} to
 * visit deleted tuples for indices that maintain delete markers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type for the de-serialized objects stored as the value
 *            under the key in the index.
 */
interface ITupleCursor<E> extends ITupleIterator<E> {

    /**
     * The backing index being traversed by the {@link ITupleCursor}.
     * 
     * @todo Make {@link IIndex} into a generic type for the backing index?
     */
    IIndex getIndex();

    /**
     * The optional inclusive lower bound imposed by the {@link ITupleCursor}.
     */
    byte[] getFromKey();

    /**
     * The optional exclusive upper bound imposed by the {@link ITupleCursor}.
     */
    byte[] getToKey();

    /**
     * Return <code>true</code> if the cursor is willing to visit deleted
     * tuples. In order to observe deleted tuples the index must have been
     * provisioned with support for delete markers enabled.
     * <p>
     * Note: When delete markers are enabled in the index and a tuple is
     * deleted, the tuple is NOT removed from the index. Instead a "delete"
     * marker is set and the value associated with the key is cleared to
     * <code>null</code>.
     * 
     * @see IndexMetadata#getDeleteMarkers()
     */
    public boolean isDeletedTupleVisitor();
    
    /**
     * Return <code>true</code> if the cursor position is defined.
     * <p>
     * Note: Use {@link #currentKey()} to obtain the key corresponding to the
     * current cursor position and {@link #tuple()} to obtain the visitable
     * tuple in the index corresponding to that cursor position (if any).
     */
    boolean isCursorPositionDefined();

    /**
     * Return the key corresponding to the current cursor position (even if
     * there is no tuple in the index for that key).
     * 
     * @return The key corresponding to the current current position -or-
     *         <code>null</code> iff the cursor position is undefined.
     */
    byte[] currentKey();
    
    /**
     * The tuple reflecting the data in the index at the current cursor
     * position.
     * 
     * @return The tuple associated with the current cursor position -or-
     *         <code>null</code> either if there is no visitable tuple
     *         corresponding to the current cursor position or if the current
     *         cursor position is undefined.
     */
    ITuple<E> tuple();

    /**
     * Position the cursor on the first visitable tuple in the natural index
     * order.  If there are no visitable tuples then the cursor position will
     * be undefined.
     * 
     * @return The current tuple -or- <code>null</code> iff there is no
     *         visitable tuple corresponding to the current cursor position.
     */
    ITuple<E> first();

    /**
     * Position the cursor on the last visitable tuple in the natural index
     * order. If there are no visitable tuples then the cursor position will be
     * undefined.
     * 
     * @return <code>true</code> if the cursor was positioned on a tuple.
     */
    ITuple<E> last();

    /**
     * Positions the cursor on the specified key.
     * <p>
     * If there is a corresponding visitable tuple in the index then it is
     * returned.
     * <p>
     * If there is no visitable tuple in the index for that <i>key</i> then
     * <code>null</code> is returned. You can use {@link #prior()} or
     * {@link #next()} to locate the first visitable tuple to either side of the
     * cursor position.
     * <p>
     * The cursor position is updated to the specified <i>key</i> regardless of
     * whether there is a visitable tuple in the index for that key.
     * 
     * @param key
     *            The key (required).
     * 
     * @return The tuple corresponding to <i>key</i> if it exists and is
     *         visitable in the index and <code>null</code> otherwise.
     * 
     * @throws IllegalArgumentException
     *             if the key lies outside of the optional key-range constraint
     *             on the cursor or on the index partition.
     */
    ITuple<E> seek(byte[] key);

    /**
     * Variant that first encodes the key using the object returned by
     * {@link IndexMetadata#getTupleSerializer()} for the backing index.
     * 
     * @param key
     *            The key (required).
     * 
     * @return The tuple corresponding to the encoded <i>key</i> if it exists
     *         and is visitable in the index and <code>null</code> otherwise.
     * 
     * @throws IllegalArgumentException
     *             if the encoded key lies outside of the optional key-range
     *             constraint on the cursor or on the index partition.
     */
    ITuple<E> seek(Object key);

    /**
     * Return <code>true</code> if there is another tuple that orders after
     * the current cursor position in the natural order of the index and that
     * lies within the optional constraints key-range on the cursor or on the
     * index partition.
     * <p>
     * Note: in order to maintain standard iterator semantics, this method will
     * return <code>true</code> if the current cursor position is undefined
     * and {@link #first()} would report the existence of a visitable tuple.
     */
    boolean hasNext();

    /**
     * Position the cursor on the next tuple in the natural key order of the
     * index.
     * <p>
     * Note: in order to maintain standard iterator semantics, this method will
     * visit the {@link #first()} visitable tuple if the current cursor position
     * is undefined.
     * 
     * @throws NoSuchElementException
     *             If the current cursor position is undefined and there are no
     *             visitable tuples in the index.
     * @throws NoSuchElementException
     *             if the current cursor position is defined but there is no
     *             visitable tuple that is a successor of the current cursor
     *             position in the natural order of the index.
     * 
     * @todo these two exceptions can both be summarized by "if hasNext() would
     *       return false".
     */
    ITuple<E> next();

    /**
     * Return <code>true</code> if there is another tuple that orders before
     * the current cursor position in the natural order of the index and that
     * lies within the optional key-range constraints on the cursor or on the
     * index partition.
     * <p>
     * Note: in order to maintain semantics parallel to standard iterator
     * semantics, this method will return <code>true</code> if the current
     * cursor position is undefined and {@link #last()} would report the
     * existence of a visitable tuple.
     */
    boolean hasPrior();

    /**
     * Position the cursor on the previous tuple in the natural key order of the
     * index.
     * <p>
     * Note: in order to maintain semantics parallel to standard iterator
     * semantics, this method will visit the {@link #last()} visitable tuple if
     * the current cursor position is undefined.
     * 
     * @throws NoSuchElementException
     *             If the current cursor position is undefined and there are no
     *             visitable tuples in the index.
     * @throws NoSuchElementException
     *             if the current cursor position is defined but there is no
     *             visitable tuple that is a predecessor of the current cursor
     *             position in the natural order of the index.
     *             
     * @todo these two exceptions can both be summarized by "if hasNext() would
     *       return false".
     */
    ITuple<E> prior();

    /**
     * Removes the tuple (if any) from the index corresponding to the current
     * cursor position. The cursor position is NOT changed by this method. After
     * removing the current tuple, {@link #tuple()} will return
     * <code>null</code> to indicate that there is no tuple in the index
     * corresponding to the deleted tuple. (When delete markers are enabled and
     * deleted tuples are being visited, then {@link #tuple()} will return the
     * new state of the tuple with its delete marker set.)
     * 
     * @throws IllegalStateException
     *             if the cursor position is not defined.
     */
    void remove();

    /**
     * Return an iterator that traverses the tuples in the reverse of the
     * natural index order. The iterator is backed by the {@link ITupleCursor}
     * and operations on the iterator effect the state of the cursor and visa
     * versa.
     */
    ITupleIterator<E> asReverseIterator();

}
