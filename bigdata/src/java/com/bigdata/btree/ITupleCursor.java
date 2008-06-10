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

/**
 * Interface for random-access and cursor-based {@link ITuple} operations on a
 * local index or index partition.
 * <p>
 * Note: standard iterator semantics are maintained for the use case where a
 * forward scan is being performed. {@link #hasNext()} will return
 * <code>true</code> if the cursor position is undefined and there are
 * visitable tuples and {@link #next()} will visit the {@link #first()}
 * visitable tuple when the cursor position is undefined.
 * <p>
 * Note: Normally {@link ITupleCursor}s are provisioned such that deleted
 * tuples are not visited. This corresponds to the most common application
 * requirement. However, cursors MAY be provisioned to visit deleted tuples for
 * indices that maintain delete markers.
 * <p>
 * Note: When {@link #first()}, {@link #last()}, or {@link #seek(byte[])}
 * return <code>null</code> this is an indication that there is no visitable
 * tuple in the index corresponding to that request. However, you may still use
 * {@link #tuple()} to obtain a view onto the tuple associated with that
 * resulting cursor position. Such tuples always appear as if there were deleted
 * regardless of whether the index supports delete markers or not.
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
     * tuples.
     */
    public boolean isDeletedTupleVisitor();
    
    /**
     * Return <code>true</code> if the cursor position is defined.
     * <p>
     * Note: Some methods declared by this interface will throw an
     * {@link IllegalStateException} if the cursor position is not defined. The
     * cursor position may be defined using {@link #first()}, {@link #last()},
     * or {@link #seek(byte[])}. It is always defined if you obtained the
     * {@link ITupleCursor} using the {@link IRangeQuery} API. Once the cursor
     * position is defined, it is not possible for it to become undefined even
     * if the tuple at that cursor position is removed from the index. All that
     * happens is that the tuple at the cursor position will be flagged as
     * "deleted".
     */
    boolean isCursorPositionDefined();

    /**
     * The tuple reflecting the data in the index at the current cursor
     * position.
     * <p>
     * Note: When there is no tuple in the index corresponding to the current
     * cursor position the returned tuple will have the key corresponding to the
     * current cursor position but it will appear to be deleted.
     * 
     * @return The tuple associated with the current cursor position -or-
     *         <code>null</code> iff the current cursor position is undefined.
     */
    ITuple<E> tuple();

    /**
     * Position the cursor on the first visitable tuple in the natural index
     * order.
     * <p>
     * Note: When there is no visitable tuple then the cursor is positioned at
     * the optional inclusive lower bound for the {@link ITupleCursor}. If that
     * constraint is not defined, then the cursor is positioned at the optional
     * inclusive lower bound for the index partition. If that constraint is not
     * defined then the cursor is positioned on the key
     * <code>new byte[]{}</code>, which is the first allowable key for an
     * index.
     * 
     * @return The current tuple -or- <code>null</code> iff there is no tuple
     *         corresponding to the current cursor position.
     *         <p>
     *         Note that cursor position is always defined after invoking this
     *         method and that the tuple for the current cursor position may be
     *         recovered using {@link #tuple()} if this method returns
     *         <code>null</code>.
     */
    ITuple<E> first();

    /**
     * Position the cursor on the last visitable tuple in the natural index
     * order.
     * <p>
     * Note: When there are no visitable tuples the cursor will be positioned
     * using {@link #first()}. This is done since the upper bound is exclusive
     * and therefore can not be visited while the lower bound is inclusive and
     * defined even if there is no tuple associated with that key.
     * 
     * @return <code>true</code> if the cursor was positioned on a tuple.
     *         Otherwise the cursor is positioned using {@link #first()}.
     *         <p>
     *         Note that cursor position is always defined after invoking this
     *         method and that the tuple for the current cursor position may be
     *         recovered using {@link #tuple()} if this method returns
     *         <code>null</code>.
     */
    ITuple<E> last();

    /**
     * Positions the cursor on the specified key.
     * <p>
     * If there is a corresponding visitable tuple in the index then it is
     * returned.
     * <p>
     * If there is NOT a corresponding visitable tuple in the index
     * <code>null</code> is returned and the cursor is positioned on the first
     * visitable tuple occuring after the <i>key</i> in the natural order of
     * the index. If there is no visitable tuple in the index that is a
     * successor of the probe key then the cursor position is undefined.
     * 
     * @param key
     *            The key (required).
     * 
     * @return An object that can be used to read the tuple for the key iff the
     *         key exists and is visitable in the index and <code>null</code>
     *         otherwise.
     * 
     * @throws IllegalArgumentException
     *             if the key lies outside of the optional key-range constraint
     *             on the cursor or on the index partition.
     */
    ITuple<E> seek(byte[] key);

    /**
     * Variant that first encodes the key using the {@link ITupleSerializer}
     * declared on the {@link IndexMetadata} for the backing index.
     * 
     * @param key
     *            The key (required).
     * 
     * @return An object that can be used to read the tuple for the encoded key
     *         iff the encoded key exists and is visitable in the index and
     *         <code>null</code> otherwise.
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
     */
    ITuple<E> prior();

    /**
     * Removes the tuple (if any) from the index corresponding to the current
     * cursor position.
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
