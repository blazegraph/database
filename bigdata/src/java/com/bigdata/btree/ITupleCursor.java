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
 * This interface is intentionally kept small and does not include methods such as
 * first() or last() whose semantics would be misleading when applied to an
 * index partition vs a partitioned index.
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
public interface ITupleCursor<E> extends ITupleIterator<E> {

    /**
     * The backing index being traversed by the {@link ITupleCursor}.
     */
    IIndex getIndex();

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
     *             if the key is <code>null</code>.
     * @throws KeyOutOfRangeException
     *             if the key lies outside of the optional constrain on the
     *             {@link ITupleCursor}.
     * @throws KeyOutOfRangeException
     *             if the key lies outside of the key-range constraint on an
     *             index partition.
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
     *             if {@link #hasNext()} would return <code>false</code>.
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
     * Position the cursor on the first visitable tuple ordered less than the
     * current cursor position in the natural key order of the index and return
     * that tuple.
     * <p>
     * Note: in order to maintain semantics parallel to standard iterator
     * semantics, this method will visit the {@link #last()} visitable tuple if
     * the current cursor position is undefined.
     * 
     * @throws NoSuchElementException
     *             if {@link #hasPrior()} would return <code>false</code>.
     */
    ITuple<E> prior();

    /**
     * Removes the tuple (if any) from the index corresponding to the current
     * cursor position. The cursor position is NOT changed by this method. After
     * removing the current tuple, {@link ITupleCursor2#tuple()} will return
     * <code>null</code> to indicate that there is no tuple in the index
     * corresponding to the deleted tuple. (When delete markers are enabled and
     * deleted tuples are being visited, then {@link ITupleCursor2#tuple()} will
     * return the new state of the tuple with its delete marker set.)
     * 
     * @throws IllegalStateException
     *             if the cursor position is not defined.
     */
    void remove();

}
