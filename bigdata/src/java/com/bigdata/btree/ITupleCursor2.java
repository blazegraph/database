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
 * Created on Aug 9, 2008
 */

package com.bigdata.btree;

/**
 * Extended interface.
 * <p>
 * When {@link #first()}, {@link #last()}, or
 * {@link ITupleCursor#seek(byte[])} return <code>null</code> it is an
 * indication that there is no visitable tuple in the index corresponding to
 * that request. Likewise {@link #tuple()} will return <code>null</code> if
 * the cursor position does not correspond to a tuple in the index or if the
 * tuple at that cursor position has since been deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITupleCursor2<E> extends ITupleCursor<E> {

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
     * Position the cursor on the first visitable tuple in the natural index
     * order for the index or index partition over which the cursor is defined.
     * If there are no visitable tuples then the cursor position will be
     * undefined.
     * 
     * @return The current tuple -or- <code>null</code> iff there is no
     *         visitable tuple corresponding to the current cursor position.
     */
    ITuple<E> first();

    /**
     * Position the cursor on the last visitable tuple in the natural index
     * order for the index or index partition over which the cursor is defined.
     * If there are no visitable tuples then the cursor position will be
     * undefined.
     * 
     * @return <code>true</code> if the cursor was positioned on a tuple.
     */
    ITuple<E> last();

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
     * Position the cursor on the first visitable tuple ordered greater than the
     * current cursor position in the natural key order of the index and return
     * that tuple.
     * 
     * @return The tuple -or- <code>null</code> iff there is no such visitable
     *         tuple.
     */
    ITuple<E> nextTuple();
    
    /**
     * Position the cursor on the first visitable tuple ordered less than the
     * current cursor position in the natural key order of the index and return
     * that tuple.
     * 
     * @return The tuple -or- <code>null</code> iff there is no such visitable
     *         tuple.
     */
    ITuple<E> priorTuple();
    
//  /**
//  * Change the half-open range for the cursor. This can be useful if you want
//  * to perform a series of key-range scans. If the {@link #currentKey()} is
//  * no longer within the bounds for the cursor it will not be on a visitable
//  * tuple after invoking this method and you must {@link #seek(byte[])} to a
//  * key in the new half-open range before you can use the sequential access
//  * methods or visit the current tuple.
//  * <p>
//  * Note: The bounds may be constrained or relaxed, but never relaxed beyond
//  * those given when the cursor was provisioned. For example, if you
//  * initially specify neither a lower bound nor an upper bound then the
//  * bounds may be set to any half-open range. However, if there is a lower
//  * bound then you can never shift the lower bound to a lessor value so as to
//  * expose additional keys. Likewise, if there is an upper bound then you can
//  * never shift the upper bound to a greater value so as to expose additional
//  * keys.
//  * 
//  * @param fromKey
//  *            The optional inclusive lower bound imposed by the
//  *            {@link ITupleCursor}.
//  * @param toKey
//  *            The optional exclusive upper bound imposed by the
//  *            {@link ITupleCursor}.
//  */
// void bounds(byte[] fromKey, byte[] toKey);

}
