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
 * Created on Jun 20, 2008
 */

package com.bigdata.relation.accesspath;

/**
 * An unsynchronized buffer backed by a fixed capacity array that migrates
 * references onto the caller's buffer (which is normally thread-safe) using
 * {@link IBuffer#add(int)}.
 * <p>
 * <strong>This implementation is NOT thread-safe.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnsynchronizedArrayBuffer<E> extends
        AbstractUnsynchronizedArrayBuffer<E> {

    /**
     * The buffer onto which chunks are evicted by {@link #overflow()}.
     */
    private final IBuffer<E[]> target;

    /**
     * @param target
     *            The target buffer onto which the elements will be flushed.
     * @param capacity
     *            The capacity of the backing buffer.
     */
    public UnsynchronizedArrayBuffer(final IBuffer<E[]> target,
            final int capacity) {

        this(target, capacity, null/* filter */);

    }

    /**
     * @param target
     *            The target buffer onto which chunks of elements will be
     *            flushed.
     * @param capacity
     *            The capacity of the backing buffer.
     * @param filter
     *            Filter to keep elements out of the buffer (optional).
     */
    public UnsynchronizedArrayBuffer(final IBuffer<E[]> target,
            final int capacity, final IElementFilter<E> filter) {

        super(capacity, filter);

        if (target == null)
            throw new IllegalArgumentException();

        this.target = target;
    }

    /** Add the chunk to the target buffer. */
    final protected void handleChunk(final E[] chunk) {

        target.add(chunk);

    }

}
