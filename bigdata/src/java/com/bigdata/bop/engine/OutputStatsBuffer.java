/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jan 1, 2011
 */

package com.bigdata.bop.engine;

import java.util.concurrent.Future;

import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Delegation pattern handles the {@link SinkTransitionMetadata}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OutputStatsBuffer<E> implements IBlockingBuffer<E> {

    private final IBlockingBuffer<E> b;

    private final BOpStats stats;

    public OutputStatsBuffer(final IBlockingBuffer<E> b, final BOpStats stats) {

        this.b = b;

        this.stats = stats;

    }

    public IAsynchronousIterator<E> iterator() {
        return b.iterator();
    }

    public void setFuture(final Future future) {
        b.setFuture(future);
    }

    public void abort(final Throwable cause) {
        b.abort(cause);
    }

    public void close() {
        b.close();
    }

    public Future getFuture() {
        return b.getFuture();
    }

    public boolean isOpen() {
        return b.isOpen();
    }

    public long flush() {
        return b.flush();
    }

    /**
     * Tracks {@link BOpStats#unitsOut} and {@link BOpStats#chunksOut}.
     * <p>
     * Note: {@link BOpStats#chunksOut} will report the #of chunks added to this
     * buffer. However, the buffer MAY combine chunks either on add() or when
     * drained by the iterator so the actual #of chunks read back from the
     * iterator MAY differ.
     * <p>
     * {@inheritDoc}
     */
    public void add(final E e) {

        b.add(e);

        if (e.getClass().getComponentType() != null) {

            stats.unitsOut.add(((Object[]) e).length);

        } else {

            stats.unitsOut.increment();

        }

        stats.chunksOut.increment();

    }
    
    public boolean isEmpty() {
        return b.isEmpty();
    }

    public void reset() {
        b.reset();
    }

    public int size() {
        return b.size();
    }
    
}