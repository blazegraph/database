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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractSubtaskStats {

    /**
     * The #of elements in the output chunks (not including any eliminated
     * duplicates).
     * <p>
     * Note: The {@link AtomicLong} provides an atomic update guarantee which
     * some of the unit tests rely on.
     */
    public final AtomicLong elementsOut = new AtomicLong(0);

    /**
     * The #of chunks written onto the index partition using RMI.
     * <p>
     * Note: The {@link AtomicLong} provides an atomic update guarantee which
     * some of the unit tests rely on.
     */
    public final AtomicLong chunksOut = new AtomicLong();

    /**
     * Elapsed time waiting for another chunk to be ready so that it can be
     * written onto the index partition.
     */
    public long elapsedChunkWaitingNanos = 0L;

    /**
     * Elapsed nanoseconds writing chunks on an index partition (RMI request).
     */
    public long elapsedChunkWritingNanos = 0L;

    /**
     * The average #of nanoseconds for a chunk to become ready so that it can be
     * written on the sink (this is an average of the totals to date, not a
     * moving average).
     */
    public double getAverageNanosPerWait() {

        final long chunksOut = this.chunksOut.get();
        
        return (chunksOut == 0L ? 0 : elapsedChunkWaitingNanos
                / (double) chunksOut);

    }

    /**
     * The average #of nanoseconds per chunk written on the sink (this is an
     * average of the totals to date, not a moving average).
     */
    public double getAverageNanosPerWrite() {
        
        final long chunksOut = this.chunksOut.get();
        
        return (chunksOut == 0L ? 0 : elapsedChunkWritingNanos
                / (double) chunksOut);

    }

    /**
     * The average #of elements (tuples) per chunk written on the sink (this is
     * an average of the totals to date, not a moving average).
     */
    public double getAverageElementsPerWrite() {

        final long chunksOut = this.chunksOut.get();

        final long elementsOut = this.elementsOut.get();

        return (chunksOut == 0L ? 0 : elementsOut / (double) chunksOut);

    }

    public AbstractSubtaskStats() {

    }

    public String toString() {

        return getClass().getName() + "{chunksOut=" + chunksOut
                + ", elementsOut=" + elementsOut
                + ", elapsedChunkWaitingNanos=" + elapsedChunkWaitingNanos
                + ", elapsedChunkWritingNanos=" + elapsedChunkWritingNanos
                + ", averageNanosPerWait=" + getAverageNanosPerWait()
                + ", averageNanosPerWrite=" + getAverageNanosPerWrite()
                + ", averageElementsPerWrite=" + getAverageElementsPerWrite()
                + "}";

    }

}
