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

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractSubtaskStats {

    /**
     * The #of elements in the output chunks (not including any eliminated
     * duplicates).
     */
    public long elementsOut = 0L;

    /**
     * The #of chunks written onto the index partition using RMI.
     */
    public long chunksOut = 0L;

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
     * written on the sink.
     */
    public double getAverageNanosPerWait() {

        return (chunksOut == 0L ? 0 : elapsedChunkWaitingNanos
                / (double) chunksOut);

    }

    /**
     * The average #of nanoseconds per chunk written on the sink.
     */
    public double getAverageNanosPerWrite() {

        return (chunksOut == 0L ? 0 : elapsedChunkWritingNanos
                / (double) chunksOut);

    }

    /**
     * The average #of elements (tuples) per chunk written on the sink.
     */
    public double getAverageElementsPerWrite() {

        return (chunksOut == 0L ? 0 : elementsOut / (double) chunksOut);

    }

    public AbstractSubtaskStats() {

    }

    public String toString() {

        return getClass().getName() + "{chunksOut=" + chunksOut
                + ", elementsOut=" + elementsOut
                + ", elapsedChunkWaitingNanos=" + elapsedChunkWaitingNanos
                + ", elapsedChunkWritingNanos=" + elapsedChunkWritingNanos
                + ", averageNanosPerWait=" + getAverageNanosPerWrite()
                + ", averageNanosPerWrite=" + getAverageNanosPerWrite()
                + ", averageElementsPerWrite=" + getAverageElementsPerWrite()
                + "}";

    }

}
