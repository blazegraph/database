/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.util;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * Utility class for operations on the public interfaces.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASUtil {

//    private static final Logger log = Logger.getLogger(GASUtil.class);

    /**
     * The average fan out of the frontier.
     * 
     * @param frontierSize
     *            The size of the frontier.
     * @param nedges
     *            The number of edges visited when mapping the operation across
     *            that frontier.
     * 
     * @return The average fan out.
     */
    public static double fanOut(final int frontierSize, final long nedges) {
    
        return ((int) (nedges * 10d / frontierSize)) / 10d;
    
    }

    /**
     * The traversed edges per second.
     * 
     * @param nedges
     *            The number of traversed edges.
     * @param elapsedNanos
     *            The elapsed time (nanoseconds).
     *            
     * @return The traversed edges per second.
     */
    public static long getTEPS(final long nedges, long elapsedNanos) {

        // avoid division by zero.
        if (elapsedNanos == 0)
            elapsedNanos = 1;

        // edges/nanosecond.
        final double tepns = ((double) nedges) / elapsedNanos;

        // scale units to edges/second.
        final double teps = tepns * TimeUnit.SECONDS.toNanos(1);

        // Round off to get rid of those nasty factions.
        final long r = Math.round(teps);

        return r;

    }

    /**
     * An empty vertex iterator.
     */
    @SuppressWarnings({ "unchecked" })
    public static final Iterator<Value> EMPTY_VERTICES_ITERATOR = EmptyIterator.DEFAULT;

    /**
     * An empty edge iterator.
     */
    @SuppressWarnings({ "unchecked" })
    public static final Iterator<Statement> EMPTY_EDGES_ITERATOR = EmptyIterator.DEFAULT;

}
