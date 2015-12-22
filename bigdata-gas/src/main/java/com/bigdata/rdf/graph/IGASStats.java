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
package com.bigdata.rdf.graph;

/**
 * Statistics for GAS algorithms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASStats {

    void add(final long frontierSize, final long nedges, final long elapsedNanos);

    void add(final IGASStats o);

    long getNRounds();

    /**
     * The cumulative size of the frontier across the iterations.
     */
    long getFrontierSize();

    /**
     * The number of traversed edges across the iterations.
     */
    long getNEdges();

    /**
     * The elapsed nanoseconds across the iterations.
     */
    long getElapsedNanos();

}
