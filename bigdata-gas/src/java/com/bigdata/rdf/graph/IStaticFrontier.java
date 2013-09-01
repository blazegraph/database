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

import java.util.Iterator;

import org.openrdf.model.Value;

/**
 * Interface abstracts the fixed frontier as known on entry into a new
 * round.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public interface IStaticFrontier extends Iterable<Value> {

    /**
     * The number of vertices in the frontier.
     * 
     * TODO Long? Or just do not allow in scale-out?
     */
    int size();

    /**
     * Return <code>true</code> if the frontier is known to be empty.
     */
    boolean isEmpty();
    
    /**
     * Return <code>true</code> iff the frontier is known to be compact (no
     * duplicate vertices).
     * <p>
     * Note: If the frontier is not compact, then the {@link IGASEngine} may
     * optionally elect to eliminate duplicate work when it schedules the
     * vertices in the frontier.
     * <p>
     * Note: A non-compact frontier can arise when the {@link IGASScheduler}
     * chooses a per-thread approach and then copies the per-thread segments
     * onto the shared backing array in parallel. This can reduce the time
     * between rounds, which can speed up the overall execution of the algorithm
     * significantly.
     */
    boolean isCompact();

    /**
     * Reset the frontier from the supplied vertices.
     * 
     * @param minCapacity
     *            The minimum capacity of the new frontier. (A minimum capacity
     *            is specified since many techniques to compact the frontier can
     *            only estimate the required capacity.)
     * @param ordered
     *            <code>true</code> iff the vertices are not known to be ordered
     *            and SHOULD be ordered (some backends do not benefit from
     *            ordering the vertices).
     * @param vertices
     *            The vertices in the new frontier.
     */
    void resetFrontier(int minCapacity, boolean sort, Iterator<Value> vertices);

}
