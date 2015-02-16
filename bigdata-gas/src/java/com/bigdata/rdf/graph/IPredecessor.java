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

import org.openrdf.model.Value;

/**
 * A interface for {@link IGASProgram}s that compute paths and track a
 * predecessor relationship among the visited vertices. This interface can be
 * used to eliminate vertices from the visited set that are not on a path to a
 * set of specified target vertices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IPredecessor<VS, ES, ST> {

    /**
     * Remove any vertices from the visited set that do not line on path that
     * leads to at least one of the target vertices.
     * 
     * @param ctx
     *            The {@link IGASContext}.
     * @param targetVertices
     *            An array of zero or more target vertices.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    public void prunePaths(final IGASContext<VS, ES, ST> ctx,
            final Value[] targetVertices);

}
