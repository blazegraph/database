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
 * An interface for computing reductions over the vertices of a graph.
 * 
 * @param <T>
 *            The type of the result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IResultHandler.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public interface IReducer<VS, ES, ST, T> {

    /**
     * Method is invoked for each result and is responsible for combining the
     * results in whatever manner is meaningful for the procedure.
     * Implementations of this method MUST be <strong>thread-safe</strong>.
     * 
     * @param result
     *            The result from applying the procedure to a single index
     *            partition.
     */
    public void visit(IGASState<VS, ES, ST> state, Value u);

    /**
     * Return the aggregated results as an implementation dependent object.
     */
    public T get();

}
