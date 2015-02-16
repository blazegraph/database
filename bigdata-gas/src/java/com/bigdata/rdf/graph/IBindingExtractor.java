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

import java.util.List;

/**
 * This interface makes it possible to extract bindings for variables from an
 * {@link IGASProgram}.
 * 
 * @param <VS>
 *            The generic type for the per-vertex state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ES>
 *            The generic type for the per-edge state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ST>
 *            The generic type for the SUM. This is often directly related to
 *            the generic type for the per-edge state, but that is not always
 *            true. The SUM type is scoped to the GATHER + SUM operation (NOT
 *            the computation).
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IBindingExtractor<VS, ES, ST> {

    /**
     * Return a list of interfaces that may be used to extract variable bindings
     * for the vertices visited by the algorithm.
     */ 
    List<IBinder<VS, ES, ST>> getBinderList();

}
