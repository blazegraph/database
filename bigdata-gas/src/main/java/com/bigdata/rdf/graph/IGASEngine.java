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
 * The interface used to submit an {@link IGASProgram} for evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASEngine {

    /**
     * Obtain an execution context for the specified {@link IGASProgram}.
     * 
     * @param graphAccessor
     *            Indicates the graph to be processed.
     * @param program
     *            The program to execute against that graph.
     * 
     * @param <VS>
     *            The generic type for the per-vertex state. This is scoped to
     *            the computation of the {@link IGASProgram}.
     * @param <ES>
     *            The generic type for the per-edge state. This is scoped to the
     *            computation of the {@link IGASProgram}.
     * @param <ST>
     *            The generic type for the SUM. This is often directly related
     *            to the generic type for the per-edge state, but that is not
     *            always true. The SUM type is scoped to the GATHER + SUM
     *            operation (NOT the computation).
     */
    <VS, ES, ST> IGASContext<VS, ES, ST> newGASContext(
            IGraphAccessor graphAccessor, IGASProgram<VS, ES, ST> program);

    /**
     * Return <code>true</code> iff the frontier should be sorted. Backends that
     * benefit from an ordered frontier (e.g., to vector IOs) should return
     * <code>true</code>. Backends that do not benefit from an ordered frontier
     * (e.g., a backend based on hash collections in memory) should return
     * <code>false</code>.
     */
    boolean getSortFrontier();
    
    /**
     * Polite shutdown.
     */
    void shutdown();

    /**
     * Immediate shutdown.
     */
    void shutdownNow();

    /**
     * The parallelism for the SCATTER and GATHER phases.
     */
    int getNThreads();

    /*
     * Note: This is a problem since we then need to scope the SailConnection
     * internally.
     */
//    /**
//     * Access the default graph. In platforms that support multiple graphs, the
//     * accessed graph will be platform specific. (The sense of graph here is a
//     * triple or a quad store, not a named graph.)
//     */
//    IGraphAccessor newGraphAccessor();
    
}