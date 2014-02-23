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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Execution context for an {@link IGASProgram}. This is distinct from the
 * {@link IGASEngine} so we can support distributed evaluation and concurrent
 * evaluation of multiple {@link IGASProgram}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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
 */
public interface IGASContext<VS, ES, ST> extends Callable<IGASStats> {

    /**
     * Return the program that is being evaluated.
     */
    IGASProgram<VS, ES, ST> getGASProgram();

    /**
     * The computation state.
     */
    IGASState<VS, ES, ST> getGASState();

    /**
     * The graph access object.
     */
    IGraphAccessor getGraphAccessor();

    /**
     * Specify the maximum number of iterations for the algorithm.
     * 
     * @param newValue
     *            The maximum number of iterations.
     * 
     * @throws IllegalArgumentException
     *             if the new value is non-positive.
     */
    void setMaxIterations(int newValue);

    /**
     * Return the maximum number iterations for the algorithm.
     */
    int getMaxIterations();

    /**
     * Specify the maximum number of vertices that may be visited. The algorithm
     * will halt if this value is exceeded.
     * 
     * @param newValue
     *            The maximum number of vertices in the frontier.
     * 
     * @throws IllegalArgumentException
     *             if the new value is non-positive.
     */
    void setMaxVisited(int newValue);

    /**
     * Return the maximum number of vertices that may be visited. The algorithm
     * will halt if this value is exceeded.
     */
    int getMaxVisited();

    /**
     * Execute one iteration.
     * 
     * @param stats
     *            Used to report statistics about the execution of the
     *            algorithm.
     * 
     * @return true iff the new frontier is empty.
     */
    boolean doRound(IGASStats stats) throws Exception, ExecutionException,
            InterruptedException;

    /**
     * Execute the associated {@link IGASProgram}.
     */
    @Override
    IGASStats call() throws Exception;

}