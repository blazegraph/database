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

import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGraphAccessor;

/**
 * A fixture for a graph.
 */
public interface IGraphFixture {

    /**
     * Return the provisioned {@link Sail}.
     */
    Sail getSail();

    /**
     * Destroy the provisioned {@link Sail}.
     */
    void destroy() throws Exception;

    /**
     * Load a graph into the test harness.
     * 
     * @param resource
     *            Zero or more resources to be loaded.
     * 
     * @throws Exception
     * 
     */
    void loadGraph(String... resource) throws Exception;

    IGASEngine newGASEngine(int nthreads);

    /**
     * 
     * @param cxn
     *            A connection that will be used to read on the graph.
     * @return
     * 
     *         TODO DYNAMIC GRAPHS: This does not support dynamic graph patterns
     *         because the view is determined by the connection that is
     *         supplied. For bigdata, dynamic graphs are achieved by providing
     *         the means to identify the graph of interest and then advancing
     *         the commit time view of the graph before each GAS evaluation
     *         round.
     */
    IGraphAccessor newGraphAccessor(SailConnection cxn);

}