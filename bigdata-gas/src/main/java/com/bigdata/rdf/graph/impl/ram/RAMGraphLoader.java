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
package com.bigdata.rdf.graph.impl.ram;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandlerException;

import com.bigdata.rdf.graph.impl.ram.RAMGASEngine.RAMGraph;
import com.bigdata.rdf.graph.util.GraphLoader;

/**
 * TODO Blank nodes have global scope. We should have a bnode resolution scope
 * to the source document to be consistent with RDF semantics. This could be
 * imposed through the {@link RAMStatementHandler}.
 * 
 * TODO It is not possible to parallelize the load since the mutation operations
 * on the {@link RAMGraph} assume a single writer (there is no built in
 * synchronization). This makes load performance quite slow.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RAMGraphLoader extends GraphLoader {

    private final RAMGraph g;

    public RAMGraphLoader(final RAMGraph g) {
        if (g == null)
            throw new IllegalArgumentException();
        this.g = g;
    }

    @Override
    protected AddStatementHandler newStatementHandler() {
        return new RAMStatementHandler();
    }

    private class RAMStatementHandler extends AddStatementHandler {

        @Override
        protected void addStatement(final Statement stmt, final Resource[] c)
                throws RDFHandlerException {

            g.add(stmt);

            ntriples++;

        }

    }

    @Override
    protected ValueFactory getValueFactory() {
        return g.getValueFactory();
    }
    
}
