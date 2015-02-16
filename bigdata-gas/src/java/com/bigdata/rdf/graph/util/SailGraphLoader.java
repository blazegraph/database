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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

public class SailGraphLoader extends GraphLoader {

    /**
     * The connection used to load the data.
     */
    private final SailConnection cxn;

    /**
     * 
     * @param cxn
     *            The connection used to load the data.
     */
    public SailGraphLoader(final SailConnection cxn) {

        this.cxn = cxn;

    }

    @Override
    protected AddStatementHandler newStatementHandler() {

        return new SailStatementHandler();
        
    }

    private class SailStatementHandler extends AddStatementHandler {

        @Override
        protected void addStatement(final Statement stmt, final Resource[] c)
                throws RDFHandlerException {

            try {

                cxn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        c);

                if (c == null || c.length == 0)
                    ntriples++;
                else
                    ntriples += c.length;

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

        }

    }

    @Override
    protected ValueFactory getValueFactory() {

        return null;
        
    }

}
