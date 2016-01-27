/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created Nov 2013
 */

/*
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
package com.bigdata.rdf.sparql.ast.eval;

import java.io.IOException;
import java.io.InputStream;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

public abstract class AbstractDataAndSPARQLTestCase extends AbstractASTEvaluationTestCase {

    public AbstractDataAndSPARQLTestCase() {
    }

    public AbstractDataAndSPARQLTestCase(final String name) {
        super(name);
    }

	public class AbsHelper {

		protected final String queryStr;
		
		/**
		 * This is the astContainer of the last query executed.
		 */
		protected ASTContainer astContainer;

		public AbsHelper(final String queryStr) {

		    this.queryStr = queryStr;
		    
		}

		protected AbstractTripleStore getTripleStore() {
		    
		    return store;
		    
		}

        protected void compareTupleQueryResults(
                final TupleQueryResult queryResult,
                final TupleQueryResult expectedResult, final boolean checkOrder)
                throws QueryEvaluationException {

            AbstractQueryEngineTestCase.compareTupleQueryResults(getName(), "",
                    store, astContainer, queryResult, expectedResult, false,
                    checkOrder);
            
        }

        /**
         * Load data from an input stream.
         * 
         * @param is
         *            The stream (required).
         * @param format
         *            The format (required).
         * @param uri
         *            The baseURL (required).
         * @return The #of triples read from the stream.
         */
        long loadData(final InputStream is, final RDFFormat format,
                final String uri) {

            final RDFParser rdfParser = RDFParserRegistry.getInstance()
                    .get(format).getParser();

            rdfParser.setValueFactory(store.getValueFactory());

            rdfParser.setVerifyData(true);

            rdfParser.setStopAtFirstError(true);

            rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

            final AddStatementHandler handler = new AddStatementHandler();

            if (getTripleStore().isQuads()) {

                // Set the default context.
                handler.setContext(new URIImpl(uri));

            }
            
            rdfParser.setRDFHandler(handler);
                        
            /*
             * Run the parser, which will cause statements to be inserted.
             */


            try {

                rdfParser.parse(is, baseURI);

                return handler.close();

            } catch (Exception e) {

                throw new RuntimeException(e);

            } finally {

                try {

                    is.close();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }
		}

        /**
         * Helper class adds statements to the sail as they are visited by a
         * parser.
         */
        private class AddStatementHandler extends RDFHandlerBase {

            private final StatementBuffer<Statement> buffer;

            private Resource context = null;
            
            private long n = 0L;

            public AddStatementHandler() {

                buffer = new StatementBuffer<Statement>(store, 1000/* capacity */);

            }

            public void setContext(final Resource context) {

                this.context = context;
                
            }
            
            @Override
            public void handleStatement(final Statement stmt)
                    throws RDFHandlerException {

                final Resource s = stmt.getSubject();
                final URI p = stmt.getPredicate();
                final Value o = stmt.getObject();
                final Resource c = stmt.getContext() == null ? this.context
                        : stmt.getContext();

//                if (log.isDebugEnabled())
//                    log.debug("<" + s + "," + p + "," + o + "," + c + ">");

                buffer.add(s, p, o, c, StatementEnum.Explicit);

                n++;

            }

            /**
             * 
             * @return The #of statements visited by the parser.
             */
            public long close() {

                buffer.flush();

                return n;

            }

        }

	}

}
