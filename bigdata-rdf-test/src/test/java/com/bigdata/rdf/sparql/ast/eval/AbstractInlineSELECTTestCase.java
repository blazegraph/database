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
/*
 * Created Nov 2013
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.Iterations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.dawg.DAWGTestResultSetUtil;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * The idea here is that the subclasses provide the data for the test
 * (i.e. the triples, the query and the results) inline and not in separate
 * files. One goal is to reduce the level of redundant comments
 * that can be out of sync.
 * 
 * See {@link AbstractDataDrivenSPARQLTestCase} for the out-of-line files based approach,
 * which may be more suitable for larger queries, test data or results.
 * @author jeremycarroll
 */
public abstract class AbstractInlineSELECTTestCase extends AbstractDataAndSPARQLTestCase
        {

    private static final Logger log = Logger
            .getLogger(AbstractInlineSELECTTestCase.class);
    
    private final Map<String,String> prefixes = new HashMap<String,String>();
    /**
     * 
     */
    public AbstractInlineSELECTTestCase() {
    }

    /**
     * @param name
     */
    public AbstractInlineSELECTTestCase(String name) {
        super(name);
        addPrefix("","http://example.org/banana#");
        addPrefix("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        addPrefix("rdfs","http://www.w3.org/2000/01/rdf-schema#");
        addPrefix("xsd","http://www.w3.org/2001/XMLSchema#");
    }

    public void addPrefix(String prefix, String namespace) {
		prefixes.put(prefix, namespace);
	}

	public class Execute extends AbsHelper {

        
        private static final String FILL_IN_URI = "http://please-do-not-user-relative-uris/";

        
        private TupleQueryResult executeSelect(String sparql) throws QueryEvaluationException, MalformedQueryException {
        	String prefixes = sparqlPrefixes();
            astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(prefixes+sparql, FILL_IN_URI);
        	return ASTEvalHelper.evaluateTupleQuery(store, astContainer, new QueryBindingSet(), null /* dataset */);
        }
        
        /**
         * 
         * See http://stackoverflow.com/a/7716231/2276263
         * @param testURI
         * @throws Exception
         * 
         */
        public Execute(final String queryStr) throws Exception {
        	super(queryStr);

           

            if (log.isInfoEnabled())
                log.info("\nquery:\n" + queryStr);
        }

		/**
         * Load some RDF data.
         * 
         * @param data
         *            The in-line data to be loaded.
         * 
         * @return The #of statements parsed from the source. If there are
         *         duplicate told statements, then there may be fewer statements
         *         written onto the KB.
         * @throws UnsupportedEncodingException 
         */
        private long loadData(String data) throws UnsupportedEncodingException {
        	
        	if (data == null || data.length() == 0) {
        		return 0;
        	}
        	
        	data = ttlPrefixes() + data;

            byte utf8Data[] = data.getBytes("utf-8");
            final InputStream is = new ByteArrayInputStream(utf8Data);
            String uri = FILL_IN_URI;

            RDFFormat format = RDFFormat.TRIG;
			return loadData(is, format, uri);

        }

		public void expectResultSet(String vars, String ... bindings) throws Exception {
            final TupleQueryResult expectedResult = expectedTupleQueryResult(vars, bindings);

            loadData(trigData());
            
            final TupleQueryResult queryResult = executeSelect(queryStr);
            compareTupleQueryResults(queryResult, expectedResult, false);
		}

		private TupleQueryResult expectedTupleQueryResult(String vars, String ...bindings ) throws QueryEvaluationException, MalformedQueryException {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT ");
			sb.append(vars);
			sb.append("\n{} BINDINGS ");
			sb.append(vars);
			sb.append("{\n");
			for (String binding:bindings) {
			    sb.append(" ( ");
			    sb.append(binding);
			    sb.append(" )\n");
			}
			sb.append("}\n");
			return executeSelect( sb.toString());
		}

		protected String trigData() {
			return AbstractInlineSELECTTestCase.this.trigData();
		}

    }

	protected abstract String trigData();

	public String sparqlPrefixes() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry: prefixes.entrySet()) {
			sb.append("PREFIX ");
			sb.append(entry.getKey());
			sb.append(": <");
			sb.append(entry.getValue());
			sb.append(">\n");
		}
		sb.append("\n");
		return sb.toString();
	}

	public String ttlPrefixes() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry: prefixes.entrySet()) {
			sb.append("@prefix ");
			sb.append(entry.getKey());
			sb.append(": <");
			sb.append(entry.getValue());
			sb.append(">.\n");
		}
		sb.append("\n");
		return sb.toString();
	}

}
