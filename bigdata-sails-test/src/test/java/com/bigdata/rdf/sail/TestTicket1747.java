/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.List;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.sparql.ast.*;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution;

/**
 * Test suite for an issue where IV resolution of vocabulary terms
 * interferes with query parsing and deferred IV resolution ({@link ASTDeferredIVResolution}.
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1747">
 * Persistent "Unknown extension" issue in WIkidata Query Service</a>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestTicket1747 extends QuadsTestCase {
	
    public TestTicket1747() {
	}

	public TestTicket1747(String arg0) {
		super(arg0);
	}

	public void testBug() throws Exception {

//		Setup a triple store using a vocabulary that declares xsd:dateTime (this should be in the default vocabulary).
		
		final BigdataSail sail = getSail();
		try {
			executeQuery(new BigdataSailRepository(sail));
		} finally {
			sail.__tearDownUnitTest();
		}
	}

	private void executeQuery(final BigdataSailRepository repo)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException, IOException, VisitorException {
		try {
			repo.initialize();
			final BigdataSailRepositoryConnection conn = repo.getConnection();
			try {
				BigdataValueFactory vf = conn.getValueFactory();
			
				//				Verify that you can resolve xsd:dateTime to a non-mock IV.
				IV xsdDateTimeIV = conn.getTripleStore().getLexiconRelation().resolve(vf.createURI(XMLSchema.DATETIME.stringValue())).getIV();
				assertFalse(xsdDateTimeIV.isNullIV());
				assertTrue(xsdDateTimeIV.isVocabulary());

//				Parse the first query on this ticket.
				String query = "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\r\n" + 
						"PREFIX wikibase: <http://wikiba.se/ontology#>\r\n" + 
						"PREFIX p: <http://www.wikidata.org/prop/>\r\n" + 
						"PREFIX v: <http://www.wikidata.org/prop/statement/>\r\n" + 
						"PREFIX q: <http://www.wikidata.org/prop/qualifier/>\r\n" + 
						"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
						"SELECT ?entity (year(?date) as ?year)\r\n" + 
						"WHERE\r\n" + 
						"{\r\n" + 
						"?entityS wdt:P569 ?date .\r\n" + 
						"SERVICE wikibase:label {\r\n" + 
						"bd:serviceParam wikibase:language \"en\" .\r\n" + 
						"?entityS rdfs:label ?entity\r\n" + 
						"}\r\n" + 
						"FILTER (datatype(?date) = xsd:dateTime)\r\n" + 
						"FILTER (month(?date) = month(now()))\r\n" + 
						"FILTER (day(?date) = day(now()))\r\n" + 
						"}\r\n" + 
						"LIMIT 10"; 
		        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);

//				Verify that re-resolution of xsd:dateTime does not return a MockIV.
		        xsdDateTimeIV = conn.getTripleStore().getLexiconRelation().resolve(XMLSchema.DATETIME).getIV();
		        assertFalse(xsdDateTimeIV.isNullIV());
		        assertTrue(xsdDateTimeIV.isVocabulary());

//				Invoke deferred IV resolution on the parsed query.
		        ASTDeferredIVResolution.resolveQuery(conn.getTripleStore(), astContainer);
		        // astContainer instance is updated with resolved IVs at this point
	        
//				Verify that re-resolution of xsd:dateTime does not return a MockIV.
		        xsdDateTimeIV = conn.getTripleStore().getLexiconRelation().resolve(XMLSchema.DATETIME).getIV();
		        assertFalse(xsdDateTimeIV.isNullIV());
		        assertTrue(xsdDateTimeIV.isVocabulary());
//				Verify that the parsed query does not contain a MockIV for xsd:dateTime
		        QueryRoot queryRoot = astContainer.getOriginalAST();
		        GraphPatternGroup whereClause = queryRoot.getWhereClause();
				List<FilterNode> filterNodes = whereClause.getChildren(FilterNode.class);
				for (FilterNode filterNode: filterNodes) {
					checkNode(filterNode);
		        }
			} finally {
				conn.close();
			}
		} finally {
			repo.shutDown();
		}
	}

	private void checkNode(BOp bop) {
		if (bop instanceof ConstantNode) {
			BigdataValue value = ((ConstantNode)bop).getValue();
			if (XMLSchema.DATETIME.equals(value)) {
				IV xsdDateTimeIV = value.getIV();
		        assertFalse(xsdDateTimeIV.isNullIV());
				assertTrue(xsdDateTimeIV.isVocabulary());
			}
		} else {
			for (BOp arg: bop.args()) {
				checkNode(arg);
			}
		}
	}
}
