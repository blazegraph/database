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
import java.util.Set;

import junit.framework.Assert;

import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store. You can specify
 * this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on. If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 * 
 * @see https://jira.blazegraph.com/browse/BLZG-1788
 * 		Typed literals in VALUES clause not matching data
 */
public class TestTicket1788 extends QuadsTestCase {
	
    public TestTicket1788() {
	}

	public TestTicket1788(String arg0) {
		super(arg0);
	}

	public void testBug() throws Exception {

		final BigdataSail sail = getSail();
		try {
			executeQuery(new BigdataSailRepository(sail));
		} finally {
			sail.__tearDownUnitTest();
		}
	}

	private void executeQuery(final BigdataSailRepository repo)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException, IOException {
		try {
			repo.initialize();
			final BigdataSailRepositoryConnection conn = repo.getConnection();
			conn.setAutoCommit(false);
			try {
				conn.add(getClass().getResourceAsStream("TestTicket1788.n3"), "",
						RDFFormat.TURTLE);
				conn.commit();
				
				final String query = "SELECT * {\r\n" + 
						"  ?s <http://p> ?o .\r\n" + 
						"  values ?o { \"A\"^^xsd:string \"B\"^^xsd:string \"D\"^^xsd:string }\r\n" + 
						"}";
				final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
						query);
				final TupleQueryResult tqr = q.evaluate();
				int cnt = 0;
				while (tqr.hasNext()) {
				    BindingSet bindings = tqr.next();
				    cnt++;
				    if(log.isInfoEnabled())
				        log.info("bindings="+bindings);
				}
				tqr.close();
				assertEquals(2, cnt);

			} finally {
				conn.close();
			}
		} finally {
			repo.shutDown();
		}
	}

}
