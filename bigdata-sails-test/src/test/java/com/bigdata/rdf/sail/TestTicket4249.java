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
import java.util.Properties;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.store.TempTripleStore.Options;

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
 * 
 * @author Igor Kim
 * @see https://jira.blazegraph.com/browse/BLZG-4249
 * SUBSTR with starting location less than 1
 */
public class TestTicket4249 extends QuadsTestCase {
	
    public TestTicket4249() {
	}

	public TestTicket4249(String arg0) {
		super(arg0);
	}
	
	@Override
	public Properties getProperties() {
		Properties properties = super.getProperties();

		return properties;
	}

	public void testBug() throws Exception {

		final BigdataSail sail = getSail();
		try {
			BigdataSailRepository repo = new BigdataSailRepository(sail);
			try {
				repo.initialize();
				final RepositoryConnection conn = repo.getConnection();
				conn.setAutoCommit(false);
				ValueFactory vf = conn.getValueFactory();
				try {
					conn.add(getClass().getResourceAsStream("TestTicket2043.n3"), "",
							RDFFormat.TURTLE);
					conn.commit();
					executeQuery(conn, "123", -1, 2, "");
					executeQuery(conn, "123", 0, 2, "1");
					executeQuery(conn, "123", 1, 2, "12");
					executeQuery(conn, "123", 1, 2, "12");
					executeQuery(conn, "123", 100, 1, "");
					executeQuery(conn, "12345", 1.5, 2.6, "234");
					executeQuery(conn, "12345", 0, 3, "12");
					executeQuery(conn, "12345", 0d/0d, 3, "");
					executeQuery(conn, "12345", 1, 0d/0d, "");
					executeQuery(conn, "12345", -42, 1d/0d, "12345");
					executeQuery(conn, "12345", -1d/0d, 2d/0d, "");
					executeQuery(conn, vf.createLiteral("foobar"), 4, vf.createLiteral("bar"));
					executeQuery(conn, vf.createLiteral("foobar","en"), 4, vf.createLiteral("bar", "en"));
					executeQuery(conn, vf.createLiteral("foobar", XMLSchema.STRING), 4, vf.createLiteral("bar", XMLSchema.STRING));
					executeQuery(conn, vf.createLiteral("foobar"), 4, 1, vf.createLiteral("b"));
					executeQuery(conn, vf.createLiteral("foobar", "en"), 4, 1, vf.createLiteral("b", "en"));
					executeQuery(conn, vf.createLiteral("foobar", XMLSchema.STRING), 4, 1, vf.createLiteral("b", XMLSchema.STRING));
				} finally {
					conn.close();
				}
			} finally {
				repo.shutDown();
			}
		} finally {
			sail.__tearDownUnitTest();
		}
	}

	private void executeQuery(final RepositoryConnection conn, final String string, final double start, final double length, final String expected)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException, IOException, VisitorException {

		final ValueFactory vf = conn.getValueFactory();
		final String query = "select ?substring WHERE { BIND ( SUBSTR(?string, ?start, ?length) as ?substring ) . }";
		final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		q.setBinding("string", vf.createLiteral(string));
		q.setBinding("start", vf.createLiteral(start));
		q.setBinding("length", vf.createLiteral(length));
		final TupleQueryResult tqr = q.evaluate();
		try {
			while (tqr.hasNext()) {
				final BindingSet bindings = tqr.next();
				// assert expected value
				assertEquals(expected, bindings.getBinding("substring").getValue().stringValue());
			}
		} finally {
			tqr.close();
		}
	}

	private void executeQuery(final RepositoryConnection conn, final Literal string, final int start, final Literal expected)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		final ValueFactory vf = conn.getValueFactory();
		final String query = "select ?substring WHERE { BIND ( SUBSTR(?string, ?start) as ?substring ) . }";
		final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		q.setBinding("string", string);
		q.setBinding("start", vf.createLiteral(start));
		final TupleQueryResult tqr = q.evaluate();
		try {
			while (tqr.hasNext()) {
				final BindingSet bindings = tqr.next();
				// assert expected value
				assertEquals(expected, bindings.getBinding("substring").getValue());
			}
		} finally {
			tqr.close();
		}
	}

	private void executeQuery(final RepositoryConnection conn, final Literal string, final int start, final int length, final Literal expected)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		final ValueFactory vf = conn.getValueFactory();
		final String query = "select ?substring WHERE { BIND ( SUBSTR(?string, ?start, ?length) as ?substring ) . }";
		final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		q.setBinding("string", string);
		q.setBinding("start", vf.createLiteral(start));
		q.setBinding("length", vf.createLiteral(length));
		final TupleQueryResult tqr = q.evaluate();
		try {
			while (tqr.hasNext()) {
				final BindingSet bindings = tqr.next();
				// assert expected value
				assertEquals(expected, bindings.getBinding("substring").getValue());
			}
		} finally {
			tqr.close();
		}
	}

}
