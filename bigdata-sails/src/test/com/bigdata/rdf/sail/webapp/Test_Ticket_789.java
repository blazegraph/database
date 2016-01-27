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

package com.bigdata.rdf.sail.webapp;

import java.util.Arrays;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.DatasetImpl;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Test suite for remote support of Sesame API Query methods (setBinding, setDataset, setIncludeInferred) and support of contexts in getStatements
 * 
 *  @author igorkim
 *  
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-789" > BigdataSailRemoteRepositoryConnection should implement interface methods </a>
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-753"> Result of BigdataSailRemoteRepositoryConnection.getStatements() and export() do not preserve statement context.
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-1325"> BigdataSailRemoteRepositoryConnection must support setBinding(), getBindings(), clearBindings(), and removeBinding()
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-1326"> BigdataSailRemoteRepositoryConnection must support setDataset(), getDataset()
 */
public class Test_Ticket_789<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	private final Resource s = new URIImpl("http://test/s");
	private final URI p = new URIImpl("http://test/p");
	private final URI p1 = new URIImpl("http://test/p1");
	private final Value o = new URIImpl("http://test/o");
	private final Resource c = new URIImpl("http://test/c");
	private final URI defaultGraph1 = new URIImpl("http://test/defaultGraph1");
	private final URI defaultGraph2 = new URIImpl("http://test/defaultGraph2");
	private final URI namedGraph1 = new URIImpl("http://test/namedGraph1");
	private final URI namedGraph2 = new URIImpl("http://test/namedGraph2");

	public Test_Ticket_789() {

		super();

	}

	public Test_Ticket_789(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_789.class,
				"test.*", TestMode.quads);

	}

	private void populate() throws Exception {
		final ValueFactoryImpl vf = ValueFactoryImpl.getInstance();
		final Statement[] a = new Statement[] {
				vf.createStatement(s, p, o, c),
				vf.createStatement(s, p, o, namedGraph1),
				vf.createStatement(s, p1, o, namedGraph2),
				vf.createStatement(s, p, o, defaultGraph1),
				vf.createStatement(s, p, p, defaultGraph2),
				vf.createStatement(p, p, p, c)
		};

		final AddOp addOp = new AddOp(Arrays.asList(a));

		m_repo.add(addOp);
	}

	/**
	 * Test supposed to check if remote call for getStatements properly handle
	 * contexts
	 */
	public void test_ticket_753_getStatements() throws Exception {

		populate();

		final GraphQueryResult resultIncludeInferred = m_repo.getStatements(s, p, o, true, c);
		try {
			int count = 0;
			while (resultIncludeInferred.hasNext()) {
				resultIncludeInferred.next();
				count++;
			}
			// check that only one of the (s,p,o) triple returned for the specified context c,
			// as the other one belongs to different context namedGraph1
			assertEquals(1, count);
		} finally {
			resultIncludeInferred.close();
		}

	}

	public void test_ticket_1325_bindings() throws Exception {
		populate();
		
		BigdataSailRemoteRepository sailRepo = m_repo.getBigdataSailRemoteRepository();
		BigdataSailRemoteRepositoryConnection con = sailRepo.getConnection();
		
		final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {?s ?p ?o}");
		tq.setBinding("s", s);
		tq.setBinding("o", o);
		{
			TupleQueryResult tqr = tq.evaluate();
			try {
				int count = 0;
				while (tqr.hasNext()) {
					BindingSet bs = tqr.next();
					assertEquals(s, bs.getBinding("s").getValue());
					assertEquals(o, bs.getBinding("o").getValue());
					count++;
				}
				// there are 2 statements with specified subject and object: (s, p, o) and (s, p1, o) 
				assertEquals(2, count);
			} finally {
				tqr.close();
			}
			tq.clearBindings();
			assertEquals(0, tq.getBindings().size());
		}
	}

	public void test_ticket_1326_dataset() throws Exception {
		populate();
		
		BigdataSailRemoteRepository sailRepo = m_repo.getBigdataSailRemoteRepository();
		BigdataSailRemoteRepositoryConnection con = sailRepo.getConnection();
		final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {?s ?p ?o}");

		final DatasetImpl dataset = new DatasetImpl();
		dataset.addDefaultGraph(defaultGraph1);

		tq.setDataset(dataset);
		{
			TupleQueryResult tqr = tq.evaluate();
			try {
				int count = 0;
				while (tqr.hasNext()) {
					tqr.next();
					count++;
				}
				// there is only 1 statement in defaultGraph1 
				assertEquals(1, count);
			} finally {
				tqr.close();
			}
		}
	}

}
