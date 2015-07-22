/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.sail.remote;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.sail.webapp.client.EncodeDecodeValue;
import com.bigdata.rdf.sail.webapp.client.MockRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryDecls;

/**
 * Test of the Java client for the REST API using mock objects to verify that
 * the generated http request is correct.
 * 
 * @author igorkim
 */
public class BigdataSailRemoteRepositoryConnectionTest extends TestCase {
	
	private BigdataSailRemoteRepositoryConnection con;
	private MockRemoteRepository remote;
	private BigdataSailRemoteRepository repo;

	@Before
    protected void setUp() {
		remote = MockRemoteRepository.create("s\n", "");
		repo = remote.getBigdataSailRemoteRepository();
		con = new BigdataSailRemoteRepositoryConnection(repo);
	}
	
	@After
	protected void tearDown() throws RepositoryException {
        if (con != null) {
            con.close();
            con = null;
        }
        if (repo != null) {
            repo.shutDown();
            remote = null;
        }
	}

	@Test
	public void testTupleQueryBindings() throws Exception {
		final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
		final Value s = new URIImpl(":s");
		final Value p = new URIImpl(":p");
		tq.setBinding("s", s);
		tq.setBinding("p", p);
		TupleQueryResult tqr = tq.evaluate();
		try {
			assertEquals(EncodeDecodeValue.encodeValue(s),remote.data.opts.getRequestParam("$s"));
			assertEquals(EncodeDecodeValue.encodeValue(p),remote.data.opts.getRequestParam("$p"));
			assertEquals(EncodeDecodeValue.encodeValue(s),remote.data.request.getParams().get("$s").getValue());
			assertEquals(EncodeDecodeValue.encodeValue(p),remote.data.request.getParams().get("$p").getValue());
		} finally {
			tqr.close();
		}
	}

	@Test
	public void testTupleQueryIncludeInferred() throws Exception {
		final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
		tq.setIncludeInferred(false);
		tq.evaluate();
		assertEquals("false", remote.data.opts.getRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED));
		assertEquals("false", remote.data.request.getParams().get(RemoteRepositoryDecls.INCLUDE_INFERRED).getValue());
		
		tq.setIncludeInferred(true);
		final TupleQueryResult tqr = tq.evaluate();
		try {
			assertEquals("true", remote.data.opts.getRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED));
			assertEquals("true", remote.data.request.getParams().get(RemoteRepositoryDecls.INCLUDE_INFERRED).getValue());
		} finally {
			tqr.close();
		}
	}
	
	@Test
	public void testTupleQueryDataset() throws Exception {
	    final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
	    final DatasetImpl dataset = new DatasetImpl();
	    final URI defaultGraph1 = new URIImpl(":defaultGraph1");
	    final URI defaultGraph2 = new URIImpl(":defaultGraph2");
	    final URI namedGraph1 = new URIImpl(":namedGraph1");
	    final URI namedGraph2 = new URIImpl(":namedGraph2");
		dataset.addDefaultGraph(defaultGraph1);
		dataset.addDefaultGraph(defaultGraph2);
		dataset.addNamedGraph(namedGraph1);
		dataset.addNamedGraph(namedGraph2);
		tq.setDataset(dataset);
		final TupleQueryResult tqr = tq.evaluate();
		try {
			assertEquals(defaultGraph1.stringValue(),remote.data.opts.getRequestParam(RemoteRepositoryDecls.DEFAULT_GRAPH_URI));
			assertEquals(defaultGraph1.stringValue(),remote.data.request.getParams().get(RemoteRepositoryDecls.DEFAULT_GRAPH_URI).getValue());
			assertEquals(namedGraph1.stringValue(),remote.data.opts.getRequestParam(RemoteRepositoryDecls.NAMED_GRAPH_URI));
			assertEquals(namedGraph1.stringValue(),remote.data.request.getParams().get(RemoteRepositoryDecls.NAMED_GRAPH_URI).getValue());
		} finally {
			tqr.close();
		}
	}

	@Test
	public void testTupleQueryBaseURI() throws Exception {
	    final String baseURI = ":baseURI";
	    final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}", baseURI);
	    final TupleQueryResult tqr = tq.evaluate();
		try {
			assertEquals(baseURI, remote.data.opts.getRequestParam(RemoteRepositoryDecls.BASE_URI));
			assertEquals(baseURI,remote.data.opts.getRequestParam(RemoteRepositoryDecls.BASE_URI));
		} finally {
			tqr.close();
		}
	}

	@Test
	public void testGetStatements() throws RepositoryException {
	    final Resource s = new URIImpl(":s");
	    final URI p = new URIImpl(":p");
	    final Value o = new URIImpl(":o");
	    final boolean includeInferred = false;
	    final Resource c = new URIImpl(":c");
	    final RepositoryResult<Statement> stmts = con.getStatements(s, p, o, includeInferred, c);
		try {
			assertEquals(EncodeDecodeValue.encodeValue(s), remote.data.opts.getRequestParam("s"));
			assertEquals(EncodeDecodeValue.encodeValue(p), remote.data.opts.getRequestParam("p"));
			assertEquals(EncodeDecodeValue.encodeValue(o), remote.data.opts.getRequestParam("o"));
			assertEquals(EncodeDecodeValue.encodeValue(c), remote.data.opts.getRequestParam("c"));
			assertEquals(Boolean.toString(includeInferred), remote.data.opts.getRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED));
		} finally {
			stmts.close();
		}
	}
	
}
