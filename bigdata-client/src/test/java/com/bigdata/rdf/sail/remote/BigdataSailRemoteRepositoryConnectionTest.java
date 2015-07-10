package com.bigdata.rdf.sail.remote;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.http.protocol.Protocol;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.RepositoryException;

import com.bigdata.rdf.sail.webapp.QueryServlet;
import com.bigdata.rdf.sail.webapp.client.MockRemoteRepository;

public class BigdataSailRemoteRepositoryConnectionTest {
	
	private BigdataSailRemoteRepositoryConnection con;
	private MockRemoteRepository remote;
	private BigdataSailRemoteRepository repo;

	@Before
	public void prepare() {
		remote = MockRemoteRepository.create("{\"head\":{\"vars\":[\"s\"]}}");
		repo = remote.getBigdataSailRemoteRepository();
		con = new BigdataSailRemoteRepositoryConnection(repo);
	}
	
	@After
	public void tearDown() throws RepositoryException {
		con.close();
		con = null;
		repo.shutDown();
		remote = null;
	}

	@Test
	public void testTupleQueryBindings() throws Exception {
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
		Value s = new URIImpl(":s");
		Value p = new URIImpl(":p");
		tq.setBinding("s", s);
		tq.setBinding("p", p);
		tq.evaluate();
		assertEquals("<:s>",remote.data.opts.getRequestParam("$s"));
		assertEquals("<:p>",remote.data.opts.getRequestParam("$p"));
		assertEquals("<:s>",remote.data.request.getParams().get("$s").getValue());
		assertEquals("<:p>",remote.data.request.getParams().get("$p").getValue());
		System.out.println("Done bindings");
	}

	@Test
	public void testTupleQueryIncludeInferred() throws Exception {
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
		tq.setIncludeInferred(false);
		tq.evaluate();
		assertEquals("false", remote.data.opts.getRequestParam(QueryServlet.INCLUDE_INFERRED));
		assertEquals("false", remote.data.request.getParams().get(QueryServlet.INCLUDE_INFERRED).getValue());
		
		tq.setIncludeInferred(true);
		tq.evaluate();
		assertEquals("true", remote.data.opts.getRequestParam(QueryServlet.INCLUDE_INFERRED));
		assertEquals("true", remote.data.request.getParams().get(QueryServlet.INCLUDE_INFERRED).getValue());
		System.out.println("Done inferred");
	}
	
	@Test
	public void testTupleQueryDataset() throws Exception {
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}");
		DatasetImpl dataset = new DatasetImpl();
		URI defaultGraph1 = new URIImpl(":defaultGraph1");
		URI defaultGraph2 = new URIImpl(":defaultGraph2");
		URI namedGraph1 = new URIImpl(":namedGraph1");
		URI namedGraph2 = new URIImpl(":namedGraph2");
		dataset.addDefaultGraph(defaultGraph1);
		dataset.addDefaultGraph(defaultGraph2);
		dataset.addNamedGraph(namedGraph1);
		dataset.addNamedGraph(namedGraph2);
		tq.setDataset(dataset);
		tq.evaluate();
		assertEquals(defaultGraph1.stringValue(),remote.data.opts.getRequestParam(Protocol.DEFAULT_GRAPH_PARAM_NAME));
		assertEquals(defaultGraph1.stringValue(),remote.data.request.getParams().get(Protocol.DEFAULT_GRAPH_PARAM_NAME).getValue());
		assertEquals(namedGraph1.stringValue(),remote.data.opts.getRequestParam(Protocol.NAMED_GRAPH_PARAM_NAME));
		assertEquals(namedGraph1.stringValue(),remote.data.request.getParams().get(Protocol.NAMED_GRAPH_PARAM_NAME).getValue());
	
		System.out.println("Done dataset");
	}

	@Test
	public void testTupleQueryBaseURI() throws Exception {
		String baseURI = ":baseURI";
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * from {?s ?p ?o}", baseURI);
		tq.evaluate();
		assertEquals(baseURI, remote.data.opts.getRequestParam(Protocol.BASEURI_PARAM_NAME));
		assertEquals(baseURI,remote.data.opts.getRequestParam(Protocol.BASEURI_PARAM_NAME));
	
		System.out.println("Done baseURI");
	}

}
