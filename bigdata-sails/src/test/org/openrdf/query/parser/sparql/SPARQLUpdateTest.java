/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

/**
 * Tests for SPARQL 1.1 Update functionality.
 * 
 * @author Jeen Broekstra
 * 
 *         FIXME This was imported in order to expose {@link #con} to our
 *         implementation of the class, which let's us override
 *         {@link #loadDataset(String)} in order to turn off auto-commit. I've
 *         written Jeen to request the appropriate changes to the class in the
 *         openrdf SVN, at which point we should be able to drop this class from
 *         our SVN repository.
 */
public abstract class SPARQLUpdateTest extends TestCase {

	static final Logger logger = LoggerFactory.getLogger(SPARQLUpdateTest.class);

	private Repository rep;

	protected RepositoryConnection con;

	private ValueFactory f;

	private URI bob;

	private URI alice;

	private URI graph1;

	private URI graph2;

	protected static final String EX_NS = "http://example.org/";

	/**
	 * @throws java.lang.Exception
	 */
	//@Before
	public void setUp()
		throws Exception
	{
		logger.debug("setting up test");

		rep = createRepository();
		con = rep.getConnection();
		f = rep.getValueFactory();

		loadDataset("/testdata-update/dataset-update.trig");

		bob = f.createURI(EX_NS, "bob");
		alice = f.createURI(EX_NS, "alice");

		graph1 = f.createURI(EX_NS, "graph1");
		graph2 = f.createURI(EX_NS, "graph2");

		logger.debug("setup complete.");
	}

	/**
	 * @throws java.lang.Exception
	 */
	//@After
	public void tearDown()
		throws Exception
	{
		logger.debug("tearing down...");
		con.close();
		con = null;

		rep.shutDown();
		rep = null;

		super.tearDown();
		logger.debug("tearDown complete.");
	}

	/* test methods */

	//@Test
	public void testInsertWhere()
		throws Exception
	{
		logger.debug("executing test InsertWhere");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}

	//@Test
	public void testDeleteInsertWhere()
		throws Exception
	{
		logger.debug("executing test DeleteInsertWhere");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

//		System.err.println(dumpStore());

		operation.execute();

//		System.err.println(dumpStore());

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

	}

	//@Test
	public void testInsertTransformedWhere()
		throws Exception
	{
		logger.debug("executing test InsertTransformedWhere");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label [] . } WHERE {?y ex:containsPerson ?x.  }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, null, true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, null, true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, null, true));
	}

	//@Test
	public void testInsertWhereGraph()
		throws Exception
	{
		logger.debug("executing testInsertWhereGraph");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {GRAPH ?g {?x rdfs:label ?y . }} WHERE {GRAPH ?g {?x foaf:name ?y }}");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String message = "labels should have been inserted in corresponding named graphs only.";
		assertTrue(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
		assertTrue(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
	}

	//@Test
	public void testInsertWhereUsing()
		throws Exception
	{

		logger.debug("executing testInsertWhereUsing");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label ?y . } USING ex:graph1 WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String message = "label should have been inserted in default graph, for ex:bob only";
		assertTrue(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
        /*
         * Note: I added the following line to verify that <alice, rdfs:label,
         * "Alice"> is not asserted into the default graph either.
         */
        assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
	}

	//@Test
	public void testInsertWhereWith()
		throws Exception
	{
		logger.debug("executing testInsertWhereWith");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("WITH ex:graph1 INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String message = "label should have been inserted in graph1 only, for ex:bob only";
		assertTrue(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
	}

	//@Test
	public void testDeleteWhereShortcut()
		throws Exception
	{
		logger.debug("executing testDeleteWhereShortcut");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		String msg = "foaf:name properties should have been deleted";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		msg = "foaf:knows properties should not have been deleted";
		assertTrue(msg, con.hasStatement(bob, FOAF.KNOWS, null, true));
		assertTrue(msg, con.hasStatement(alice, FOAF.KNOWS, null, true));
	}

	//@Test
	public void testDeleteWhere()
		throws Exception
	{
		logger.debug("executing testDeleteWhere");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE {?x foaf:name ?y } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		String msg = "foaf:name properties should have been deleted";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

	}

	//@Test
	public void testDeleteTransformedWhere()
		throws Exception
	{
		logger.debug("executing testDeleteTransformedWhere");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		String msg = "foaf:name properties should have been deleted";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		msg = "ex:containsPerson properties should not have been deleted";
		assertTrue(msg, con.hasStatement(graph1, f.createURI(EX_NS, "containsPerson"), bob, true));
		assertTrue(msg, con.hasStatement(graph2, f.createURI(EX_NS, "containsPerson"), alice, true));

	}

	//@Test
	public void testInsertData()
		throws Exception
	{
		logger.debug("executing testInsertData");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI book1 = f.createURI(EX_NS, "book1");

		assertFalse(con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
		assertFalse(con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));

		operation.execute();

		String msg = "two new statements about ex:book1 should have been inserted";
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
		assertTrue(msg, con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
	}

	//@Test
	public void testInsertDataMultiplePatterns()
		throws Exception
	{
		logger.debug("executing testInsertData");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { ex:book1 dc:title \"book 1\". ex:book1 dc:creator \"Ringo\" . ex:book2 dc:creator \"George\". } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI book1 = f.createURI(EX_NS, "book1");
		URI book2 = f.createURI(EX_NS, "book2");

		assertFalse(con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
		assertFalse(con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
		assertFalse(con.hasStatement(book2, DC.CREATOR, f.createLiteral("George"), true));

		operation.execute();

		String msg = "newly inserted statement missing";
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true));
		assertTrue(msg, con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true));
		assertTrue(msg, con.hasStatement(book2, DC.CREATOR, f.createLiteral("George"), true));
	}

	//@Test
	public void testInsertDataInGraph()
		throws Exception
	{
		logger.debug("executing testInsertDataInGraph");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { GRAPH ex:graph1 { ex:book1 dc:title \"book 1\" ; dc:creator \"Ringo\" . } } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI book1 = f.createURI(EX_NS, "book1");

		assertFalse(con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
		assertFalse(con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));

		operation.execute();

		String msg = "two new statements about ex:book1 should have been inserted in graph1";
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1"), true, graph1));
		assertTrue(msg, con.hasStatement(book1, DC.CREATOR, f.createLiteral("Ringo"), true, graph1));
	}

	//@Test
	public void testDeleteData()
		throws Exception
	{
		logger.debug("executing testDeleteData");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE DATA { ex:alice foaf:knows ex:bob. } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, true));
		operation.execute();

		String msg = "statement should have been deleted.";
		assertFalse(msg, con.hasStatement(alice, FOAF.KNOWS, bob, true));
	}

	//@Test
	public void testDeleteDataMultiplePatterns()
		throws Exception
	{
		logger.debug("executing testDeleteData");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE DATA { ex:alice foaf:knows ex:bob. ex:alice foaf:mbox \"alice@example.org\" .} ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, true));
		assertTrue(con.hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
		operation.execute();

		String msg = "statement should have been deleted.";
		assertFalse(msg, con.hasStatement(alice, FOAF.KNOWS, bob, true));
		assertFalse(msg, con.hasStatement(alice, FOAF.MBOX, f.createLiteral("alice@example.org"), true));
	}

	//@Test
	public void testDeleteDataFromGraph()
		throws Exception
	{
		logger.debug("executing testDeleteDataFromGraph");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE DATA { GRAPH ex:graph1 {ex:alice foaf:knows ex:bob. } } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
		operation.execute();

		String msg = "statement should have been deleted from graph1";
		assertFalse(msg, con.hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
	}

	//@Test
	public void testDeleteDataFromWrongGraph()
		throws Exception
	{
		logger.debug("executing testDeleteDataFromWrongGraph");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());

		// statement does not exist in graph2.
		update.append("DELETE DATA { GRAPH ex:graph2 {ex:alice foaf:knows ex:bob. } } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
		assertFalse(con.hasStatement(alice, FOAF.KNOWS, bob, true, graph2));
		operation.execute();

		String msg = "statement should have not have been deleted from graph1";
		assertTrue(msg, con.hasStatement(alice, FOAF.KNOWS, bob, true, graph1));
	}

	//@Test
	public void testCreateNewGraph()
		throws Exception
	{
		logger.debug("executing testCreateNewGraph");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());

		URI newGraph = f.createURI(EX_NS, "new-graph");

		update.append("CREATE GRAPH <" + newGraph + "> ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(null, null, null, false, graph1));
		assertTrue(con.hasStatement(null, null, null, false, graph2));
		assertFalse(con.hasStatement(null, null, null, false, newGraph));
		assertTrue(con.hasStatement(null, null, null, false));
	}

	//@Test
	public void testCreateExistingGraph()
		throws Exception
	{
		logger.debug("executing testCreateExistingGraph");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("CREATE GRAPH <" + graph1 + "> ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		try {
			operation.execute();

			fail("creation of existing graph should have resulted in error.");
		}
		catch (UpdateExecutionException e) {
			// expected behavior
			con.rollback();
		}
	}

	//@Test
	public void testCopyToDefault()
		throws Exception
	{
		logger.debug("executing testCopyToDefault");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("COPY GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertFalse(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertFalse(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testCopyToExistingNamed()
		throws Exception
	{
		logger.debug("executing testCopyToExistingNamed");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("COPY GRAPH ex:graph1 TO ex:graph2");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph2));
		assertFalse(con.hasStatement(alice, FOAF.NAME, null, false, graph2));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testCopyToNewNamed()
		throws Exception
	{
		logger.debug("executing testCopyToNewNamed");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("COPY GRAPH ex:graph1 TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testCopyFromDefault()
		throws Exception
	{
		logger.debug("executing testCopyFromDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("COPY DEFAULT TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

	}

	//@Test
	public void testCopyFromDefaultToDefault()
		throws Exception
	{
		logger.debug("executing testCopyFromDefaultToDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("COPY DEFAULT TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
	}

	//@Test
	public void testAddToDefault()
		throws Exception
	{
		logger.debug("executing testAddToDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("ADD GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testAddToExistingNamed()
		throws Exception
	{
		logger.debug("executing testAddToExistingNamed");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("ADD GRAPH ex:graph1 TO ex:graph2");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph2));
		assertTrue(con.hasStatement(alice, FOAF.NAME, null, false, graph2));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testAddToNewNamed()
		throws Exception
	{
		logger.debug("executing testAddToNewNamed");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("ADD GRAPH ex:graph1 TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, graph1));
	}

	//@Test
	public void testAddFromDefault()
		throws Exception
	{
		logger.debug("executing testAddFromDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("ADD DEFAULT TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

	}

	//@Test
	public void testAddFromDefaultToDefault()
		throws Exception
	{
		logger.debug("executing testAddFromDefaultToDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("ADD DEFAULT TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
	}

	//@Test
	public void testMoveToDefault()
		throws Exception
	{
		logger.debug("executing testMoveToDefault");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("MOVE GRAPH <" + graph1.stringValue() + "> TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertFalse(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertFalse(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, (Resource)null));
		assertFalse(con.hasStatement(null, null, null, false, graph1));
	}

	//@Test
	public void testMoveToNewNamed()
		throws Exception
	{
		logger.debug("executing testMoveToNewNamed");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("MOVE GRAPH ex:graph1 TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertTrue(con.hasStatement(bob, FOAF.NAME, null, false, f.createURI(EX_NS, "graph3")));
		assertFalse(con.hasStatement(null, null, null, false, graph1));
	}

	//@Test
	public void testMoveFromDefault()
		throws Exception
	{
		logger.debug("executing testMoveFromDefault");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("MOVE DEFAULT TO ex:graph3");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertFalse(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertFalse(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, f.createURI(EX_NS, "graph3")));

	}

	//@Test
	public void testMoveFromDefaultToDefault()
		throws Exception
	{
		logger.debug("executing testMoveFromDefaultToDefault");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("MOVE DEFAULT TO DEFAULT");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
	}

	//@Test
	public void testClearAll()
		throws Exception
	{
		logger.debug("executing testClearAll");
		String update = "CLEAR ALL";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false));

	}

	//@Test
	public void testClearGraph()
		throws Exception
	{
		logger.debug("executing testClearGraph");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("CLEAR GRAPH <" + graph1.stringValue() + "> ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false, graph1));
		assertTrue(con.hasStatement(null, null, null, false, graph2));
		assertTrue(con.hasStatement(null, null, null, false));
	}

	//@Test
	public void testClearNamed()
		throws Exception
	{
		logger.debug("executing testClearNamed");
		String update = "CLEAR NAMED";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false, graph1));
		assertFalse(con.hasStatement(null, null, null, false, graph2));
		assertTrue(con.hasStatement(null, null, null, false));

	}

    //@Test
    public void testClearDefault()
        throws Exception
    {
        logger.debug("executing testClearDefault");

        String update = "CLEAR DEFAULT";

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(con.hasStatement(null, null, null, false, graph1));
        assertTrue(con.hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(con.hasStatement(null, null, null, false, new Resource[]{null}));

//        System.err.println(dumpStore());
        
        operation.execute();
        assertTrue(con.hasStatement(null, null, null, false, graph1));
        assertTrue(con.hasStatement(null, null, null, false, graph2));
        // Verify that no statements remain in the 'default' graph.
        assertFalse(con.hasStatement(null, null, null, false, new Resource[]{null}));
        
//      System.err.println(dumpStore());
    }

    //@Test
	public void testDropAll()
		throws Exception
	{
		logger.debug("executing testDropAll");
		String update = "DROP ALL";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false));

	}

	//@Test
	public void testDropGraph()
		throws Exception
	{
		logger.debug("executing testDropGraph");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DROP GRAPH <" + graph1.stringValue() + "> ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false, graph1));
		assertTrue(con.hasStatement(null, null, null, false, graph2));
		assertTrue(con.hasStatement(null, null, null, false));
	}

	//@Test
	public void testDropNamed()
		throws Exception
	{
		logger.debug("executing testDropNamed");

		String update = "DROP NAMED";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false, graph1));
		assertFalse(con.hasStatement(null, null, null, false, graph2));
		assertTrue(con.hasStatement(null, null, null, false));
	}

    //@Test
    public void testDropDefault()
        throws Exception
    {
        logger.debug("executing testDropDefault");

        String update = "DROP DEFAULT";

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        // Verify statements exist in the named graphs.
        assertTrue(con.hasStatement(null, null, null, false, graph1));
        assertTrue(con.hasStatement(null, null, null, false, graph2));
        // Verify statements exist in the default graph.
        assertTrue(con.hasStatement(null, null, null, false, new Resource[]{null}));

//        System.err
//                .println(((BigdataSailRepositoryConnection) con)
//                        .getSailConnection().getBigdataSail().getDatabase()
//                        .dumpStore());
        
        operation.execute();
        assertTrue(con.hasStatement(null, null, null, false, graph1));
        assertTrue(con.hasStatement(null, null, null, false, graph2));
        // Verify that no statements remain in the 'default' graph.
        assertFalse(con.hasStatement(null, null, null, false, new Resource[]{null}));
        
//        System.err
//                .println(((BigdataSailRepositoryConnection) con)
//                        .getSailConnection().getBigdataSail().getDatabase()
//                        .dumpStore());
    }

	//@Test
	public void testUpdateSequenceDeleteInsert()
		throws Exception
	{
		logger.debug("executing testUpdateSequenceDeleteInsert");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }; ");
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x} ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		String msg = "foaf:name properties should have been deleted";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		msg = "foaf:name properties with value 'foo' should have been added";
		assertTrue(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
		assertTrue(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
	}

	//@Test
	public void testUpdateSequenceInsertDelete()
		throws Exception
	{
		logger.debug("executing testUpdateSequenceInsertDelete");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x}; ");
		update.append(getNamespaceDeclarations());
		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		String msg = "foaf:name properties should have been deleted";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		msg = "foaf:name properties with value 'foo' should not have been added";
		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
	}

	//@Test
	public void testUpdateSequenceInsertDelete2()
		throws Exception
	{
		logger.debug("executing testUpdateSequenceInsertDelete2");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT { GRAPH ex:graph2 { ?s ?p ?o } } WHERE { GRAPH ex:graph1 { ?s ?p ?o . FILTER (?s = ex:bob) } }; ");
		update.append("WITH ex:graph1 DELETE { ?s ?p ?o } WHERE {?s ?p ?o . FILTER (?s = ex:bob) } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph1));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true, graph2));

		operation.execute();

		String msg = "statements about bob should have been removed from graph1";
		assertFalse(msg, con.hasStatement(bob, null, null, true, graph1));

		msg = "statements about bob should have been added to graph2";
		assertTrue(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true, graph2));
		assertTrue(msg, con.hasStatement(bob, FOAF.MBOX, null, true, graph2));
		assertTrue(msg, con.hasStatement(bob, FOAF.KNOWS, alice, true, graph2));
	}

	//@Test
	public void testUpdateSequenceInsertDeleteExample9()
		throws Exception
	{
		logger.debug("executing testUpdateSequenceInsertDeleteExample9");

		// replace the standard dataset with one specific to this case.
		con.clear();
		con.commit();
		loadDataset("/testdata-update/dataset-update-example9.trig");

		URI book1 = f.createURI("http://example/book1");
		URI book3 = f.createURI("http://example/book3");
		URI bookStore = f.createURI("http://example/bookStore");
		URI bookStore2 = f.createURI("http://example/bookStore2");
		
		StringBuilder update = new StringBuilder();
		update.append("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
		update.append("prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>  ");
		update.append("prefix xsd: <http://www.w3.org/2001/XMLSchema#>  ");
		update.append("prefix dc: <http://purl.org/dc/elements/1.1/>  ");
		update.append("prefix dcmitype: <http://purl.org/dc/dcmitype/>  ");
		update.append("INSERT  { GRAPH <http://example/bookStore2> { ?book ?p ?v } } ");
		update.append(" WHERE ");
		update.append(" { GRAPH  <http://example/bookStore> ");
		update.append("   { ?book dc:date ?date . ");
		update.append("       FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
		update.append("       ?book ?p ?v ");
		update.append("      } ");
		update.append(" } ;");
		update.append("WITH <http://example/bookStore> ");
		update.append(" DELETE { ?book ?p ?v } ");
		update.append(" WHERE ");
		update.append(" { ?book dc:date ?date ; ");
		update.append("         a dcmitype:PhysicalObject .");
		update.append("    FILTER ( ?date < \"2000-01-01T00:00:00-02:00\"^^xsd:dateTime ) ");
		update.append("   ?book ?p ?v");
		update.append(" } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String msg = "statements about book1 should have been removed from bookStore";
		assertFalse(msg, con.hasStatement(book1, null, null, true, bookStore));

		msg = "statements about book1 should have been added to bookStore2";
		assertTrue(msg, con.hasStatement(book1, RDF.TYPE, null, true, bookStore2));
		assertTrue(msg, con.hasStatement(book1, DC.DATE, null, true, bookStore2));
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, null, true, bookStore2));
	}

	/*
	//@Test
	public void testLoad()
		throws Exception
	{
		String update = "LOAD <http://www.daml.org/2001/01/gedcom/royal92.daml>";

		String ns = "http://www.daml.org/2001/01/gedcom/gedcom#";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertTrue(con.hasStatement(null, RDF.TYPE, f.createURI(ns, "Family"), true));
	}

	//@Test
	public void testLoadIntoGraph()
		throws Exception
	{
		String ns = "http://www.daml.org/2001/01/gedcom/gedcom#";

		String update = "LOAD <http://www.daml.org/2001/01/gedcom/royal92.daml> INTO GRAPH <" + ns + "> ";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement((Resource)null, RDF.TYPE, f.createURI(ns, "Family"), true, (Resource)null));
		assertTrue(con.hasStatement((Resource)null, RDF.TYPE, f.createURI(ns, "Family"), true, f.createURI(ns)));
	}
	*/

	/* protected methods */

	protected void loadDataset(String datasetFile)
		throws RDFParseException, RepositoryException, IOException
	{
		logger.debug("loading dataset...");
		InputStream dataset = SPARQLUpdateTest.class.getResourceAsStream(datasetFile);
		try {
			con.add(dataset, "", RDFFormat.TRIG);
		}
		finally {
			dataset.close();
		}
		logger.debug("dataset loaded.");
	}

	/**
	 * Get a set of useful namespace prefix declarations.
	 * 
	 * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
	 */
	protected String getNamespaceDeclarations() {
		StringBuilder declarations = new StringBuilder();
		declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
		declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
		declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
		declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
		declarations.append("PREFIX ex: <" + EX_NS + "> \n");
		declarations.append("PREFIX xsd: <" +  XMLSchema.NAMESPACE + "> \n");
		declarations.append("\n");

		return declarations.toString();
	}

	/**
	 * Creates, initializes and clears a repository.
	 * 
	 * @return an initialized empty repository.
	 * @throws Exception
	 */
	protected Repository createRepository()
		throws Exception
	{
		Repository repository = newRepository();
		repository.initialize();
		RepositoryConnection con = repository.getConnection();
		con.clear();
		con.clearNamespaces();
		con.close();
		return repository;
	}

	/**
	 * Create a new Repository object. Subclasses are expected to implement this
	 * method to supply the test case with a specific Repository type and
	 * configuration.
	 * 
	 * @return a new (uninitialized) Repository
	 * @throws Exception
	 */
	protected abstract Repository newRepository()
		throws Exception;
	
    private StringBuilder dumpStore() {
        
        return ((BigdataSailRepositoryConnection) con).getSailConnection()
                .getBigdataSail().getDatabase().dumpStore();
        
	}
}
