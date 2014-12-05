/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.TestSparqlUpdate;

/**
 * Tests for SPARQL 1.1 Update functionality.
 * <p>
 * Note: Also see {@link TestSparqlUpdate}. These two test suites SHOULD be kept
 * synchronized. {@link TestSparqlUpdate} runs against the NSS while this test
 * suite runs against a local kb instance. The two test suites are not exactly
 * the same because one uses the {@link RemoteRepository} to commuicate with the
 * NSS while the other uses the local API.
 * <p>
 * Note: This was imported in order to expose {@link #con} to our implementation
 * of the class, which let's us override {@link #loadDataset(String)} in order
 * to turn off auto-commit. I've written Jeen to request the appropriate changes
 * to the class in the openrdf SVN. Since then, we have added several of our own
 * tests here.
 * 
 * @author Jeen Broekstra
 * @author Bryan Thompson
 * 
 * @see TestSparqlUpdate
 * @openrdf
 */
public abstract class SPARQLUpdateTest extends TestCase {

	static final Logger logger = LoggerFactory.getLogger(SPARQLUpdateTest.class);

    /**
     * When <code>true</code>, the unit tests of the BINDINGS clause are
     * enabled.
     * 
     * FIXME BINDINGS does not work correctly for UPDATE (actually, BINDINGS are
     * not allowed for UPDATE in the SPARQL 1.1 specification draft that we
     * currently implement and have been replaced by VALUES in the more recent
     * draft, so these unit tests need to go when we pick up the change set from
     * openrdf).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/593" >
     *      openrdf 2.6.9 </a>
     */
    private static boolean BINDINGS = false;
    
    private Repository rep;

	protected RepositoryConnection con;

	protected ValueFactory f;

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
		final StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}

	/**
	 * @since openrdf 2.6.3
	 */
//	    @Test
    public void testInsertWhereWithBinding()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testInsertWhereWithBinding");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
        operation.setBinding("x", bob);

        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
    }

    /**
     * @since openrdf 2.6.6
     */
    public void testInsertWhereWithBindings2()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testInsertWhereWithBindings2");
        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?z . } WHERE {?x foaf:name ?y }");

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
        operation.setBinding("z", f.createLiteral("Bobbie"));
        operation.setBinding("x", bob);

        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bobbie"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, null, true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bobbie"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertEmptyWhere()
        throws Exception
    {
        logger.debug("executing test testInsertEmptyWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { <" + bob + "> rdfs:label \"Bob\" . } WHERE { }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertEmptyWhereWithBinding()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testInsertEmptyWhereWithBinding");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x rdfs:label ?y . } WHERE { }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
        operation.setBinding("x", bob);
        operation.setBinding("y", f.createLiteral("Bob"));

        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertNonMatchingWhere()
        throws Exception
    {
        logger.debug("executing test testInsertNonMatchingWhere");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));

        operation.execute();

        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertNonMatchingWhereWithBindings()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testInsertNonMatchingWhereWithBindings");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
        operation.setBinding("x", bob);
        operation.setBinding("y", f.createLiteral("Bob"));

        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));

        operation.execute();

        assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertWhereWithBindings()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testInsertWhereWithBindings");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT { ?x rdfs:comment ?z . } WHERE { ?x foaf:name ?y }");

        final Literal comment = f.createLiteral("Bob has a comment");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
        operation.setBinding("x", bob);
        operation.setBinding("z", comment);

        assertFalse(con.hasStatement(null, RDFS.COMMENT, comment, true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.COMMENT, comment, true));
        assertFalse(con.hasStatement(alice, RDFS.COMMENT, comment, true));

    }

    /**
     * @since openrdf 2.6.3
     */
//	    @Test
    public void testInsertWhereWithOptional()
        throws Exception
    {
        logger.debug("executing testInsertWhereWithOptional");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append(" INSERT { ?s ex:age ?incAge } ");
        // update.append(" DELETE { ?s ex:age ?age } ");
        update.append(" WHERE { ?s foaf:name ?name . ");
        update.append(" OPTIONAL {?s ex:age ?age . BIND ((?age + 1) as ?incAge)  } ");
        update.append(" } ");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        final URI age = f.createURI(EX_NS, "age");

        assertFalse(con.hasStatement(alice, age, null, true));
        assertTrue(con.hasStatement(bob, age, null, true));

        operation.execute();

        RepositoryResult<Statement> result = con.getStatements(bob, age, null, true);

        while (result.hasNext()) {
            final Statement stmt = result.next();
            if (logger.isInfoEnabled())
                logger.info(stmt.toString());
        }

        assertTrue(con.hasStatement(bob, age, f.createLiteral("43", XMLSchema.INTEGER), true));

        result = con.getStatements(alice, age, null, true);

        while (result.hasNext()) {
            final Statement stmt = result.next();
            if (logger.isInfoEnabled())
                logger.info(stmt.toString());
        }
        assertFalse(con.hasStatement(alice, age, null, true));
    }

    /**
     * @since 2.6.10
     */
    public void testInsertWhereWithBlankNode()
            throws Exception
    {
        logger.debug("executing testInsertWhereWithBlankNode");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append(" INSERT { ?s ex:complexAge [ rdf:value ?age; rdfs:label \"old\" ] . } ");
        update.append(" WHERE { ?s ex:age ?age . ");
        update.append(" } ");

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        URI age = f.createURI(EX_NS, "age");
        URI complexAge = f.createURI(EX_NS, "complexAge");

        assertTrue(con.hasStatement(bob, age, null, true));

        operation.execute();

        RepositoryResult<Statement> sts = con.getStatements(bob, complexAge, null, true);

        assertTrue(sts.hasNext());

        Value v1 = sts.next().getObject();

        sts.close();

        sts = con.getStatements(null, RDF.VALUE, null, true);

        assertTrue(sts.hasNext());

        Value v2 = sts.next().getSubject();

        assertEquals(v1, v2);

        sts.close();

        String query = getNamespaceDeclarations()
                + " SELECT ?bn ?age ?l WHERE { ex:bob ex:complexAge ?bn. ?bn rdf:value ?age. ?bn rdfs:label ?l .} ";

        TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();

        assertTrue(result.hasNext());

        BindingSet bs = result.next();

        assertFalse(result.hasNext());

    }
    
	//@Test
	public void testDeleteInsertWhere()
		throws Exception
	{
		logger.debug("executing test DeleteInsertWhere");
		final StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

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

//    @Test
    public void testDeleteWhereOptional()
        throws Exception
    {
        logger.debug("executing test testDeleteWhereOptional");
        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append(" DELETE { ?x foaf:name ?y; foaf:mbox ?mbox. } ");
        update.append(" WHERE {?x foaf:name ?y. ");
        update.append(" OPTIONAL { ?x foaf:mbox ?mbox. FILTER (str(?mbox) = \"bob@example.org\") } }");

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        Literal mboxBob = f.createLiteral("bob@example.org");
        Literal mboxAlice = f.createLiteral("alice@example.org");
        assertTrue(con.hasStatement(bob, FOAF.MBOX, mboxBob , true));
        assertTrue(con.hasStatement(alice, FOAF.MBOX, mboxAlice, true));


        assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
        
        operation.execute();

        assertFalse(con.hasStatement(bob, FOAF.MBOX, mboxBob , true));
        assertTrue(con.hasStatement(alice, FOAF.MBOX, mboxAlice, true));
        
        assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

    }

    /** @since OPENRDF 2.6.6. */
//    @Test
    public void testDeleteInsertWhereWithBindings()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testDeleteInsertWhereWithBindings");
        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        operation.setBinding("x", bob);
        
        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
    }
    
    /** @since OPENRDF 2.6.6. */
//    @Test
    public void testDeleteInsertWhereWithBindings2()
        throws Exception
    {
        if (!BINDINGS)
            return;
        logger.debug("executing test testDeleteInsertWhereWithBindings2");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?z . } WHERE {?x foaf:name ?y }");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        operation.setBinding("z", f.createLiteral("person"));
        
        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        operation.execute();

        assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("person"), true));
        assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("person"), true));

        assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
    }
    
	/** @since openrdf 2.6.3 */
    public void testDeleteInsertWhereLoopingBehavior() throws Exception {
        logger.debug("executing test testDeleteInsertWhereLoopingBehavior");
        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append(" DELETE { ?x ex:age ?y } INSERT {?x ex:age ?z }");
        update.append(" WHERE { ");
        update.append("   ?x ex:age ?y .");
        update.append("   BIND((?y + 1) as ?z) ");
        update.append("   FILTER( ?y < 46 ) ");
        update.append(" } ");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL,
                update.toString());

        final URI age = f.createURI(EX_NS, "age");
        final Literal originalAgeValue = f.createLiteral("42", XMLSchema.INTEGER);
        final Literal correctAgeValue = f.createLiteral("43", XMLSchema.INTEGER);
        final Literal inCorrectAgeValue = f.createLiteral("46", XMLSchema.INTEGER);

        assertTrue(con.hasStatement(bob, age, originalAgeValue, true));

        operation.execute();

        assertFalse(con.hasStatement(bob, age, originalAgeValue, true));
        assertTrue(con.hasStatement(bob, age, correctAgeValue, true));
        assertFalse(con.hasStatement(bob, age, inCorrectAgeValue, true));
    }

    /** @since OPENRDF 2.6.10. */
    public void testConsecutiveUpdatesInSameTransaction()
            throws Exception
    {
        // this tests if consecutive updates in the same transaction behave
        // correctly. See issue SES-930
        logger.debug("executing test testConsecutiveUpdatesInSameTransaction");

        StringBuilder update1 = new StringBuilder();
        update1.append(getNamespaceDeclarations());
        update1.append("DELETE { ?x foaf:name ?y } WHERE {?x foaf:name ?y }");

        boolean autoCommit = con.isAutoCommit();

        con.setAutoCommit(false);
        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update1.toString());

        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        operation.execute();

        // update should be visible to own connection.
        assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

        StringBuilder update2 = new StringBuilder();
        update2.append(getNamespaceDeclarations());
        update2.append("INSERT { ?x rdfs:label ?y } WHERE {?x foaf:name ?y }");

        operation = con.prepareUpdate(QueryLanguage.SPARQL, update2.toString());

        operation.execute();

        // update should not have resulted in any inserts: where clause is empty.
        assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
        assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

        con.setAutoCommit(autoCommit);

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
//        /*
//         * Note: I added the following line to verify that <alice, rdfs:label,
//         * "Alice"> is not asserted into the default graph either. (This change
//		   * was picked up by openrdf).
//         */
        assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
//		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
//		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
	}

	//@Test
	public void testInsertWhereWith()
		throws Exception
	{
		logger.debug("executing testInsertWhereWith");

		final StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("WITH ex:graph1 INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String message = "label should have been inserted in graph1 only, for ex:bob only";
		assertTrue(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph2));
//		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph2));
//		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true, graph1));
        assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}

    /**
     * <pre>
     * DELETE WHERE {?x foaf:name ?y }
     * </pre>
     * @throws Exception
     */
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

    /**
     * <pre>
     * DELETE WHERE {GRAPH ?g {?x foaf:name ?y} }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/568 (DELETE WHERE
     *      fails with Java AssertionError)
     */
    //@Test
    public void testDeleteWhereShortcut2()
        throws Exception
    {
        
        logger.debug("executing testDeleteWhereShortcut2");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE WHERE { GRAPH ?g {?x foaf:name ?y } }");

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
    
    /**
     * <pre>
     * DELETE {?x foaf:name ?y } WHERE {?x foaf:name ?y }
     * </pre>
     */
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

    /*
     * Note: blank nodes are not permitted in the DELETE clause template.
     * 
     * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
     * DELETE/INSERT WHERE handling of blank nodes </a>
     */
//	//@Test
//	public void testDeleteTransformedWhere()
//		throws Exception
//	{
//		logger.debug("executing testDeleteTransformedWhere");
//
//		StringBuilder update = new StringBuilder();
//		update.append(getNamespaceDeclarations());
//		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }");
//
//		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		operation.execute();
//
//		String msg = "foaf:name properties should have been deleted";
//		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		msg = "ex:containsPerson properties should not have been deleted";
//		assertTrue(msg, con.hasStatement(graph1, f.createURI(EX_NS, "containsPerson"), bob, true));
//		assertTrue(msg, con.hasStatement(graph2, f.createURI(EX_NS, "containsPerson"), alice, true));
//
//	}

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

    /**
     * @since openrdf 2.6.10 (but test is older than that).
     */
    public void testDeleteTransformedWhere()
            throws Exception
    {
        logger.debug("executing testDeleteTransformedWhere");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name ?n } WHERE {?x ex:containsPerson ?y . ?y foaf:name ?n . }");

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

    /**
     * @since openrdf 2.6.10
     */
    public void testInsertDataBlankNode()
        throws Exception
    {
        logger.debug("executing testInsertDataBlankNode");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { _:foo dc:title \"book 1\" ; dc:creator \"Ringo\" . } ");

        Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        assertFalse(con.hasStatement(null, DC.TITLE, f.createLiteral("book 1"), true));
        assertFalse(con.hasStatement(null, DC.CREATOR, f.createLiteral("Ringo"), true));

        operation.execute();

        RepositoryResult<Statement> titleStatements = con.getStatements(null, DC.TITLE, f.createLiteral("book 1"), true);
        assertNotNull(titleStatements);
        
        RepositoryResult<Statement> creatorStatements = con.getStatements(null, DC.CREATOR, f.createLiteral("Ringo"), true);
        assertNotNull(creatorStatements);
        
        BNode bookNode = null;
        if (titleStatements.hasNext()) {
            Statement ts = titleStatements.next();
            assertFalse(titleStatements.hasNext());
            
            Resource subject = ts.getSubject();
            assertTrue(subject instanceof BNode);
            bookNode = (BNode)subject;
        }
        titleStatements.close();
        assertNotNull(bookNode);
        assertFalse("_:foo".equals(bookNode.getID()));
        
        if (creatorStatements.hasNext()) {
            Statement cs = creatorStatements.next();
            assertFalse (creatorStatements.hasNext());
            
            Resource subject = cs.getSubject();
            assertTrue(subject instanceof BNode);
            assertEquals(bookNode, subject);
        }
        else {
            fail("at least one creator statement expected");
        }
        creatorStatements.close();
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

	/** @since OPENRDF 2.6.3 */
//    @Test
    public void testInsertDataInGraph2()
        throws Exception
    {
        logger.debug("executing testInsertDataInGraph2");

        final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT DATA { GRAPH ex:graph1 { ex:Human rdfs:subClassOf ex:Mammal. ex:Mammal rdfs:subClassOf ex:Animal. ex:george a ex:Human. ex:ringo a ex:Human. } } ");

        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        final URI human = f.createURI(EX_NS, "Human");
        final URI mammal = f.createURI(EX_NS, "Mammal");
        final URI george = f.createURI(EX_NS, "george");

        operation.execute();

        assertTrue(con.hasStatement(human, RDFS.SUBCLASSOF, mammal, true, graph1));
        assertTrue(con.hasStatement(mammal, RDFS.SUBCLASSOF, null, true, graph1));
        assertTrue(con.hasStatement(george, RDF.TYPE, human, true, graph1));
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

//        ((BigdataSailRepositoryConnection) con).getSailConnection()
//                .addListener(new ISPARQLUpdateListener() {
//            
//            @Override
//            public void updateEvent(SPARQLUpdateEvent e) {
//                System.err.println("elapsed="+e.getElapsedNanos()+", op="+e.getUpdate());
//            }
//        });

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

		assertTrue(con.hasStatement(alice, FOAF.NAME, null, false, graph2));
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

		URI graph3 = f.createURI(EX_NS, "graph3");

		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, false, graph1));
		operation.execute();
		assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, (Resource)null));
		assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, (Resource)null));
        assertTrue(con.hasStatement(graph1, DC.PUBLISHER, null, false, graph3));
        assertTrue(con.hasStatement(graph2, DC.PUBLISHER, null, false, graph3));
        assertTrue(con.hasStatement(alice, FOAF.KNOWS, bob, false, graph1));
        assertFalse(con.hasStatement(alice, FOAF.KNOWS, bob, false, graph3));
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
		final String update = "CLEAR ALL";

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

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
		final String update = "DROP ALL";

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

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

    /*
     * Note: blank nodes are not permitted in the DELETE clause template.
     * 
     * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
     * DELETE/INSERT WHERE handling of blank nodes </a>
     */
//	//@Test
//	public void testUpdateSequenceDeleteInsert()
//		throws Exception
//	{
//		logger.debug("executing testUpdateSequenceDeleteInsert");
//
//		StringBuilder update = new StringBuilder();
//		update.append(getNamespaceDeclarations());
//		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y }; ");
//		update.append(getNamespaceDeclarations());
//		update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x} ");
//
//		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		operation.execute();
//
//		String msg = "foaf:name properties should have been deleted";
//		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		msg = "foaf:name properties with value 'foo' should have been added";
//		assertTrue(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
//		assertTrue(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
//	}

    /*
     * Note: blank nodes are not permitted in the DELETE clause template.
     * 
     * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
     * DELETE/INSERT WHERE handling of blank nodes </a>
     */
//	//@Test
//	public void testUpdateSequenceInsertDelete()
//		throws Exception
//	{
//		logger.debug("executing testUpdateSequenceInsertDelete");
//
//		StringBuilder update = new StringBuilder();
//		update.append(getNamespaceDeclarations());
//		update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x}; ");
//		update.append(getNamespaceDeclarations());
//		update.append("DELETE {?y foaf:name [] } WHERE {?x ex:containsPerson ?y } ");
//
//		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
//
//		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		operation.execute();
//
//		String msg = "foaf:name properties should have been deleted";
//		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
//		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
//
//		msg = "foaf:name properties with value 'foo' should not have been added";
//		assertFalse(msg, con.hasStatement(bob, FOAF.NAME, f.createLiteral("foo"), true));
//		assertFalse(msg, con.hasStatement(alice, FOAF.NAME, f.createLiteral("foo"), true));
//	}

    /** @since openrdf 2.6.10 */
//    @Test
    public void testUpdateSequenceDeleteInsert()
        throws Exception
    {
        logger.debug("executing testUpdateSequenceDeleteInsert");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name ?n } WHERE {?x ex:containsPerson ?y. ?y foaf:name ?n . }; ");
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

    public void testUpdateSequenceInsertDelete()
        throws Exception
    {
        logger.debug("executing testUpdateSequenceInsertDelete");

        StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
        update.append("INSERT {?x foaf:name \"foo\" } WHERE {?y ex:containsPerson ?x}; ");
        update.append(getNamespaceDeclarations());
        update.append("DELETE {?y foaf:name ?n } WHERE {?x ex:containsPerson ?y. ?y foaf:name ?n . } ");

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

		final URI book1 = f.createURI("http://example/book1");
//		URI book3 = f.createURI("http://example/book3");
		final URI bookStore = f.createURI("http://example/bookStore");
		final URI bookStore2 = f.createURI("http://example/bookStore2");
		
		final StringBuilder update = new StringBuilder();
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

		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();

		String msg = "statements about book1 should have been removed from bookStore";
		assertFalse(msg, con.hasStatement(book1, null, null, true, bookStore));

		msg = "statements about book1 should have been added to bookStore2";
		assertTrue(msg, con.hasStatement(book1, RDF.TYPE, null, true, bookStore2));
		assertTrue(msg, con.hasStatement(book1, DC.DATE, null, true, bookStore2));
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, null, true, bookStore2));
	}

    /**
     * Unit test for
     * 
     * <pre>
     * DROP ALL;
     * INSERT DATA {
     * GRAPH <http://example.org/one> {
     * <a> <b> <c> .
     * <d> <e> <f> .
     * }};
     * ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;
     * DROP SILENT GRAPH <http://example.org/one>  ;
     * </pre>
     * 
     * The IV cache was not not being propagated correctly with the result that
     * we were seeing mock IVs for "one" and "two". The UPDATE would work
     * correctly the 2nd time since the URIs had been entered into the
     * dictionary by then.
     * 
     * @throws RepositoryException 
     * @throws MalformedQueryException 
     * @throws UpdateExecutionException 
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/567" >
     *      Failure to set cached value on IV results in incorrect behavior for
     *      complex UPDATE operation </a>
     */
	public void testTicket567() throws RepositoryException, MalformedQueryException, UpdateExecutionException {

        // replace the standard dataset with one specific to this case.
        con.clear();
        con.commit();

        final StringBuilder update = new StringBuilder();
        update.append("DROP ALL;\n");
        update.append("INSERT DATA {\n");
        update.append(" GRAPH <http://example.org/one> {\n");
        update.append("   <http://example.org/a> <http://example.org/b> <http://example.org/c> .\n");
        update.append("   <http://example.org/d> <http://example.org/e> <http://example.org/f> .\n");
        update.append("}};\n");
        update.append("ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;\n");
        update.append("DROP SILENT GRAPH <http://example.org/one>  ;\n");
        
        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        operation.execute();

        final URI one = f.createURI("http://example.org/one");
        final URI two = f.createURI("http://example.org/two");

        String msg = "Nothing in graph <one>";
        assertFalse(msg, con.hasStatement(null, null, null, true, one));

        msg = "statements are in graph <two>";
        assertTrue(msg, con.hasStatement(null, null, null, true, two));
	    
	}
	
    /**
     * In the ticket, the source file contains the following triples. In this
     * test, those are triples are written into the graph using INSERT DATA
     * rather than LOAD so this test can be self-contained.
     * 
     * <pre>
     * _:bnode <http://example/p> 2 .
     * _:bnode a <http://example/Foo> .
     * <http://example/s> <http://example/p> 2 .
     * </pre>
     * 
     * Load into graphA:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * DROP SILENT GRAPH tempGraph: ;
     * DROP SILENT GRAPH graphA: ;
     * INSERT DATA {
     * GRAPH graphA: {
     *   _:bnode <http://example/p> 2 . 
     *   _:bnode a <http://example/Foo> . 
     *   <http://example/s> <http://example/p> 2 . 
     * }}
     * </pre>
     * 
     * Verify that all three triples are in graphA:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH graphA: { ?s ?p ?v . } }
     * </pre>
     * 
     * Now delete some triples from graphA while inserting the deleted triples
     * into tempGraph:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * DROP SILENT GRAPH tempGraph: ;
     * DELETE { GRAPH graphA:    { ?s ?p ?v . } }
     * INSERT { GRAPH tempGraph: { ?s ?p ?v . } }
     * WHERE { GRAPH graphA: { 
     *     ?s a <http://example/Foo> .
     *     ?s ?p ?v . } }
     * </pre>
     * 
     * The result should be that the combination of tempGraph and graphA should
     * have the exact same number of triples that we started with. But now
     * notice that graphA has 0 triples:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH graphA: { ?s ?p ?v . } }
     * </pre>
     * 
     * However, tempGraph has only 2 triples:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH tempGraph: { ?s ?p ?v . } }
     * </pre>
     * 
     * so one triple is missing:
     * 
     * <pre>
     * <http://example/s> <http://example/p> 2 .
     * </pre>
     * 
     * @throws QueryEvaluationException
     */
    public void testTicket571() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException,
            QueryEvaluationException {

        final URI graphA = f.createURI("http://example.org/graphA");
        final URI tempGraph = f.createURI("http://example.org/tmp");
        final URI s = f.createURI("http://example/s>");
        final URI p = f.createURI("http://example/p>");
        final URI x = f.createURI("http://example/x>");
        final URI foo = f.createURI("http://example/Foo>");
        final URI rdfType = f.createURI(RDF.TYPE.stringValue());
        final Literal two = f.createLiteral("2", XSD.INTEGER);

        // replace the standard dataset with one specific to this case.
        con.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();

        /**
         * Load into graphA (note: file is "file:///tmp/junk.ttl" in the
         * ticket).
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
//         * PREFIX tempGraph:  <http://example/temp>
//         * DROP SILENT GRAPH tempGraph: ;
//         * DROP SILENT GRAPH graphA: ;
         * LOAD <file:///tmp/junk.ttl> INTO GRAPH graphA: ;
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL,//
                "PREFIX graphA:  <http://example/graphA> \n" + //
//                "PREFIX tempGraph:  <http://example/temp> \n"+//
//                "DROP SILENT GRAPH tempGraph: ;\n"+//
//                "DROP SILENT GRAPH graphA: ;\n"+//
                "INSERT DATA { \n"+//
                " GRAPH graphA: { \n" +//
                "   _:bnode <http://example/p> 2 . \n"+//
                "   _:bnode a <http://example/Foo> . \n"+//
                "   <http://example/s> <http://example/p> 2 . \n"+//
                "}}\n"//
//                "LOAD <file:bigdata-sails/src/test/org/openrdf/query/parser/sparql/ticket571.ttl> INTO GRAPH graphA: ;\n"//
        ).execute();
        
        /**
         * Verify that all three triples are in graphA:
         * 
         * <pre>
         * SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 3L, countSolutions(query));
            
        }
        
        /**
         * Now delete some triples from graphA while inserting the deleted
         * triples into tempGraph:
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
         * PREFIX tempGraph:  <http://example/temp>
         * DROP SILENT GRAPH tempGraph: ;  ### Note: redundant
         * DELETE { GRAPH graphA:    { ?s ?p ?v . } } 
         * INSERT { GRAPH tempGraph: { ?s ?p ?v . } }
         * WHERE { GRAPH graphA: { 
         *     ?s a <http://example/Foo> .
         *     ?s ?p ?v . } }
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL, //
                "PREFIX graphA:  <http://example/graphA> \n" + //
                "PREFIX tempGraph:  <http://example/temp> \n" +//
//                "DROP SILENT GRAPH tempGraph: ;\n"+//
                "DELETE { GRAPH graphA:    { ?s ?p ?v . } } \n"+//
                "INSERT { GRAPH tempGraph: { ?s ?p ?v . } } \n"+//
                "WHERE { GRAPH graphA: { \n"+//
                "    ?s a <http://example/Foo> . \n"+//
                "    ?s ?p ?v . } }\n"//
        ).execute();

        /**
         * graphA should have one triple remaining:
         * 
         * <pre>
         * <http://example/s> <http://example/p> 2 .
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 1L, countSolutions(query));
            
        }

        /**
         * The other 2 triples should have been moved into tempGraph.
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/temp> { ?s ?p ?v . } }";

            assertEquals("tempGraph", 2L, countSolutions(query));
            
        }

    }
    
    /**
     * This test is based on a forum post. This post provided an example of an
     * issue with Unicode case-folding in the REGEX operator and a means to
     * encode the Unicode characters to avoid doubt about which characters were
     * transmitted and receieved.
     * 
     * @throws Exception 
     * 
     * @see <a href=
     *      "https://sourceforge.net/projects/bigdata/forums/forum/676946/topic/7073971"
     *      >Forum post on the REGEX Unicode case-folding issue</a>
     *      
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/655">
     *      SPARQL REGEX operator does not perform case-folding correctly for
     *      Unicode data</a>
     */
    public void testUnicodeCleanAndRegex() throws Exception {

        /*
         * If I work around this problem by switching the unencoded Unicode
         * characters into SPARQL escape sequences like this:
         */

        // Insert statement:
        final String updateStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA { GRAPH ns:graph { ns:auml ns:label \"\u00C4\", \"\u00E4\" } }\n";

        con.prepareUpdate(QueryLanguage.SPARQL, updateStr).execute();

        // Test query:
        final String queryStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "SELECT * { GRAPH ns:graph { ?s ?p ?o FILTER(regex(?o, \"\u00E4\", \"i\")) } }";

        assertEquals(2L, countSolutions(queryStr));

        /*
         * Then I still get only one result for the query, the triple with '?'
         * which is \u00E4. But if I now add the 'u' flag to the regex, I get
         * both triples as result, so this seems to be a viable workaround.
         * Always setting the UNICODE_CASE flag sounds like a good idea, and in
         * fact, Jena ARQ seems to do that when given the 'i' flag:
         */
        
    }

    //@Test
	public void testLoad()
		throws Exception
	{
	    final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf>";
	    
	    final String ns = "http://bigdata.com/test/data#";
	    
		final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		
        assertTrue(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

	}

    //@Test
    public void testLoadSilent()
        throws Exception
    {
        final String update = "LOAD SILENT <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/NOT-FOUND.rdf>";
        
        final String ns = "http://bigdata.com/test/data#";
        
        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        operation.execute();

        assertFalse(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

	//@Test
	public void testLoadIntoGraph()
		throws Exception
	{

        final URI g1 = f.createURI("http://www.bigdata.com/g1");

        final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf> "
                + "INTO GRAPH <" + g1.stringValue() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

        operation.execute();
        
        assertFalse(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, (Resource)null));

        assertTrue(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true, g1));

	}

    /**
     * Verify ability to load data from a gzip resource.
     */
    public void testLoadGZip()
            throws Exception
        {
        final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf.gz>";
        
        final String ns = "http://bigdata.com/test/data#";
        
        con.prepareUpdate(QueryLanguage.SPARQL, update).execute();

        assertTrue(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }

    // TODO test for stopAtFirstError
    // TODO test for verifyData
    // TODO test for preserveBNodeIDs
    // TODO test for datatypeHandling
    // And apply those tests to the other UPDATE test suite as well (NSS)
//    public void testLoad_stopAtFirstError_false()
//            throws Exception
//        {
//            final String update = "LOAD stopAtFirstError=false <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/smallWithError.rdf>";
//            
//            final String ns = "http://bigdata.com/test/data#";
//            
//            final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);
//
//            operation.execute();
//
//            assertFalse(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
//                    f.createLiteral("Michael Personick"), true));
//
//        }

//    /**
//     * Verify ability to load data from a zip resource.
//     */
//    public void testLoadZip()
//            throws Exception
//        {
//        final String update = "LOAD <file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf.zip>";
//        
//        final String ns = "http://bigdata.com/test/data#";
//        
//        con.prepareUpdate(QueryLanguage.SPARQL, update).execute();
//
//        assertTrue(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
//                f.createLiteral("Michael Personick"), true));
//
//    }

	/* protected methods */

	protected void loadDataset(final String datasetFile)
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
     * Count solutions for a TupleQuery.
     * 
     * @param query
     *            The SPARQL query.
     * @return The #of solutions.
     * @throws QueryEvaluationException
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    protected long countSolutions(final String query)
            throws QueryEvaluationException, RepositoryException,
            MalformedQueryException {
        TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL,
                query).evaluate();
        try {
            long n = 0;
            while (result.hasNext()) {
                final BindingSet bset = result.next();
                n++;
                if (logger.isInfoEnabled())
                    logger.info(bset.toString());
            }
            return n;
        } finally {
            result.close();
        }
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
