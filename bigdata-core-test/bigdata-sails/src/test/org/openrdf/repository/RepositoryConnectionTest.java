/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
/*
 * Pulled in to extend TestCase.
 */
package org.openrdf.repository;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import junit.framework.TestCase;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iterations;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.util.Namespaces;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.contextaware.ContextAwareConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.store.BD;

public abstract class RepositoryConnectionTest extends TestCase {

	/**
	 * Timeout all individual tests after 1 minute.
	 */
	@Rule
	public Timeout to = new Timeout(60000);

	private static final String URN_TEST_OTHER = "urn:test:other";

	private static final String SPARQL_DEL_ALL = "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }";

	private static final String URN_TEST_O1 = "urn:test:o1";

	private static final String URN_TEST_S1 = "urn:test:s1";

	private static final String URN_TEST_P2 = "urn:test:p2";

	private static final String URN_TEST_P1 = "urn:test:p1";

	private static final String URN_PRED = "urn:pred";

	private static final String RDF_PREFIX = "rdf";

	private static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";

	private static final String RDFS_PREFIX = "rdfs";

	private static final String EXAMPLE_NS = "http://example.org/";

	private static final String EXAMPLE = "example";

	private static final String ASK = "ASK ";

	private static final String PREFIX_FOAF = "PREFIX foaf: <";

	private static final String PERSON = "person";

	private static final String UNEXPECTED_TYPE = "unexpected query object type: ";

	private static final String UNSUPPORTED_OP = "unsupported operation: ";

	private static final String NEWLY_ADDED = "Repository should contain newly added statement";

	private static final String MBOX = "mbox";

	private static final String NAME = "name";

	protected static final String FOAF_NS = "http://xmlns.com/foaf/0.1/";

	public static final String TEST_DIR_PREFIX = "/testcases/";

	protected Repository testRepository;

	protected RepositoryConnection testCon;

	protected RepositoryConnection testCon2;

	protected ValueFactory vf;

	protected Resource bob;

	protected Resource alice;

	protected Resource alexander;

	protected URI name;

	protected URI mbox;

	protected final URI publisher = DC.PUBLISHER;

	protected URI unknownContext;

	protected URI context1;

	protected URI context2;

	protected Literal nameAlice;

	protected Literal nameBob;

	protected Literal mboxAlice;

	protected Literal mboxBob;

	public RepositoryConnectionTest(String name) {
	    super(name);
	}
	
	@Before
	public void setUp()
		throws Exception
	{
		testRepository = createRepository();
		testRepository.initialize();

		testCon = testRepository.getConnection();
		testCon.clear();
		testCon.clearNamespaces();

		testCon2 = testRepository.getConnection();

		vf = testRepository.getValueFactory();

		// Initialize values
		bob = vf.createBNode();
		alice = vf.createBNode();
		alexander = vf.createBNode();

		name = vf.createURI(FOAF_NS + NAME);
		mbox = vf.createURI(FOAF_NS + MBOX);

		nameAlice = vf.createLiteral("Alice");
		nameBob = vf.createLiteral("Bob");

		mboxAlice = vf.createLiteral("alice@example.org");
		mboxBob = vf.createLiteral("bob@example.org");

		unknownContext = new URIImpl("urn:unknownContext");

		context1 = vf.createURI("urn:x-local:graph1");
		context2 = vf.createURI("urn:x-local:graph2");
	}

	@After
	public void tearDown()
		throws Exception
	{
		testCon2.close();
		testCon.close();
		testRepository.shutDown();
	}

	/**
	 * Gets an (uninitialized) instance of the repository that should be tested.
	 * 
	 * @return an uninitialized repository.
	 */
	protected abstract Repository createRepository()
		throws Exception;

	@Test
	public void testAddStatement()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		assertTrue(NEWLY_ADDED, testCon.hasStatement(bob, name, nameBob, false));

		Statement statement = vf.createStatement(alice, name, nameAlice);
		testCon.add(statement);

		assertTrue(NEWLY_ADDED, testCon.hasStatement(statement, false));
		assertTrue(NEWLY_ADDED, testCon.hasStatement(alice, name, nameAlice, false));

		Repository tempRep = new SailRepository(new MemoryStore());
		tempRep.initialize();
		RepositoryConnection con = tempRep.getConnection();

		con.add(testCon.getStatements(null, null, null, false));

		assertTrue("Temp Repository should contain newly added statement",
				con.hasStatement(bob, name, nameBob, false));
		con.close();
		tempRep.shutDown();
	}

	@Test
	public void testAddLiteralWithNewline()
		throws Exception
	{
		Literal test = vf.createLiteral("this is a test\n");
		testCon.add(bob, RDFS.LABEL, test);

		assertTrue(NEWLY_ADDED, testCon.hasStatement(bob, RDFS.LABEL, test, false));
	}

	@Test
	public void testTransactionIsolation()
		throws Exception
	{
		testCon.begin();
		testCon.add(bob, name, nameBob);
		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
		assertThat(testCon2.hasStatement(bob, name, nameBob, false), is(equalTo(false)));
		testCon.commit();
		testCon2.close();
		testCon2 = testRepository.getConnection();
		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
		assertThat(testCon2.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
	}

	@Test
	public void testReadOfAddedStatement1()
		throws Exception
	{
		URI context = vf.createURI("urn:context");
		URI subject = vf.createURI("urn:test");
		URI predicate = vf.createURI("urn:property");
		Value one = vf.createLiteral(1);
		Value two = vf.createLiteral(2);

		testCon.begin();
		testCon.add(subject, predicate, one, context);
		testCon.commit();

		testCon.begin();

		RepositoryResult<Statement> statements = testCon.getStatements(null, predicate, null, false, context);
		assertTrue("should find the add", statements.hasNext());
		statements.next();

		testCon.add(subject, predicate, two, context);
		// no.
//		assertTrue("should find the second add", statements.hasNext());

		statements.close();
		testCon.rollback();
	}

	@Test
	public void testReadOfAddedStatement2()
		throws Exception
	{
		URI duff = vf.createURI("urn:duff");
		URI context = vf.createURI("urn:context");
		URI subject = vf.createURI("urn:test");
		URI predicate = vf.createURI("urn:property");
		Value one = vf.createLiteral(1);
		Value two = vf.createLiteral(2);

		testCon.begin();
		testCon.add(subject, predicate, one, context);
		testCon.commit();

		testCon.begin();
		testCon.add(duff, duff, duff, duff);
		RepositoryResult<Statement> statements = testCon.getStatements(null, predicate, null, false, context);
		assertTrue("should find the add", statements.hasNext());
		statements.next();

		testCon.add(subject, predicate, two, context);
        // no.
//		assertTrue("should find the second add", statements.hasNext());

		statements.close();
		testCon.rollback();
	}

	@Test
	public void testTransactionIsolationForRead()
		throws Exception
	{
		testCon.begin();
		try {
			// Add but do not commit
			testCon.add(OWL.CLASS, RDFS.COMMENT, RDF.STATEMENT);
			assertTrue("Should be able to see uncommitted statement on same connection",
					testCon.hasStatement(OWL.CLASS, RDFS.COMMENT, RDF.STATEMENT, true));

			assertFalse(
					"Should not be able to see uncommitted statement on separate connection outside transaction",
					testCon2.hasStatement(OWL.CLASS, RDFS.COMMENT, RDF.STATEMENT, true));

			testCon2.begin();
			try {
				assertFalse(
						"Should not be able to see uncommitted statement on separate connection inside transaction",
						testCon2.hasStatement(OWL.CLASS, RDFS.COMMENT, RDF.STATEMENT, true));
			}
			finally {
				testCon2.rollback();
			}

		}
		finally {
			testCon.rollback();
		}

	}

	@Test
	public void testAddReader()
		throws Exception
	{
		Reader defaultGraph = new InputStreamReader(
				RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "default-graph.ttl"),
				"UTF-8");
		testCon.add(defaultGraph, "", RDFFormat.TURTLE);
		defaultGraph.close();
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameBob, false));
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameAlice, false));

		// add file graph1.ttl to context1
		InputStream graph1Stream = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "graph1.ttl");
		Reader graph1 = new InputStreamReader(graph1Stream, "UTF-8");
		testCon.add(graph1, "", RDFFormat.TURTLE, context1);
		graph1.close();

		// add file graph2.ttl to context2
		InputStream graph2Stream = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "graph2.ttl");
		Reader graph2 = new InputStreamReader(graph2Stream, "UTF-8");
		testCon.add(graph2, "", RDFFormat.TURTLE, context2);
		graph2.close();
		assertTrue("alice should be known in the store", testCon.hasStatement(null, name, nameAlice, false));
		assertFalse("alice should not be known in context1",
				testCon.hasStatement(null, name, nameAlice, false, context1));
		assertTrue("alice should be known in context2",
				testCon.hasStatement(null, name, nameAlice, false, context2));
		assertTrue("bob should be known in the store", testCon.hasStatement(null, name, nameBob, false));
		assertFalse("bob should not be known in context2",
				testCon.hasStatement(null, name, nameBob, false, context2));
		assertTrue("bib should be known in context1",
				testCon.hasStatement(null, name, nameBob, false, context1));
	}

	@Test
	public void testAddInputStream()
		throws Exception
	{
		// add file default-graph.ttl to repository, no context
		InputStream defaultGraph = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "default-graph.ttl");
		testCon.add(defaultGraph, "", RDFFormat.TURTLE);
		defaultGraph.close();
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameBob, false));
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameAlice, false));

		// add file graph1.ttl to context1
		InputStream graph1 = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "graph1.ttl");
		testCon.add(graph1, "", RDFFormat.TURTLE, context1);
		graph1.close();

		// add file graph2.ttl to context2
		InputStream graph2 = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "graph2.ttl");
		testCon.add(graph2, "", RDFFormat.TURTLE, context2);
		graph2.close();
		assertTrue("alice should be known in the store", testCon.hasStatement(null, name, nameAlice, false));
		assertFalse("alice should not be known in context1",
				testCon.hasStatement(null, name, nameAlice, false, context1));
		assertTrue("alice should be known in context2",
				testCon.hasStatement(null, name, nameAlice, false, context2));
		assertTrue("bob should be known in the store", testCon.hasStatement(null, name, nameBob, false));
		assertFalse("bob should not be known in context2",
				testCon.hasStatement(null, name, nameBob, false, context2));
		assertTrue("bib should be known in context1",
				testCon.hasStatement(null, name, nameBob, false, context1));
	}

	@Test
	public void testAddGzipInputStream()
		throws Exception
	{
		// add file default-graph.ttl to repository, no context
		InputStream defaultGraph = RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "default-graph.ttl.gz");
		try {
			testCon.add(defaultGraph, "", RDFFormat.TURTLE);
		}
		finally {
			defaultGraph.close();
		}

		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameBob, false));
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameAlice, false));

	}

	@Test
	public void testAddZipFile()
		throws Exception
	{
		testCon.add(RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "graphs.zip"), "",
				RDFFormat.TURTLE);
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameBob, false));
		assertTrue(NEWLY_ADDED, testCon.hasStatement(null, publisher, nameAlice, false));
		assertTrue("alice should be known in the store", testCon.hasStatement(null, name, nameAlice, false));
		assertTrue("bob should be known in the store", testCon.hasStatement(null, name, nameBob, false));
	}

	@Test
	public void testAddMalformedLiteralsDefaultConfig()
		throws Exception
	{
		try {
			testCon.add(
					RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"),
					"", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in default configuration");
		}
		catch (RDFParseException e) {
			// ignore, as expected
		}
	}

	@Test
	public void testAddMalformedLiteralsStrictConfig()
		throws Exception
	{
		Set<RioSetting<?>> empty = Collections.emptySet();
		testCon.getParserConfig().setNonFatalErrors(empty);

		try {
			testCon.add(
					RepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"),
					"", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in strict configuration");

		}
		catch (RDFParseException e) {
			// ingnore, as expected.
		}
	}

	@Test
	public void testAutoCommit()
		throws Exception
	{
		testCon.begin();
		testCon.add(alice, name, nameAlice);

		assertTrue("Uncommitted update should be visible to own connection",
				testCon.hasStatement(alice, name, nameAlice, false));

		testCon.commit();

		assertTrue("Repository should contain statement after commit",
				testCon.hasStatement(alice, name, nameAlice, false));
	}

	@Test
	public void testRollback()
		throws Exception
	{
		testCon.begin();
		testCon.add(alice, name, nameAlice);

		assertTrue("Uncommitted updates should be visible to own connection",
				testCon.hasStatement(alice, name, nameAlice, false));

		testCon.rollback();

		assertFalse("Repository should not contain statement after rollback",
				testCon.hasStatement(alice, name, nameAlice, false));
	}

	@Test
	public void testSimpleTupleQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		StringBuilder queryBuilder = new StringBuilder(128);
        queryBuilder.append(" prefix foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?name ?mbox");
        queryBuilder.append(" where { ?s foaf:name ?name . ?s foaf:mbox ?mbox . }");
		TupleQueryResult result = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding(NAME), is(equalTo(true)));
				assertThat(solution.hasBinding(MBOX), is(equalTo(true)));
				Value nameResult = solution.getValue(NAME);
				Value mboxResult = solution.getValue(MBOX);
				assertThat(nameResult, anyOf(is(equalTo((Value)nameAlice)), is(equalTo((Value)nameBob))));
				assertThat(mboxResult, anyOf(is(equalTo((Value)mboxAlice)), is(equalTo((Value)mboxBob))));
			}
		}
		finally {
			result.close();
		}
	}

	@Test
	public void testPrepareSPARQLQuery()
		throws Exception
	{

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?person");
		queryBuilder.append(" WHERE { ?person foaf:name ?y . }");

		try {
			testCon.prepareQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		}
		catch (UnsupportedOperationException e) {
			fail(UNSUPPORTED_OP + e.getMessage());
		}
		catch (ClassCastException e) {
			fail(UNEXPECTED_TYPE + e.getMessage());
		}

		queryBuilder = new StringBuilder();
		queryBuilder.append(" BASE <http://base.uri>");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX ex: <http://example.org/>");
		queryBuilder.append(" PREFIX : <http://example.org/foo#>");
		queryBuilder.append(" SELECT ?person");
		queryBuilder.append(" WHERE { ?person foaf:name ?y . }");

		try {
			testCon.prepareQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		}
		catch (UnsupportedOperationException e) {
			fail(UNSUPPORTED_OP + e.getMessage());
		}
		catch (ClassCastException e) {
			fail(UNEXPECTED_TYPE + e.getMessage());
		}
	}

	@Test
	public void testPreparedTupleQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(" prefix foaf: <" + FOAF_NS + ">");
        queryBuilder.append(" select ?name ?mbox ");
        queryBuilder.append(" where { ?s foaf:name ?name . ?s foaf:mbox ?mbox . }");
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);
		TupleQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding(NAME), is(equalTo(true)));
				assertThat(solution.hasBinding(MBOX), is(equalTo(true)));
				Value nameResult = solution.getValue(NAME);
				Value mboxResult = solution.getValue(MBOX);
				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
			}
		}
		finally {
			result.close();
		}
	}

	@Test
	public void testPreparedTupleQuery2()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(" prefix foaf: <" + FOAF_NS + ">");
        queryBuilder.append(" SELECT ?name ?mbox");
        queryBuilder.append(" where { ?VAR foaf:name ?name . ?VAR foaf:mbox ?mbox . }");
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("VAR", bob);
		TupleQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding(NAME), is(equalTo(true)));
				assertThat(solution.hasBinding(MBOX), is(equalTo(true)));
				Value nameResult = solution.getValue(NAME);
				Value mboxResult = solution.getValue(MBOX);
				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
			}
		}
		finally {
			result.close();
		}
	}

	@Test
	public void testSimpleGraphQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);

		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);

		StringBuilder queryBuilder = new StringBuilder(128);
        queryBuilder.append(" prefix foaf: <" + FOAF_NS + ">");
        queryBuilder.append(" construct ");
        queryBuilder.append(" where { ?s foaf:name ?name . ?s foaf:mbox ?mbox . }");

		GraphQueryResult result = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();

		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));

			while (result.hasNext()) {
				Statement st = result.next();
				if (name.equals(st.getPredicate())) {
					assertThat(st.getObject(), anyOf(is(equalTo((Value)nameAlice)), is(equalTo((Value)nameBob))));
				}
				else {
					assertThat(st.getPredicate(), is(equalTo(mbox)));
					assertThat(st.getObject(), anyOf(is(equalTo((Value)mboxAlice)), is(equalTo((Value)mboxBob))));
				}
			}
		}
		finally {
			result.close();
		}
	}

	@Test
	public void testPreparedGraphQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		StringBuilder queryBuilder = new StringBuilder(128);
        queryBuilder.append(" prefix foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" construct ");
		queryBuilder.append(" where { ?s foaf:name ?name . ?s foaf:mbox ?mbox . }");
		GraphQuery query = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);
		GraphQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				URI predicate = st.getPredicate();
				assertThat(predicate, anyOf(is(equalTo(name)), is(equalTo(mbox))));
				Value object = st.getObject();
				if (name.equals(predicate)) {
					assertEquals("unexpected value for name: " + object, nameBob, object);
				}
				else {
					assertThat(predicate, is(equalTo(mbox)));
					assertEquals("unexpected value for mbox: " + object, mboxBob, object);
				}
			}
		}
		finally {
			result.close();
		}
	}

	@Test
	public void testSimpleBooleanQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);

		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);

		StringBuilder queryBuilder = new StringBuilder(64);
		queryBuilder.append(PREFIX_FOAF + FOAF_NS + "> ");
		queryBuilder.append(ASK);
		queryBuilder.append("{ ?p foaf:name ?name }");

		boolean exists = testCon.prepareBooleanQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();

		assertThat(exists, is(equalTo(true)));
	}

	@Test
	public void testPreparedBooleanQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);

		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append(PREFIX_FOAF + FOAF_NS + "> ");
		queryBuilder.append(ASK);
		queryBuilder.append("{ ?p foaf:name ?name }");

		BooleanQuery query = testCon.prepareBooleanQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);

		assertThat(query.evaluate(), is(equalTo(true)));
	}

	@Test
	public void testDataset()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append(PREFIX_FOAF + FOAF_NS + "> ");
		queryBuilder.append(ASK);
		queryBuilder.append("{ ?p foaf:name ?name }");
		BooleanQuery query = testCon.prepareBooleanQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);
		assertThat(query.evaluate(), is(equalTo(true)));
		DatasetImpl dataset = new DatasetImpl();

		// default graph: {context1}
		dataset.addDefaultGraph(context1);
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(true)));

		// default graph: {context1, context2}
		dataset.addDefaultGraph(context2);
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(true)));

		// default graph: {context2}
		dataset.removeDefaultGraph(context1);
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(false)));
		queryBuilder.setLength(0);
		queryBuilder.append(PREFIX_FOAF + FOAF_NS + "> ");
		queryBuilder.append(ASK);
		queryBuilder.append("{ GRAPH ?g { ?p foaf:name ?name } }");
		query = testCon.prepareBooleanQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);

		// default graph: {context2}; named graph: {}
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(false)));

		// default graph: {context1, context2}; named graph: {context2}
		dataset.addDefaultGraph(context1);
		dataset.addNamedGraph(context2);
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(false)));

		// default graph: {context1, context2}; named graph: {context1, context2}
		dataset.addNamedGraph(context1);
		query.setDataset(dataset);
		assertThat(query.evaluate(), is(equalTo(true)));
	}

	@Test
	public void testGetStatements()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		assertTrue("Repository should contain statement", testCon.hasStatement(bob, name, nameBob, false));

		RepositoryResult<Statement> result = testCon.getStatements(null, name, null, false);

		try {
			assertNotNull("Iterator should not be null", result);
			assertTrue("Iterator should not be empty", result.hasNext());

			while (result.hasNext()) {
				Statement st = result.next();
				assertNull("Statement should not be in a context ", st.getContext());
				assertTrue("Statement predicate should be equal to name ", st.getPredicate().equals(name));
			}
		}
		finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testCon.getStatements(null, name, null, false),
				new ArrayList<Statement>());

		assertNotNull("List should not be null", list);
		assertFalse("List should not be empty", list.isEmpty());
	}

	@Test
	public void testGetStatementsMalformedTypedLiteral()
		throws Exception
	{
		Literal invalidIntegerLiteral = vf.createLiteral("the number four", XMLSchema.INTEGER);
		try {
			URI pred = vf.createURI(URN_PRED);
			testCon.add(bob, pred, invalidIntegerLiteral);

			RepositoryResult<Statement> statements = testCon.getStatements(bob, pred, null, true);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(invalidIntegerLiteral));
		}
		catch (RepositoryException e) {
			// shouldn't happen
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetStatementsMalformedLanguageLiteral()
		throws Exception
	{
		Literal invalidLanguageLiteral = vf.createLiteral("the number four", "en_us");
		try {
			URI pred = vf.createURI(URN_PRED);
			testCon.add(bob, pred, invalidLanguageLiteral);

			RepositoryResult<Statement> statements = testCon.getStatements(bob, pred, null, true);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(invalidLanguageLiteral));
		}
		catch (RepositoryException e) {
			e.printStackTrace();
			// shouldn't happen
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetStatementsInSingleContext()
		throws Exception
	{
		testCon.begin();
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.commit();
		assertTrue("Repository should contain statement", testCon.hasStatement(bob, name, nameBob, false));
		assertTrue("Repository should contain statement in context1",
				testCon.hasStatement(bob, name, nameBob, false, context1));
		assertFalse("Repository should not contain statement in context2",
				testCon.hasStatement(bob, name, nameBob, false, context2));

		// Check handling of getStatements without context IDs
		RepositoryResult<Statement> result = testCon.getStatements(bob, name, null, false);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getSubject(), is(equalTo((Resource)bob)));
				assertThat(st.getPredicate(), is(equalTo(name)));
				assertThat(st.getObject(), is(equalTo((Value)nameBob)));
				assertThat(st.getContext(), is(equalTo((Resource)context1)));
			}
		}
		finally {
			result.close();
		}

		// Check handling of getStatements with a known context ID
		result = testCon.getStatements(null, null, null, false, context1);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getContext(), is(equalTo((Resource)context1)));
			}
		}
		finally {
			result.close();
		}

		// Check handling of getStatements with an unknown context ID
		result = testCon.getStatements(null, null, null, false, unknownContext);
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(false)));
		}
		finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testCon.getStatements(null, name, null, false, context1),
				new ArrayList<Statement>());
		assertNotNull("List should not be null", list);
		assertFalse("List should not be empty", list.isEmpty());
	}

	@Test
	public void testGetStatementsInMultipleContexts()
		throws Exception
	{
		testCon.clear();

		testCon.begin();
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
		testCon.commit();

		// get statements with either no context or context2
		CloseableIteration<? extends Statement, RepositoryException> iter = testCon.getStatements(null, null,
				null, false, null, context2);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource)context2))));
			}

			assertEquals("there should be three statements", 3, count);
		}
		finally {
			iter.close();
		}

		// get all statements with context1 or context2. Note that context1 and
		// context2 are both known
		// in the store because they have been created through the store's own
		// value vf.
		iter = testCon.getStatements(null, null, null, false, context1, context2);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				// we should have _only_ statements from context2
				assertThat(st.getContext(), is(equalTo((Resource)context2)));
			}
			assertEquals("there should be two statements", 2, count);
		}
		finally {
			iter.close();
		}

		// get all statements with unknownContext or context2.
		iter = testCon.getStatements(null, null, null, false, unknownContext, context2);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				// we should have _only_ statements from context2
				assertThat(st.getContext(), is(equalTo((Resource)context2)));
			}
			assertEquals("there should be two statements", 2, count);
		}
		finally {
			iter.close();
		}

		// add statements to context1
		testCon.begin();
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
		testCon.commit();

		iter = testCon.getStatements(null, null, null, false, context1);
		try {
			assertThat(iter, is(notNullValue()));
			assertThat(iter.hasNext(), is(equalTo(true)));
		}
		finally {
			iter.close();
		}

		// get statements with either no context or context2
		iter = testCon.getStatements(null, null, null, false, null, context2);
		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				// we should have _only_ statements from context2, or without
				// context
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource)context2))));
			}
			assertEquals("there should be four statements", 4, count);
		}
		finally {
			iter.close();
		}

		// get all statements with context1 or context2
		iter = testCon.getStatements(null, null, null, false, context1, context2);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(),
						anyOf(is(equalTo((Resource)context1)), is(equalTo((Resource)context2))));
			}
			assertEquals("there should be four statements", 4, count);
		}
		finally {
			iter.close();
		}
	}

	@Test
	public void testDuplicateFilter()
		throws Exception
	{
		testCon.begin();
		testCon.add(bob, name, nameBob);
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, name, nameBob, context2);
		testCon.commit();

		RepositoryResult<Statement> result = testCon.getStatements(bob, name, null, true);
		result.enableDuplicateFilter();

		int count = 0;
		while (result.hasNext()) {
			result.next();
			count++;
		}
		assertThat(count, is(equalTo(1)));
	}

	@Test
	public void testRemoveStatements()
		throws Exception
	{
		testCon.begin();
		testCon.add(bob, name, nameBob);
		testCon.add(alice, name, nameAlice);
		testCon.commit();

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(true)));

		testCon.remove(bob, name, nameBob);

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(false)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(true)));

		testCon.remove(alice, null, null);
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(false)));
		assertThat(testCon.isEmpty(), is(equalTo(true)));
	}

	@Test
	public void testRemoveStatementCollection()
		throws Exception
	{
		testCon.begin();
		testCon.add(alice, name, nameAlice);
		testCon.add(bob, name, nameBob);
		testCon.commit();

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(true)));

		Collection<Statement> c = Iterations.addAll(testCon.getStatements(null, null, null, false),
				new ArrayList<Statement>());

		testCon.remove(c);

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(false)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(false)));
	}

	@Test
	public void testRemoveStatementIteration()
		throws Exception
	{
		testCon.begin();
		testCon.add(alice, name, nameAlice);
		testCon.add(bob, name, nameBob);
		testCon.commit();

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(true)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(true)));

		CloseableIteration<? extends Statement, RepositoryException> iter = testCon.getStatements(null, null,
				null, false);

		try {
			testCon.remove(iter);
		}
		finally {
			iter.close();
		}

		assertThat(testCon.hasStatement(bob, name, nameBob, false), is(equalTo(false)));
		assertThat(testCon.hasStatement(alice, name, nameAlice, false), is(equalTo(false)));
	}

	@Test
	public void testGetNamespace()
		throws Exception
	{
		setupNamespaces();
		assertThat(testCon.getNamespace(EXAMPLE), is(equalTo(EXAMPLE_NS)));
		assertThat(testCon.getNamespace(RDFS_PREFIX), is(equalTo(RDFS_NS)));
		assertThat(testCon.getNamespace(RDF_PREFIX), is(equalTo("http://www.w3.org/1999/02/22-rdf-syntax-ns#")));
		assertThat(testCon.getNamespace("undefined"), is(nullValue()));
	}

	@Test
	public void testGetNamespaces()
		throws Exception
	{
		setupNamespaces();
		Map<String, String> map = Namespaces.asMap(Iterations.asSet(testCon.getNamespaces()));
		assertThat(map.size(), is(equalTo(3)));
		assertThat(map.keySet(), hasItems(EXAMPLE, RDFS_PREFIX, RDF_PREFIX));
		assertThat(map.get(EXAMPLE), is(equalTo(EXAMPLE_NS)));
		assertThat(map.get(RDFS_PREFIX), is(equalTo(RDFS_NS)));
		assertThat(map.get(RDF_PREFIX), is(equalTo("http://www.w3.org/1999/02/22-rdf-syntax-ns#")));
	}

	private void setupNamespaces()
		throws IOException, RDFParseException, RepositoryException
	{
		testCon.setNamespace(EXAMPLE, EXAMPLE_NS);
		testCon.setNamespace(RDF_PREFIX, "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		testCon.setNamespace(RDFS_PREFIX, RDFS_NS);

		// Translated from earlier RDF document. Is this line even necessary?
		testCon.add(vf.createURI(EXAMPLE_NS, "Main"), vf.createURI(RDFS_NS, "label"),
				vf.createLiteral("Main Node"));
	}

	@Test
	public void testClear()
		throws Exception
	{
		testCon.add(bob, name, nameBob);
		assertThat(testCon.hasStatement(null, name, nameBob, false), is(equalTo(true)));
		testCon.clear();
		assertThat(testCon.hasStatement(null, name, nameBob, false), is(equalTo(false)));
	}

	@Test
	public void testRecoverFromParseError()
		throws RepositoryException, IOException
	{
		String invalidData = "bad";
		String validData = "@prefix foo: <http://example.org/foo#>.\nfoo:a foo:b foo:c.";

		try {
			testCon.add(new StringReader(invalidData), "", RDFFormat.TURTLE);
			fail("Invalid data should result in an exception");
		}
		catch (RDFParseException e) {
			// Expected behaviour
		}

		try {
			testCon.add(new StringReader(validData), "", RDFFormat.TURTLE);
		}
		catch (RDFParseException e) {
			fail("Valid data should not result in an exception");
		}

		assertEquals("Repository contains incorrect number of statements", 1, testCon.size());
	}

	@Test
	public void testStatementSerialization()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		Statement st;
		RepositoryResult<Statement> statements = testCon.getStatements(null, null, null, true);
		try {
			st = statements.next();
		}
		finally {
			statements.close();
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(baos);
		out.writeObject(st);
		out.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bais);
		Statement deserialized = (Statement)in.readObject();
		in.close();

		assertThat(deserialized, is(equalTo(st)));

		assertThat(testCon.hasStatement(st, true), is(equalTo(true)));
		assertThat(testCon.hasStatement(deserialized, true), is(equalTo(true)));
	}

	@Test
	public void testBNodeSerialization()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		Statement st;
		RepositoryResult<Statement> statements = testCon.getStatements(null, null, null, false);
		try {
			st = statements.next();
		}
		finally {
			statements.close();
		}

		BNode bnode = (BNode)st.getSubject();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(baos);
		out.writeObject(bnode);
		out.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bais);
		BNode deserializedBNode = (BNode)in.readObject();
		in.close();

		assertThat(deserializedBNode, is(equalTo(bnode)));

		assertThat(testCon.hasStatement(bnode, name, nameBob, true), is(equalTo(true)));
		assertThat(testCon.hasStatement(deserializedBNode, name, nameBob, true), is(equalTo(true)));
	}

	@Test
	public void testURISerialization()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		Statement st;
		RepositoryResult<Statement> statements = testCon.getStatements(null, null, null, false);
		try {
			st = statements.next();
		}
		finally {
			statements.close();
		}

		URI uri = st.getPredicate();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(baos);
		out.writeObject(uri);
		out.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bais);
		URI deserializedURI = (URI)in.readObject();
		in.close();

		assertThat(deserializedURI, is(equalTo(uri)));

		assertThat(testCon.hasStatement(bob, uri, nameBob, true), is(equalTo(true)));
		assertThat(testCon.hasStatement(bob, deserializedURI, nameBob, true), is(equalTo(true)));
	}

	@Test
	public void testLiteralSerialization()
		throws Exception
	{
		testCon.add(bob, name, nameBob);

		Statement st;
		RepositoryResult<Statement> statements = testCon.getStatements(null, null, null, false);
		try {
			st = statements.next();
		}
		finally {
			statements.close();
		}

		Literal literal = (Literal)st.getObject();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(baos);
		out.writeObject(literal);
		out.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bais);
		Literal deserialized = (Literal)in.readObject();
		in.close();

		assertThat(deserialized, is(equalTo(literal)));

		assertThat(testCon.hasStatement(bob, name, literal, true), is(equalTo(true)));
		assertThat(testCon.hasStatement(bob, name, deserialized, true), is(equalTo(true)));
	}

	@Test
	public void testGraphSerialization()
		throws Exception
	{
		testCon.add(bob, name, nameBob);
		testCon.add(alice, name, nameAlice);

		Graph graph;
		RepositoryResult<Statement> statements = testCon.getStatements(null, null, null, true);
		try {
			graph = new LinkedHashModel(Iterations.asList(statements));
		}
		finally {
			statements.close();
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(baos);
		out.writeObject(graph);
		out.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bais);
		Graph deserializedGraph = (Graph)in.readObject();
		in.close();

		assertThat(deserializedGraph.isEmpty(), is(equalTo(false)));

		for (Statement st : deserializedGraph) {
			assertThat(graph, hasItem(st));
			assertThat(testCon.hasStatement(st, true), is(equalTo(true)));
		}
	}

	@Test
	public void testEmptyRollback()
		throws Exception
	{
		assertThat(testCon.isEmpty(), is(equalTo(true)));
		assertThat(testCon2.isEmpty(), is(equalTo(true)));
		testCon.begin();
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.isEmpty(), is(equalTo(false)));
		assertThat(testCon2.isEmpty(), is(equalTo(true)));
		testCon.rollback();
		assertThat(testCon.isEmpty(), is(equalTo(true)));
		assertThat(testCon2.isEmpty(), is(equalTo(true)));
	}

	@Test
	public void testEmptyCommit()
		throws Exception
	{
		assertThat(testCon.isEmpty(), is(equalTo(true)));
		assertThat(testCon2.isEmpty(), is(equalTo(true)));
		testCon.begin();
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.isEmpty(), is(equalTo(false)));
		assertThat(testCon2.isEmpty(), is(equalTo(true)));
		testCon.commit();
		testCon2.close();
		testCon2 = testRepository.getConnection();
		assertThat(testCon.isEmpty(), is(equalTo(false)));
		assertThat(testCon2.isEmpty(), is(equalTo(false)));
	}

	@Test
	public void testOpen()
		throws Exception
	{
		assertThat(testCon.isOpen(), is(equalTo(true)));
		assertThat(testCon2.isOpen(), is(equalTo(true)));
		testCon.close();
		assertThat(testCon.isOpen(), is(equalTo(false)));
		assertThat(testCon2.isOpen(), is(equalTo(true)));
	}

	@Test
	public void testSizeRollback()
		throws Exception
	{
		assertThat(testCon.size(), is(equalTo(0L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.begin();
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.size(), is(equalTo(1L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.size(), is(equalTo(2L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.rollback();
		assertThat(testCon.size(), is(equalTo(0L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
	}

	@Test
	public void testSizeCommit()
		throws Exception
	{
		assertThat(testCon.size(), is(equalTo(0L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.begin();
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.size(), is(equalTo(1L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.add(vf.createBNode(), vf.createURI(URN_PRED), vf.createBNode());
		assertThat(testCon.size(), is(equalTo(2L)));
		assertThat(testCon2.size(), is(equalTo(0L)));
		testCon.commit();
		testCon2.close();
		testCon2 = testRepository.getConnection();
		assertThat(testCon.size(), is(equalTo(2L)));
		assertThat(testCon2.size(), is(equalTo(2L)));
	}

	@Test
	public void testAddRemove()
		throws Exception
	{
		URI FOAF_PERSON = vf.createURI("http://xmlns.com/foaf/0.1/Person");
		final Statement stmt = vf.createStatement(bob, name, nameBob);

		testCon.add(bob, RDF.TYPE, FOAF_PERSON);

		testCon.begin();
		testCon.add(stmt);
		testCon.remove(stmt);
		testCon.commit();

		testCon.exportStatements(null, null, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st)
				throws RDFHandlerException
			{
				assertThat(st, is(not(equalTo(stmt))));
			}
		});
	}

	@Test
	public void testInferredStatementCount()
		throws Exception
	{
		assertThat(testCon.isEmpty(), is(equalTo(true)));
		int inferred = getTotalStatementCount(testCon);

		URI root = vf.createURI("urn:root");

		testCon.add(root, RDF.TYPE, RDF.LIST);
		testCon.remove(root, RDF.TYPE, RDF.LIST);

		assertThat(testCon.isEmpty(), is(equalTo(true)));
		assertThat(getTotalStatementCount(testCon), is(equalTo(inferred)));
	}

	@Test
	public void testGetContextIDs()
		throws Exception
	{
		assertThat(Iterations.asList(testCon.getContextIDs()).size(), is(equalTo(0)));

		// load data
		testCon.add(bob, name, nameBob, context1);
		assertThat(Iterations.asList(testCon.getContextIDs()), is(equalTo(Arrays.asList((Resource)context1))));

		testCon.remove(bob, name, nameBob, context1);
		assertThat(Iterations.asList(testCon.getContextIDs()).size(), is(equalTo(0)));

		testCon.add(bob, name, nameBob, context2);
		assertThat(Iterations.asList(testCon.getContextIDs()), is(equalTo(Arrays.asList((Resource)context2))));
	}

	@Test
	public void testXmlCalendarZ()
		throws Exception
	{
		String NS = "http://example.org/rdf/";
		int OFFSET = TimeZone.getDefault().getOffset(
				new GregorianCalendar(2007 - 1900, Calendar.NOVEMBER, 6).getTimeInMillis()) / 1000 / 60;
		String SELECT_BY_DATE = "SELECT ?s ?d WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#value> ?d . FILTER (?d <= ?date) }";

		DatatypeFactory data = DatatypeFactory.newInstance();
		for (int i = 1; i < 5; i++) {
			URI uri = vf.createURI(NS, "date" + i);
			XMLGregorianCalendar xcal = data.newXMLGregorianCalendar();
			xcal.setYear(2000);
			xcal.setMonth(11);
			xcal.setDay(i * 2);
			testCon.add(uri, RDF.VALUE, vf.createLiteral(xcal));
			URI uriz = vf.createURI(NS, "dateZ" + i);
			xcal = data.newXMLGregorianCalendar();
			xcal.setYear(2007);
			xcal.setMonth(11);
			xcal.setDay(i * 2);
			xcal.setTimezone(OFFSET);
			testCon.add(uriz, RDF.VALUE, vf.createLiteral(xcal));
		}

		XMLGregorianCalendar xcal = data.newXMLGregorianCalendar();
		xcal.setYear(2007);
		xcal.setMonth(11);
		xcal.setDay(6);
		xcal.setTimezone(OFFSET);
		
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, SELECT_BY_DATE);
		query.setBinding("date", vf.createLiteral(xcal));
		TupleQueryResult result = query.evaluate();
		List<BindingSet> list = new ArrayList<BindingSet>();
		while (result.hasNext()) {
			list.add(result.next());
		}
		assertThat(list.size(), is(equalTo(7)));
	}

	@Test
	public void testOptionalFilter()
		throws Exception
	{
		String optional = "{ ?s :p1 ?v1 OPTIONAL {?s :p2 ?v2 FILTER(?v1<3) } }";
		URI s = vf.createURI("urn:test:s");
		URI p1 = vf.createURI(URN_TEST_P1);
		URI p2 = vf.createURI(URN_TEST_P2);
		Value v1 = vf.createLiteral(1);
		Value v2 = vf.createLiteral(2);
		Value v3 = vf.createLiteral(3);
		testCon.add(s, p1, v1);
		testCon.add(s, p2, v2);
		testCon.add(s, p1, v3);
		String qry = "PREFIX :<urn:test:> SELECT ?s ?v1 ?v2 WHERE " + optional;
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, qry);
		TupleQueryResult result = query.evaluate();
		Set<List<Value>> set = new HashSet<List<Value>>();
		while (result.hasNext()) {
			BindingSet bindings = result.next();
			set.add(Arrays.asList(bindings.getValue("v1"), bindings.getValue("v2")));
		}
		result.close();
		assertThat(set, hasItem(Arrays.asList(v1, v2)));
		assertThat(set, hasItem(Arrays.asList(v3, null)));
	}

	@Test
	public void testOrPredicate()
		throws Exception
	{
		String union = "{ :s ?p :o FILTER (?p = :p1 || ?p = :p2) }";
		URI s = vf.createURI("urn:test:s");
		URI p1 = vf.createURI(URN_TEST_P1);
		URI p2 = vf.createURI(URN_TEST_P2);
		URI o = vf.createURI("urn:test:o");
		testCon.add(s, p1, o);
		testCon.add(s, p2, o);
		String qry = "PREFIX :<urn:test:> SELECT ?p WHERE " + union;
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, qry);
		TupleQueryResult result = query.evaluate();
		List<Value> list = new ArrayList<Value>();
		while (result.hasNext()) {
			BindingSet bindings = result.next();
			list.add(bindings.getValue("p"));
		}
		result.close();
		assertThat(list, hasItem(p1));
		assertThat(list, hasItem(p2));
	}

	@Test
	public void testSES713()
		throws Exception
	{
		String queryString = "SELECT * { ?sub ?pred ?obj . FILTER ( 'not a number' + 1 = ?obj )}";

		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult tqr = query.evaluate();
		try {
			assertFalse("Query should not return any results", tqr.hasNext());
		}
		finally {
			tqr.close();
		}
	}

	/*
	 * Note: This test gets overridden in a GC aware manner to ensure that it
	 * passes in CI. See BigdataConnectionTest.
	 */
	@Test
	public void testOrderByQueriesAreInterruptable()
		throws Exception
	{
		testCon.begin();
		for (int index = 0; index < 512; index++) {
			testCon.add(RDFS.CLASS, RDFS.COMMENT, testCon.getValueFactory().createBNode());
		}
		testCon.commit();

		final TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL,
				"SELECT * WHERE { ?s ?p ?o . ?s1 ?p1 ?o1 . ?s2 ?p2 ?o2 . ?s3 ?p3 ?o3 } ORDER BY ?s1 ?p1 ?o1 LIMIT 1000");
		query.setMaxQueryTime(2);

		final TupleQueryResult result = query.evaluate();
		final long startTime = System.currentTimeMillis();
		try {
			result.hasNext();
			fail("Query should have been interrupted");
		}
		catch (QueryInterruptedException e) {
			// Expected
			final long duration = System.currentTimeMillis() - startTime;

			assertTrue("Query not interrupted quickly enough, should have been ~2s, but was "
					+ (duration / 1000) + "s", duration < 5000);
		}
	}

	@Test
	public void testQueryDefaultGraph()
		throws Exception
	{
		URI graph = vf.createURI("urn:test:default");
		testCon.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1));
		assertThat(size(graph), is(equalTo(0)));
		testCon.add(vf.createURI("urn:test:s2"), vf.createURI(URN_TEST_P2), vf.createURI("urn:test:o2"), graph);
		assertThat(size(graph), is(equalTo(1)));
	}

	@Test
	public void testQueryBaseURI()
		throws Exception
	{
		testCon.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1));
		TupleQueryResult rs = testCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * { <> ?p ?o }",
				URN_TEST_S1).evaluate();
		try {
			assertThat(rs.hasNext(), is(equalTo(true)));
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void testUpdateBaseURI()
		throws Exception
	{
		testCon.prepareUpdate(QueryLanguage.SPARQL, "INSERT DATA { <> a <> }", URN_TEST_S1).execute();
		assertThat(testCon.size(), is(equalTo(1L)));
	}

	@Test
	public void testDeleteDefaultGraph()
		throws Exception
	{
		URI g1 = vf.createURI("urn:test:g1");
		URI g2 = vf.createURI("urn:test:g2");
		testCon.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1), g1);
		testCon.add(vf.createURI("urn:test:s2"), vf.createURI(URN_TEST_P2), vf.createURI("urn:test:o2"), g2);
		Update up = testCon.prepareUpdate(QueryLanguage.SPARQL, SPARQL_DEL_ALL);
		DatasetImpl ds = new DatasetImpl();
		ds.addDefaultGraph(g1);
		ds.addDefaultRemoveGraph(g1);
		up.setDataset(ds);
		up.execute();
		assertThat(size(g1), is(equalTo(0)));
		assertThat(size(g2), is(equalTo(1)));
	}

	@Test
	public void testDefaultContext()
		throws Exception
	{
		ContextAwareConnection con = new ContextAwareConnection(testCon);
		URI defaultGraph = vf.createURI("urn:test:default");
		con.setReadContexts(defaultGraph);
		con.setInsertContext(defaultGraph);
		con.setRemoveContexts(defaultGraph);
		con.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1));
		con.prepareUpdate("INSERT DATA { graph <urn:test:default> { <urn:test:s2> <urn:test:p2> \"l2\" }}").execute();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(2)));
		assertThat(Iterations.asList(con.getStatements(null, null, null, defaultGraph)).size(), is(equalTo(2)));
		assertThat(size(defaultGraph), is(equalTo(2)));
		con.add(vf.createURI("urn:test:s3"), vf.createURI("urn:test:p3"), vf.createURI("urn:test:o3"),
				(Resource)null);
		con.add(vf.createURI("urn:test:s4"), vf.createURI("urn:test:p4"), vf.createURI("urn:test:o4"),
				vf.createURI(URN_TEST_OTHER));
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(3)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(4)));
		assertThat(size(defaultGraph), is(equalTo(3)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(1)));
		con.prepareUpdate(SPARQL_DEL_ALL).execute();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(0)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(1)));
		assertThat(size(defaultGraph), is(equalTo(0)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(1)));
	}

	@Test
	public void testDefaultInsertContext()
		throws Exception
	{
		ContextAwareConnection con = new ContextAwareConnection(testCon);
		URI defaultGraph = vf.createURI("urn:test:default");
		con.setInsertContext(defaultGraph);
		con.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1));
		con.prepareUpdate("INSERT DATA { graph <urn:test:default> { <urn:test:s2> <urn:test:p2> \"l2\" }}").execute();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(2)));
		assertThat(Iterations.asList(con.getStatements(null, null, null, defaultGraph)).size(), is(equalTo(2)));
		assertThat(size(defaultGraph), is(equalTo(2)));
		con.add(vf.createURI("urn:test:s3"), vf.createURI("urn:test:p3"), vf.createURI("urn:test:o3"),
				(Resource)null);
		con.add(vf.createURI("urn:test:s4"), vf.createURI("urn:test:p4"), vf.createURI("urn:test:o4"),
				vf.createURI(URN_TEST_OTHER));
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(4)));
		assertThat(Iterations.asList(con.getStatements(null, null, null, defaultGraph)).size(), is(equalTo(3)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(4)));
		assertThat(size(defaultGraph), is(equalTo(3)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(1)));
		// not working.
//		con.prepareUpdate(SPARQL_DEL_ALL).execute();
		con.clear();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(0)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(0)));
		assertThat(size(defaultGraph), is(equalTo(0)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(0)));
	}

	@Test
	public void testExclusiveNullContext()
		throws Exception
	{
		ContextAwareConnection con = new ContextAwareConnection(testCon);
		URI defaultGraph = BD.NULL_GRAPH; // null context
		con.setReadContexts(defaultGraph);
		con.setInsertContext(defaultGraph);
		con.setRemoveContexts(defaultGraph);
		con.add(vf.createURI(URN_TEST_S1), vf.createURI(URN_TEST_P1), vf.createURI(URN_TEST_O1));
		con.prepareUpdate("INSERT DATA { <urn:test:s2> <urn:test:p2> \"l2\" }").execute();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(2)));
		assertThat(Iterations.asList(con.getStatements(null, null, null, defaultGraph)).size(), is(equalTo(2)));
		assertThat(size(defaultGraph), is(equalTo(2)));
		con.add(vf.createURI("urn:test:s3"), vf.createURI("urn:test:p3"), vf.createURI("urn:test:o3"),
				(Resource)null);
		con.add(vf.createURI("urn:test:s4"), vf.createURI("urn:test:p4"), vf.createURI("urn:test:o4"),

		vf.createURI(URN_TEST_OTHER));
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(3)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(4)));
		assertThat(size(defaultGraph), is(equalTo(3)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(1)));
		con.prepareUpdate(SPARQL_DEL_ALL).execute();
		assertThat(Iterations.asList(con.getStatements(null, null, null)).size(), is(equalTo(0)));
		assertThat(Iterations.asList(testCon.getStatements(null, null, null, true)).size(), is(equalTo(1)));
		assertThat(size(defaultGraph), is(equalTo(0)));
		assertThat(size(vf.createURI(URN_TEST_OTHER)), is(equalTo(1)));
	}

	private int size(URI defaultGraph)
		throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		TupleQuery qry = testCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * { ?s ?p ?o }");
		DatasetImpl dataset = new DatasetImpl();
		dataset.addDefaultGraph(defaultGraph);
		qry.setDataset(dataset);
		TupleQueryResult result = qry.evaluate();
		try {
			int count = 0;
			while (result.hasNext()) {
				result.next();
				count++;
			}
			return count;
		}
		finally {
			result.close();
		}
	}

	private int getTotalStatementCount(RepositoryConnection connection)
		throws RepositoryException
	{
		CloseableIteration<? extends Statement, RepositoryException> iter = connection.getStatements(null,
				null, null, true);

		try {
			int size = 0;

			while (iter.hasNext()) {
				iter.next();
				++size;
			}

			return size;
		}
		finally {
			iter.close();
		}
	}
}
