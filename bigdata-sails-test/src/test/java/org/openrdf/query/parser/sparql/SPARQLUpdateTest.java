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
package org.openrdf.query.parser.sparql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

import junit.framework.TestCase;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
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

/**
 * Tests for SPARQL 1.1 Update functionality.
 * 
 * @author Jeen Broekstra
 */
public abstract class SPARQLUpdateTest extends TestCase {

	protected static final Logger logger = LoggerFactory.getLogger(SPARQLUpdateTest.class);

	private Repository rep;

	protected RepositoryConnection con;

	protected ValueFactory f;

	protected URI bob;

	protected URI alice;

	protected URI graph1;

	protected URI graph2;

	protected static final String EX_NS = "http://example.org/";

	/**
	 * @throws java.lang.Exception
	 */
	@Before
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
	@After
	public void tearDown()
		throws Exception
	{
		logger.debug("tearing down...");
		con.close();
		con = null;

		rep.shutDown();
		rep = null;

		logger.debug("tearDown complete.");
	}

	/* test methods */

	@Test
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

	@Test
	public void testInsertWhereWithBinding()
		throws Exception
	{
		logger.debug("executing test testInsertWhereWithBinding");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		operation.setBinding("x", bob);

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}

	@Test
	public void testInsertWhereWithBindings2()
		throws Exception
	{
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

	@Test
	public void testInsertEmptyWhere()
		throws Exception
	{
		logger.debug("executing test testInsertEmptyWhere");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT { <" + bob + "> rdfs:label \"Bob\" . } WHERE { }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
	}

	@Test
	public void testInsertEmptyWhereWithBinding()
		throws Exception
	{
		logger.debug("executing test testInsertEmptyWhereWithBinding");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT {?x rdfs:label ?y . } WHERE { }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		operation.setBinding("x", bob);
		operation.setBinding("y", f.createLiteral("Bob"));

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
	}

	@Test
	public void testInsertNonMatchingWhere()
		throws Exception
	{
		logger.debug("executing test testInsertNonMatchingWhere");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));

		operation.execute();

		assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
	}

	@Test
	public void testInsertNonMatchingWhereWithBindings()
		throws Exception
	{
		logger.debug("executing test testInsertNonMatchingWhereWithBindings");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT { ?x rdfs:label ?y . } WHERE { ?x rdfs:comment ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		operation.setBinding("x", bob);
		operation.setBinding("y", f.createLiteral("Bob"));

		assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));

		operation.execute();

		assertFalse(con.hasStatement(bob, RDFS.LABEL, null, true));
	}

	@Test
	public void testInsertWhereWithBindings()
		throws Exception
	{
		logger.debug("executing test testInsertWhereWithBindings");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT { ?x rdfs:comment ?z . } WHERE { ?x foaf:name ?y }");

		Literal comment = f.createLiteral("Bob has a comment");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		operation.setBinding("x", bob);
		operation.setBinding("z", comment);

		assertFalse(con.hasStatement(null, RDFS.COMMENT, comment, true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.COMMENT, comment, true));
		assertFalse(con.hasStatement(alice, RDFS.COMMENT, comment, true));

	}

	@Test
	public void testInsertWhereWithOptional()
		throws Exception
	{
		logger.debug("executing testInsertWhereWithOptional");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append(" INSERT { ?s ex:age ?incAge } ");
		// update.append(" DELETE { ?s ex:age ?age } ");
		update.append(" WHERE { ?s foaf:name ?name . ");
		update.append(" OPTIONAL {?s ex:age ?age . BIND ((?age + 1) as ?incAge)  } ");
		update.append(" } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI age = f.createURI(EX_NS, "age");

		assertFalse(con.hasStatement(alice, age, null, true));
		assertTrue(con.hasStatement(bob, age, null, true));

		operation.execute();

		RepositoryResult<Statement> result = con.getStatements(bob, age, null, true);

		while (result.hasNext()) {
			System.out.println(result.next().toString());
		}

		assertTrue(con.hasStatement(bob, age, f.createLiteral("43", XMLSchema.INTEGER), true));

		result = con.getStatements(alice, age, null, true);

		while (result.hasNext()) {
			System.out.println(result.next());
		}
		assertFalse(con.hasStatement(alice, age, null, true));
	}

	@Test
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

	@Test
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

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

	}

	@Test
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
		assertTrue(con.hasStatement(bob, FOAF.MBOX, mboxBob, true));
		assertTrue(con.hasStatement(alice, FOAF.MBOX, mboxAlice, true));

		assertTrue(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertTrue(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

		operation.execute();

		assertFalse(con.hasStatement(bob, FOAF.MBOX, mboxBob, true));
		assertTrue(con.hasStatement(alice, FOAF.MBOX, mboxAlice, true));

		assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

	}

	@Test
	public void testDeleteInsertWhereWithBindings()
		throws Exception
	{
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

	@Test
	public void testDeleteInsertWhereWithBindings2()
		throws Exception
	{
		logger.debug("executing test testDeleteInsertWhereWithBindings2");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?z . } WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.setBinding("z", f.createLiteral("person"));

		assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

		operation.execute();

		assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("person"), true));
		assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("person"), true));

		assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
		assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
	}

	@Test
	public void testDeleteInsertWhereLoopingBehavior()
		throws Exception
	{
		logger.debug("executing test testDeleteInsertWhereLoopingBehavior");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append(" DELETE { ?x ex:age ?y } INSERT {?x ex:age ?z }");
		update.append(" WHERE { ");
		update.append("   ?x ex:age ?y .");
		update.append("   BIND((?y + 1) as ?z) ");
		update.append("   FILTER( ?y < 46 ) ");
		update.append(" } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI age = f.createURI(EX_NS, "age");
		Literal originalAgeValue = f.createLiteral("42", XMLSchema.INTEGER);
		Literal correctAgeValue = f.createLiteral("43", XMLSchema.INTEGER);
		Literal inCorrectAgeValue = f.createLiteral("46", XMLSchema.INTEGER);

		assertTrue(con.hasStatement(bob, age, originalAgeValue, true));

		operation.execute();

		assertFalse(con.hasStatement(bob, age, originalAgeValue, true));
		assertTrue(con.hasStatement(bob, age, correctAgeValue, true));
		assertFalse(con.hasStatement(bob, age, inCorrectAgeValue, true));
	}

	@Test
	public void testAutoCommitHandling()
		throws Exception
	{
		logger.debug("executing test testAutoCommitHandling");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE { ?x foaf:name ?y } INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }");

		try {
			con.begin();
			Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

			assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
			assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

			operation.execute();

			// update should be visible to own connection.
			assertTrue(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
			assertTrue(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

			assertFalse(con.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
			assertFalse(con.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

			RepositoryConnection con2 = rep.getConnection();
			try {
				// update should not yet be visible to separate connection
				assertFalse(con2.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
				assertFalse(con2.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

				assertTrue(con2.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
				assertTrue(con2.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));

				con.commit();

				// after commit, update should be visible to separate connection.
				assertTrue(con2.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
				assertTrue(con2.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));

				assertFalse(con2.hasStatement(bob, FOAF.NAME, f.createLiteral("Bob"), true));
				assertFalse(con2.hasStatement(alice, FOAF.NAME, f.createLiteral("Alice"), true));
			}
			finally {
				con2.close();
			}
		}
		catch (Exception e) {
			if (con.isActive()) {
				con.rollback();
			}
		}
		finally {
			con.close();
		}
	}

	@Test
	public void testConsecutiveUpdatesInSameTransaction()
		throws Exception
	{
		// this tests if consecutive updates in the same transaction behave
		// correctly. See issue SES-930
		logger.debug("executing test testConsecutiveUpdatesInSameTransaction");

		StringBuilder update1 = new StringBuilder();
		update1.append(getNamespaceDeclarations());
		update1.append("DELETE { ?x foaf:name ?y } WHERE {?x foaf:name ?y }");

		try {
			con.begin();
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

			con.commit();

			// update should not have resulted in any inserts: where clause is
			// empty.
			assertFalse(con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
			assertFalse(con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
		}
		catch (Exception e) {
			if (con.isActive()) {
				con.rollback();
			}
		}
	}

	@Test
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

	@Test
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

	@Test
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
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}
	
	@Test
	@Ignore("Fails due to WITH graph is used to evaluate WHERE clause instead of USING graph")
	// It is a new test added during update to Sesame 2.8, but fail is not introduced with the Sesame update,
	// it was rather not supported functionality already. Should be fixed as a separate commit.
	public void _testInsertWhereUsingWith()
		throws Exception
	{

		logger.debug("executing testInsertWhereUsingWith");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("WITH ex:graph2 INSERT {?x rdfs:label ?y . } USING ex:graph1 WHERE {?x foaf:name ?y }");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		operation.execute();
		con.commit();
		
		System.out.println(
				((BigdataSailRepositoryConnection)con).getTripleStore().dumpStore()
				);
//				.getSail().getIndexManager())
//		.getJournal(ITx.UNISOLATED).get..getIn.dump();
		String message = "label should have been inserted in graph2, for ex:bob only";
		assertTrue(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob", XSD.STRING), true, graph2));
		assertFalse(message, con.hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true, graph1));
		assertFalse(message, con.hasStatement(alice, RDFS.LABEL, f.createLiteral("Alice"), true));
	}

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
	public void testInsertData2()
		throws Exception
	{
		logger.debug("executing testInsertData2");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { ex:book1 dc:title \"the number four\"^^<http://www.w3.org/2001/XMLSchema#integer> . }; ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI book1 = f.createURI(EX_NS, "book1");

		assertFalse(con.hasStatement(book1, DC.TITLE, f.createLiteral("the number four", XMLSchema.INTEGER),
				true));

		operation.execute();

		String msg = "new statement about ex:book1 should have been inserted";

		assertTrue(msg,
				con.hasStatement(book1, DC.TITLE, f.createLiteral("the number four", XMLSchema.INTEGER), true));
	}

	@Test
	public void testInsertDataLangTaggedLiteral()
		throws Exception
	{
		logger.debug("executing testInsertDataLangTaggedLiteral");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { ex:book1 dc:title \"book 1\"@en . } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI book1 = f.createURI(EX_NS, "book1");

		assertFalse(con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1", "en"), true));

		operation.execute();

		String msg = "new statement about ex:book1 should have been inserted";
		assertTrue(msg, con.hasStatement(book1, DC.TITLE, f.createLiteral("book 1", "en"), true));
	}

	@Test
	public void testInsertDataGraph1()
		throws Exception
	{
		logger.debug("executing testInsertDataGraph1");

		StringBuilder update = new StringBuilder();
		update.append("INSERT DATA { \n");
		update.append("GRAPH <urn:g1> { <urn:s1> <urn:p1> <urn:o1> . } \n");
		update.append("<urn:s1> a <urn:C1> . \n");
		update.append("}");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		assertFalse(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertFalse(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));
		operation.execute();
		assertTrue(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertTrue(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));
	}

	@Test
	public void testInsertDataGraph2()
		throws Exception
	{
		logger.debug("executing testInsertDataGraph2");

		StringBuilder update = new StringBuilder();
		update.append("INSERT DATA { \n");
		update.append("<urn:s1> a <urn:C1> . \n");
		update.append("GRAPH <urn:g1> { <urn:s1> <urn:p1> <urn:o1> . } \n");
		update.append("}");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		assertFalse(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertFalse(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));
		operation.execute();
		assertTrue(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertTrue(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));
	}

	@Test
	public void testInsertDataGraph3()
		throws Exception
	{
		logger.debug("executing testInsertDataGraph3");

		StringBuilder update = new StringBuilder();
		update.append("INSERT DATA { \n");
		update.append("<urn:s1> a <urn:C1> . \n");
		update.append("GRAPH <urn:g1>{ <urn:s1> <urn:p1> <urn:o1> . <urn:s2> <urn:p2> <urn:o2> } \n");
		update.append("<urn:s2> a <urn:C2> \n");
		update.append("}");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());
		assertFalse(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertFalse(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));

		assertFalse(con.hasStatement(f.createURI("urn:s2"), f.createURI("urn:p2"), f.createURI("urn:o2"), true,
				f.createURI("urn:g1")));
		operation.execute();
		assertTrue(con.hasStatement(f.createURI("urn:s1"), RDF.TYPE, null, true, (Resource)null));
		assertTrue(con.hasStatement(f.createURI("urn:s2"), RDF.TYPE, null, true, (Resource)null));
		assertTrue(con.hasStatement(f.createURI("urn:s1"), f.createURI("urn:p1"), f.createURI("urn:o1"), true,
				f.createURI("urn:g1")));
		assertTrue(con.hasStatement(f.createURI("urn:s2"), f.createURI("urn:p2"), f.createURI("urn:o2"), true,
				f.createURI("urn:g1")));
	}

	@Test
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

		RepositoryResult<Statement> titleStatements = con.getStatements(null, DC.TITLE,
				f.createLiteral("book 1"), true);
		assertNotNull(titleStatements);

		RepositoryResult<Statement> creatorStatements = con.getStatements(null, DC.CREATOR,
				f.createLiteral("Ringo"), true);
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
			assertFalse(creatorStatements.hasNext());

			Resource subject = cs.getSubject();
			assertTrue(subject instanceof BNode);
			assertEquals(bookNode, subject);
		}
		else {
			fail("at least one creator statement expected");
		}
		creatorStatements.close();
	}

	@Test
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

	@Test
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

	@Test
	public void testInsertDataInGraph2()
		throws Exception
	{
		logger.debug("executing testInsertDataInGraph2");

		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("INSERT DATA { GRAPH ex:graph1 { ex:Human rdfs:subClassOf ex:Mammal. ex:Mammal rdfs:subClassOf ex:Animal. ex:george a ex:Human. ex:ringo a ex:Human. } } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		URI human = f.createURI(EX_NS, "Human");
		URI mammal = f.createURI(EX_NS, "Mammal");
		URI george = f.createURI(EX_NS, "george");

		operation.execute();

		assertTrue(con.hasStatement(human, RDFS.SUBCLASSOF, mammal, true, graph1));
		assertTrue(con.hasStatement(mammal, RDFS.SUBCLASSOF, null, true, graph1));
		assertTrue(con.hasStatement(george, RDF.TYPE, human, true, graph1));
	}

	@Test
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

	@Test
	public void testDeleteDataUnicode()
		throws Exception
	{
		URI i18n = con.getValueFactory().createURI(EX_NS, "東京");
		
		con.add(i18n, FOAF.KNOWS, bob);
		
		logger.debug("executing testDeleteData");
		StringBuilder update = new StringBuilder();
		update.append(getNamespaceDeclarations());
		update.append("DELETE DATA { ex:東京 foaf:knows ex:bob. } ");

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

		assertTrue(con.hasStatement(i18n, FOAF.KNOWS, bob, true));
		operation.execute();

		String msg = "statement should have been deleted.";
		assertFalse(msg, con.hasStatement(i18n, FOAF.KNOWS, bob, true));
	}
	
	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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
			if (con.isActive()) {
				con.rollback();
			}
		}
	}

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
	public void testClearAll()
		throws Exception
	{
		logger.debug("executing testClearAll");
		String update = "CLEAR ALL";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false));

	}

	@Test
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
		assertTrue(con.hasStatement(null, null, null, false, new Resource[] { null }));

		operation.execute();
		assertTrue(con.hasStatement(null, null, null, false, graph1));
		assertTrue(con.hasStatement(null, null, null, false, graph2));
		// Verify that no statements remain in the 'default' graph.
		assertFalse(con.hasStatement(null, null, null, false, new Resource[] { null }));
	}

	@Test
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

	@Test
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

	@Test
	public void testDropAll()
		throws Exception
	{
		logger.debug("executing testDropAll");
		String update = "DROP ALL";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertFalse(con.hasStatement(null, null, null, false));

	}

	@Test
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
		assertTrue(con.hasStatement(null, null, null, false, new Resource[] { null }));

		operation.execute();
		assertTrue(con.hasStatement(null, null, null, false, graph1));
		assertTrue(con.hasStatement(null, null, null, false, graph2));
		// Verify that no statements remain in the 'default' graph.
		assertFalse(con.hasStatement(null, null, null, false, new Resource[] { null }));

	}

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
	public void testUpdateSequenceInsertDeleteExample9()
		throws Exception
	{
		logger.debug("executing testUpdateSequenceInsertDeleteExample9");

		// replace the standard dataset with one specific to this case.
		con.clear();
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
	@Test
	public void testLoad()
		throws Exception
	{
		String update = "LOAD <http://www.daml.org/2001/01/gedcom/royal92.daml>";

		String ns = "http://www.daml.org/2001/01/gedcom/gedcom#";

		Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update);

		operation.execute();
		assertTrue(con.hasStatement(null, RDF.TYPE, f.createURI(ns, "Family"), true));
	}

	@Test
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
		declarations.append("PREFIX xsd: <" + XMLSchema.NAMESPACE + "> \n");
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
}
