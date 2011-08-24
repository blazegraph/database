/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

//import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

/**
 * A set of compliance tests on SPARQL query functionality which can not be
 * easily executed using the {@link SPARQL11ManifestTest} format. This includes
 * tests on queries with non-deterministic output (e.g. GROUP_CONCAT).
 * 
 * @author Jeen Broekstra
 * 
 *         FIXME This was imported solely to have the class extend TestCase.
 *         I've written to Jeen to request that he make this change in SVN, at
 *         which point we can drop the copy of this class from our SVN
 *         repository.
 */
public abstract class ComplexSPARQLQueryTest extends TestCase {

	static final Logger logger = LoggerFactory.getLogger(ComplexSPARQLQueryTest.class);

	private Repository rep;

	protected RepositoryConnection conn;

	protected ValueFactory f;

	protected static final String EX_NS = "http://example.org/";

	/**
	 * @throws java.lang.Exception
	 */
	//@Before
	public void setUp()
		throws Exception
	{
		logger.debug("setting up test");
		this.rep = newRepository();
		rep.initialize();

		f = rep.getValueFactory();
		conn = rep.getConnection();

		loadTestData();

		logger.debug("test setup complete.");
	}

	/**
	 * @throws java.lang.Exception
	 */
	//@After
	public void tearDown()
		throws Exception
	{
		logger.debug("tearing down...");
		conn.close();
		conn = null;

		rep.shutDown();
		rep = null;

		logger.debug("tearDown complete.");
	}

	//@Test
	public void testGroupConcatDistinct() {
		StringBuilder query = new StringBuilder();
		query.append(getNamespaceDeclarations());
		query.append("SELECT (GROUP_CONCAT(DISTINCT ?l) AS ?concat)");
		query.append("WHERE { ex:groupconcat-test ?p ?l . }");

		TupleQuery tq = null;
		try {
			tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
		}
		catch (RepositoryException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		catch (MalformedQueryException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			TupleQueryResult result = tq.evaluate();
			assertNotNull(result);

			while (result.hasNext()) {
				BindingSet bs = result.next();
				assertNotNull(bs);

				Value concat = bs.getValue("concat");

				assertTrue(concat instanceof Literal);

				String lexValue = ((Literal)concat).getLabel();

				int occ = countCharOccurrences(lexValue, 'a');
				assertEquals(1, occ);
				occ = countCharOccurrences(lexValue, 'b');
				assertEquals(1, occ);
				occ = countCharOccurrences(lexValue, 'c');
				assertEquals(1, occ);
				occ = countCharOccurrences(lexValue, 'd');
				assertEquals(1, occ);
			}
			result.close();
		}
		catch (QueryEvaluationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	//@Test
	public void testGroupConcatNonDistinct() {
		StringBuilder query = new StringBuilder();
		query.append(getNamespaceDeclarations());
		query.append("SELECT (GROUP_CONCAT(?l) AS ?concat)");
		query.append("WHERE { ex:groupconcat-test ?p ?l . }");

		TupleQuery tq = null;
		try {
			tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
		}
		catch (RepositoryException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		catch (MalformedQueryException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			TupleQueryResult result = tq.evaluate();
			assertNotNull(result);

			while (result.hasNext()) {
				BindingSet bs = result.next();
				assertNotNull(bs);

				Value concat = bs.getValue("concat");

				assertTrue(concat instanceof Literal);

				String lexValue = ((Literal)concat).getLabel();

				int occ = countCharOccurrences(lexValue, 'a');
				assertEquals(1, occ);
				occ = countCharOccurrences(lexValue, 'b');
				assertEquals(2, occ);
				occ = countCharOccurrences(lexValue, 'c');
				assertEquals(2, occ);
				occ = countCharOccurrences(lexValue, 'd');
				assertEquals(1, occ);
			}
			result.close();
		}
		catch (QueryEvaluationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	private int countCharOccurrences(String string, char ch) {
		int count = 0;
		for (int i = 0; i < string.length(); i++) {
			if (string.charAt(i) == ch) {
				count++;
			}
		}
		return count;
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
		declarations.append("\n");

		return declarations.toString();
	}

	protected abstract Repository newRepository()
		throws Exception;

	protected void loadTestData()
		throws RDFParseException, RepositoryException, IOException
	{
		logger.debug("loading dataset...");
		InputStream dataset = ComplexSPARQLQueryTest.class.getResourceAsStream("/testdata-query/dataset-query.trig");
		try {
			conn.add(dataset, "", RDFFormat.TRIG);
		}
		finally {
			dataset.close();
		}
		logger.debug("dataset loaded.");
	}
}
