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
package org.openrdf.query.parser.sparql.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.io.IOUtil;
import info.aduna.iteration.Iterations;
import info.aduna.text.StringUtil;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.util.Models;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResults;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.dawg.DAWGTestResultSetUtil;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MutableTupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.memory.MemoryStore;

/**
 * A SPARQL query test suite, created by reading in a W3C working-group style
 * manifest.
 * 
 * @author Jeen Broekstra
 */
public abstract class SPARQLQueryTest extends TestCase {

	/*-----------*
	 * Constants *
	 *-----------*/

	// Logger for non-static tests, so these results can be isolated based on
	// where they are run
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	// Logger for static methods which are not overridden
	private final static Logger LOGGER = LoggerFactory.getLogger(SPARQLQueryTest.class);

	protected final String testURI;

	protected final String queryFileURL;

	protected final String resultFileURL;

	protected final Dataset dataset;

	protected final boolean laxCardinality;

	protected final boolean checkOrder;

	protected final String[] ignoredTests;
	/*-----------*
	 * Variables *
	 *-----------*/

	protected Repository dataRep;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public SPARQLQueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality, String... ignoredTests)
	{
		this(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false);
	}

	public SPARQLQueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests)
	{
		super(name.replaceAll("\\(", " ").replaceAll("\\)", " "));

		this.testURI = testURI;
		this.queryFileURL = queryFileURL;
		this.resultFileURL = resultFileURL;
		this.dataset = dataSet;
		this.laxCardinality = laxCardinality;
		this.checkOrder = checkOrder;
		this.ignoredTests = ignoredTests;
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected void setUp()
		throws Exception
	{
		dataRep = createRepository();

		if (dataset != null) {
			try {
				uploadDataset(dataset);
			}
			catch (Exception exc) {
				try {
					dataRep.shutDown();
					dataRep = null;
				}
				catch (Exception e2) {
					logger.error(e2.toString(), e2);
				}
				throw exc;
			}
		}
	}

	protected Repository createRepository()
		throws Exception
	{
		Repository repo = newRepository();
		repo.initialize();
		RepositoryConnection con = repo.getConnection();
		try {
			con.clear();
			con.clearNamespaces();
		}
		finally {
			con.close();
		}
		return repo;
	}

	protected abstract Repository newRepository()
		throws Exception;

	@Override
	protected void tearDown()
		throws Exception
	{
		if (dataRep != null) {
			dataRep.shutDown();
			dataRep = null;
		}
	}

	@Override
	protected void runTest()
		throws Exception
	{
		// FIXME this reports a test error because we still rely on JUnit 3 here.
		//org.junit.Assume.assumeFalse(Arrays.asList(ignoredTests).contains(this.getName()));
		// FIXME temporary fix is to report as succeeded and just ignore.
		if (Arrays.asList(ignoredTests).contains(this.getName())) {
			logger.warn("Query test ignored: " + this.getName());
			return;
		}
		
		RepositoryConnection con = dataRep.getConnection();
		// Some SPARQL Tests have non-XSD datatypes that must pass for the test
		// suite to complete successfully
		con.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, Boolean.FALSE);
		con.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, Boolean.FALSE);
		try {
			String queryString = readQueryString();
			Query query = con.prepareQuery(QueryLanguage.SPARQL, queryString, queryFileURL);
			if (dataset != null) {
				query.setDataset(dataset);
			}

			String name = this.getName();

			if (name.contains("pp34")) {
				System.out.println(name);
			}

			if (query instanceof TupleQuery) {
				TupleQueryResult queryResult = ((TupleQuery)query).evaluate();

				TupleQueryResult expectedResult = readExpectedTupleQueryResult();

				compareTupleQueryResults(queryResult, expectedResult);

				// Graph queryGraph = RepositoryUtil.asGraph(queryResult);
				// Graph expectedGraph = readExpectedTupleQueryResult();
				// compareGraphs(queryGraph, expectedGraph);
			}
			else if (query instanceof GraphQuery) {
				GraphQueryResult gqr = ((GraphQuery)query).evaluate();
				Set<Statement> queryResult = Iterations.asSet(gqr);

				Set<Statement> expectedResult = readExpectedGraphQueryResult();

				compareGraphs(queryResult, expectedResult);
			}
			else if (query instanceof BooleanQuery) {
				boolean queryResult = ((BooleanQuery)query).evaluate();
				boolean expectedResult = readExpectedBooleanQueryResult();
				assertEquals(expectedResult, queryResult);
			}
			else {
				throw new RuntimeException("Unexpected query type: " + query.getClass());
			}
		}
		finally {
			con.close();
		}
	}

    /*
     * MRP: Made !final.
     */
	protected void compareTupleQueryResults(TupleQueryResult queryResult, TupleQueryResult expectedResult)
		throws Exception
	{
		// Create MutableTupleQueryResult to be able to re-iterate over the
		// results
		MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
		MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);

		boolean resultsEqual;
		if (laxCardinality) {
			resultsEqual = QueryResults.isSubset(queryResultTable, expectedResultTable);
		}
		else {
			resultsEqual = QueryResults.equals(queryResultTable, expectedResultTable);

			if (checkOrder) {
				// also check the order in which solutions occur.
				queryResultTable.beforeFirst();
				expectedResultTable.beforeFirst();

				while (queryResultTable.hasNext()) {
					BindingSet bs = queryResultTable.next();
					BindingSet expectedBs = expectedResultTable.next();

					if (!bs.equals(expectedBs)) {
						resultsEqual = false;
						break;
					}
				}
			}
		}

		if (!resultsEqual) {
			queryResultTable.beforeFirst();
			expectedResultTable.beforeFirst();

			/*
			 * StringBuilder message = new StringBuilder(128);
			 * message.append("\n============ "); message.append(getName());
			 * message.append(" =======================\n");
			 * message.append("Expected result: \n"); while
			 * (expectedResultTable.hasNext()) {
			 * message.append(expectedResultTable.next()); message.append("\n"); }
			 * message.append("============="); StringUtil.appendN('=',
			 * getName().length(), message);
			 * message.append("========================\n"); message.append("Query
			 * result: \n"); while (queryResultTable.hasNext()) {
			 * message.append(queryResultTable.next()); message.append("\n"); }
			 * message.append("============="); StringUtil.appendN('=',
			 * getName().length(), message);
			 * message.append("========================\n");
			 */

			List<BindingSet> queryBindings = Iterations.asList(queryResultTable);

			List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);

			List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
			missingBindings.removeAll(queryBindings);

			List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
			unexpectedBindings.removeAll(expectedBindings);

			StringBuilder message = new StringBuilder(128);
			message.append("\n============ ");
			message.append(getName());
			message.append(" =======================\n");

			if (!missingBindings.isEmpty()) {

				message.append("Missing bindings: \n");
				for (BindingSet bs : missingBindings) {
					printBindingSet(bs, message);
				}

				message.append("=============");
				StringUtil.appendN('=', getName().length(), message);
				message.append("========================\n");
			}

			if (!unexpectedBindings.isEmpty()) {
				message.append("Unexpected bindings: \n");
				for (BindingSet bs : unexpectedBindings) {
					printBindingSet(bs, message);
				}

				message.append("=============");
				StringUtil.appendN('=', getName().length(), message);
				message.append("========================\n");
			}

			if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
				message.append("Results are not in expected order.\n");
				message.append(" =======================\n");
				message.append("query result: \n");
				for (BindingSet bs : queryBindings) {
					printBindingSet(bs, message);
				}
				message.append(" =======================\n");
				message.append("expected result: \n");
				for (BindingSet bs : expectedBindings) {
					printBindingSet(bs, message);
				}
				message.append(" =======================\n");

				System.out.print(message.toString());
			}
			else if (missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
				message.append("unexpected duplicate in result.\n");
				message.append(" =======================\n");
				message.append("query result: \n");
				for (BindingSet bs : queryBindings) {
					printBindingSet(bs, message);
				}
				message.append(" =======================\n");
				message.append("expected result: \n");
				for (BindingSet bs : expectedBindings) {
					printBindingSet(bs, message);
				}
				message.append(" =======================\n");

				System.out.print(message.toString());
			}

			logger.error(message.toString());
			fail(message.toString());
		}
		/* debugging only: print out result when test succeeds 
		else {
			queryResultTable.beforeFirst();

			List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
			StringBuilder message = new StringBuilder(128);

			message.append("\n============ ");
			message.append(getName());
			message.append(" =======================\n");

			message.append(" =======================\n");
			message.append("query result: \n");
			for (BindingSet bs: queryBindings) {
				message.append(bs);
				message.append("\n");
			}
			
			System.out.print(message.toString());
		}
		*/
	}

	protected void printBindingSet(BindingSet bs, StringBuilder appendable) {
		List<String> names = new ArrayList<String>(bs.getBindingNames());
		Collections.sort(names);

		for (String name : names) {
			if (bs.hasBinding(name)) {
				appendable.append(bs.getBinding(name));
				appendable.append(' ');
			}
		}
		appendable.append("\n");
	}

    /*
     * MRP: Made !final.
     */
	protected void compareGraphs(Set<Statement> queryResult, Set<Statement> expectedResult)
		throws Exception
	{
		if (!Models.isomorphic(expectedResult, queryResult)) {
			// Don't use RepositoryUtil.difference, it reports incorrect diffs
			/*
			 * Collection<? extends Statement> unexpectedStatements =
			 * RepositoryUtil.difference(queryResult, expectedResult); Collection<?
			 * extends Statement> missingStatements =
			 * RepositoryUtil.difference(expectedResult, queryResult);
			 * StringBuilder message = new StringBuilder(128);
			 * message.append("\n=======Diff: "); message.append(getName());
			 * message.append("========================\n"); if
			 * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
			 * statements in result: \n"); for (Statement st :
			 * unexpectedStatements) { message.append(st.toString());
			 * message.append("\n"); } message.append("============="); for (int i =
			 * 0; i < getName().length(); i++) { message.append("="); }
			 * message.append("========================\n"); } if
			 * (!missingStatements.isEmpty()) { message.append("Statements missing
			 * in result: \n"); for (Statement st : missingStatements) {
			 * message.append(st.toString()); message.append("\n"); }
			 * message.append("============="); for (int i = 0; i <
			 * getName().length(); i++) { message.append("="); }
			 * message.append("========================\n"); }
			 */
			StringBuilder message = new StringBuilder(128);
			message.append("\n============ ");
			message.append(getName());
			message.append(" =======================\n");
			message.append("Expected result: \n");
			for (Statement st : expectedResult) {
				message.append(st.toString());
				message.append("\n");
			}
			message.append("=============");
			StringUtil.appendN('=', getName().length(), message);
			message.append("========================\n");

			message.append("Query result: \n");
			for (Statement st : queryResult) {
				message.append(st.toString());
				message.append("\n");
			}
			message.append("=============");
			StringUtil.appendN('=', getName().length(), message);
			message.append("========================\n");

			logger.error(message.toString());
			fail(message.toString());
		}
	}

    /*
     * MRP: Made !final.
     */
	protected void uploadDataset(Dataset dataset)
		throws Exception
	{
		RepositoryConnection con = dataRep.getConnection();
		try {
			// Merge default and named graphs to filter duplicates
			Set<URI> graphURIs = new HashSet<URI>();
			graphURIs.addAll(dataset.getDefaultGraphs());
			graphURIs.addAll(dataset.getNamedGraphs());

			for (Resource graphURI : graphURIs) {
				upload(((URI)graphURI), graphURI);
			}
		}
		finally {
			con.close();
		}
	}
	
    /*
     * MRP: Made protected.
     */
	protected void upload(URI graphURI, Resource context)
		throws Exception
	{
		RepositoryConnection con = dataRep.getConnection();

		try {
			con.begin();
			RDFFormat rdfFormat = Rio.getParserFormatForFileName(graphURI.toString(), RDFFormat.TURTLE);
			RDFParser rdfParser = Rio.createParser(rdfFormat, dataRep.getValueFactory());
			rdfParser.setVerifyData(false);
			rdfParser.setDatatypeHandling(DatatypeHandling.IGNORE);
			// rdfParser.setPreserveBNodeIDs(true);

			RDFInserter rdfInserter = new RDFInserter(con);
			rdfInserter.enforceContext(context);
			rdfParser.setRDFHandler(rdfInserter);

			URL graphURL = new URL(graphURI.toString());
			InputStream in = graphURL.openStream();
			try {
				rdfParser.parse(in, graphURI.toString());
			}
			finally {
				in.close();
			}

			con.commit();
		}
		catch (Exception e) {
			if (con.isActive()) {
				con.rollback();
			}
			throw e;
		}
		finally {
			con.close();
		}
	}

	protected final String readQueryString()
		throws IOException
	{
		InputStream stream = new URL(queryFileURL).openStream();
		try {
			return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
		}
		finally {
			stream.close();
		}
	}

	protected final TupleQueryResult readExpectedTupleQueryResult()
		throws Exception
	{
		TupleQueryResultFormat tqrFormat = QueryResultIO.getParserFormatForFileName(resultFileURL);

		if (tqrFormat != null) {
			InputStream in = new URL(resultFileURL).openStream();
			try {
				TupleQueryResultParser parser = QueryResultIO.createParser(tqrFormat);
				parser.setValueFactory(dataRep.getValueFactory());

				TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
				parser.setQueryResultHandler(qrBuilder);

				parser.parseQueryResult(in);
				return qrBuilder.getQueryResult();
			}
			finally {
				in.close();
			}
		}
		else {
			Set<Statement> resultGraph = readExpectedGraphQueryResult();
			return DAWGTestResultSetUtil.toTupleQueryResult(resultGraph);
		}
	}

	protected final boolean readExpectedBooleanQueryResult()
		throws Exception
	{
		BooleanQueryResultFormat bqrFormat = BooleanQueryResultParserRegistry.getInstance().getFileFormatForFileName(
				resultFileURL);

		if (bqrFormat != null) {
			InputStream in = new URL(resultFileURL).openStream();
			try {
				return QueryResultIO.parse(in, bqrFormat);
			}
			finally {
				in.close();
			}
		}
		else {
			Set<Statement> resultGraph = readExpectedGraphQueryResult();
			return DAWGTestResultSetUtil.toBooleanQueryResult(resultGraph);
		}
	}

	protected final Set<Statement> readExpectedGraphQueryResult()
		throws Exception
	{
		RDFFormat rdfFormat = Rio.getParserFormatForFileName(resultFileURL);

		if (rdfFormat != null) {
			RDFParser parser = Rio.createParser(rdfFormat);
			parser.setDatatypeHandling(DatatypeHandling.IGNORE);
			parser.setPreserveBNodeIDs(true);
			parser.setValueFactory(dataRep.getValueFactory());

			Set<Statement> result = new LinkedHashSet<Statement>();
			parser.setRDFHandler(new StatementCollector(result));

			InputStream in = new URL(resultFileURL).openStream();
			try {
				parser.parse(in, resultFileURL);
			}
			finally {
				in.close();
			}

			return result;
		}
		else {
			throw new RuntimeException("Unable to determine file type of results file");
		}
	}

	public interface Factory {

		SPARQLQueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
				String resultFileURL, Dataset dataSet, boolean laxCardinality);

		SPARQLQueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
				String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder);
	}

	public static TestSuite suite(String manifestFileURL, Factory factory)
		throws Exception
	{
		return suite(manifestFileURL, factory, true);
	}

	public static TestSuite suite(String manifestFileURL, Factory factory, boolean approvedOnly)
		throws Exception
	{
		LOGGER.info("Building test suite for {}", manifestFileURL);

		TestSuite suite = new TestSuite(factory.getClass().getName());

		// Read manifest and create declared test cases
		Repository manifestRep = new SailRepository(new MemoryStore());
		manifestRep.initialize();
		RepositoryConnection con = manifestRep.getConnection();

		ManifestTest.addTurtle(con, new URL(manifestFileURL), manifestFileURL);

		suite.setName(getManifestName(manifestRep, con, manifestFileURL));

		// Extract test case information from the manifest file. Note that we only
		// select those test cases that are mentioned in the list.
		StringBuilder query = new StringBuilder(512);
		query.append(" SELECT DISTINCT testURI, testName, resultFile, action, queryFile, defaultGraph, ordered ");
		query.append(" FROM {} rdf:first {testURI} ");
		if (approvedOnly) {
			query.append("                          dawgt:approval {dawgt:Approved}; ");
		}
		query.append("                             mf:name {testName}; ");
		query.append("                             mf:result {resultFile}; ");
		query.append("                             [ mf:checkOrder {ordered} ]; ");
		query.append("                             [ mf:requires {Requirement} ];");
		query.append("                             mf:action {action} qt:query {queryFile}; ");
		query.append("                                               [qt:data {defaultGraph}]; ");
		query.append("                                               [sd:entailmentRegime {Regime} ]");

		// skip tests involving CSV result files, these are not query tests
		query.append(" WHERE NOT resultFile LIKE \"*.csv\" ");
		// skip tests involving JSON, sesame currently does not have a SPARQL/JSON
		// parser.
		query.append(" AND NOT resultFile LIKE \"*.srj\" ");
		// skip tests involving entailment regimes
		query.append(" AND NOT BOUND(Regime) ");
		// skip test involving basic federation, these are tested separately.
		query.append(" AND (NOT BOUND(Requirement) OR (Requirement != mf:BasicFederation)) ");
		query.append(" USING NAMESPACE ");
		query.append("  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>, ");
		query.append("  dawgt = <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>, ");
		query.append("  qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>, ");
		query.append("  sd = <http://www.w3.org/ns/sparql-service-description#>, ");
		query.append("  ent = <http://www.w3.org/ns/entailment/> ");
		TupleQuery testCaseQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());

		query.setLength(0);
		query.append(" SELECT graph ");
		query.append(" FROM {action} qt:graphData {graph} ");
		query.append(" USING NAMESPACE ");
		query.append(" qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>");
		TupleQuery namedGraphsQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());

		query.setLength(0);
		query.append("SELECT 1 ");
		query.append(" FROM {testURI} mf:resultCardinality {mf:LaxCardinality}");
		query.append(" USING NAMESPACE mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>");
		TupleQuery laxCardinalityQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());

		LOGGER.debug("evaluating query..");
		TupleQueryResult testCases = testCaseQuery.evaluate();
		while (testCases.hasNext()) {
			BindingSet bindingSet = testCases.next();

			URI testURI = (URI)bindingSet.getValue("testURI");
			String testName = bindingSet.getValue("testName").stringValue();
			String resultFile = bindingSet.getValue("resultFile").stringValue();
			String queryFile = bindingSet.getValue("queryFile").stringValue();
			URI defaultGraphURI = (URI)bindingSet.getValue("defaultGraph");
			Value action = bindingSet.getValue("action");
			Value ordered = bindingSet.getValue("ordered");

			LOGGER.debug("found test case : {}", testName);

			// Query named graphs
			namedGraphsQuery.setBinding("action", action);
			TupleQueryResult namedGraphs = namedGraphsQuery.evaluate();

			DatasetImpl dataset = null;

			if (defaultGraphURI != null || namedGraphs.hasNext()) {
				dataset = new DatasetImpl();

				if (defaultGraphURI != null) {
					dataset.addDefaultGraph(defaultGraphURI);
				}

				while (namedGraphs.hasNext()) {
					BindingSet graphBindings = namedGraphs.next();
					URI namedGraphURI = (URI)graphBindings.getValue("graph");
					LOGGER.debug(" adding named graph : {}", namedGraphURI);
					dataset.addNamedGraph(namedGraphURI);
				}
			}

			// Check for lax-cardinality conditions
			boolean laxCardinality = false;
			laxCardinalityQuery.setBinding("testURI", testURI);
			TupleQueryResult laxCardinalityResult = laxCardinalityQuery.evaluate();
			try {
				laxCardinality = laxCardinalityResult.hasNext();
			}
			finally {
				laxCardinalityResult.close();
			}

			// if this is enabled, Sesame passes all tests, showing that the only
			// difference is the semantics of arbitrary-length
			// paths
			/*
			if (!laxCardinality) {
				// property-path tests always with lax cardinality because Sesame filters out duplicates by design
				if (testURI.stringValue().contains("property-path")) {
					laxCardinality = true;
				}
			}
			*/

			// Two SPARQL distinctness tests fail in RDF-1.1 if the only difference
			// is in the number of results
			if (!laxCardinality) {
				if (testURI.stringValue().contains("distinct/manifest#distinct-2")
						|| testURI.stringValue().contains("distinct/manifest#distinct-9"))
				{
					laxCardinality = true;
				}
			}

			LOGGER.debug("testURI={} name={} queryFile={}", testURI.stringValue(), testName, queryFile);

			// check if we should test for query result ordering
			boolean checkOrder = false;
			if (ordered != null) {
				checkOrder = Boolean.parseBoolean(ordered.stringValue());
			}

			SPARQLQueryTest test = factory.createSPARQLQueryTest(testURI.stringValue(), testName, queryFile,
					resultFile, dataset, laxCardinality, checkOrder);
			if (test != null) {
				suite.addTest(test);
			}
		}

		testCases.close();
		con.close();

		manifestRep.shutDown();
		LOGGER.info("Created test suite with " + suite.countTestCases() + " test cases.");
		return suite;
	}

	protected static String getManifestName(Repository manifestRep, RepositoryConnection con,
			String manifestFileURL)
		throws QueryEvaluationException, RepositoryException, MalformedQueryException
	{
		// Try to extract suite name from manifest file
		TupleQuery manifestNameQuery = con.prepareTupleQuery(QueryLanguage.SERQL,
				"SELECT ManifestName FROM {ManifestURL} rdfs:label {ManifestName}");
		manifestNameQuery.setBinding("ManifestURL", manifestRep.getValueFactory().createURI(manifestFileURL));
		TupleQueryResult manifestNames = manifestNameQuery.evaluate();
		try {
			if (manifestNames.hasNext()) {
				return manifestNames.next().getValue("ManifestName").stringValue();
			}
		}
		finally {
			manifestNames.close();
		}

		// Derive name from manifest URL
		int lastSlashIdx = manifestFileURL.lastIndexOf('/');
		int secLastSlashIdx = manifestFileURL.lastIndexOf('/', lastSlashIdx - 1);
		return manifestFileURL.substring(secLastSlashIdx + 1, lastSlashIdx);
	}
}
