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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.io.FileUtil;
import info.aduna.io.IOUtil;

import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.DeleteData;
import org.openrdf.query.algebra.InsertData;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.helpers.SailUpdateExecutor;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

/**
 * A SPARQL 1.1 syntax test, created by reading in a W3C working-group style
 * manifest.
 * 
 * @author Jeen Broekstra
 */
public abstract class SPARQL11SyntaxTest extends TestCase {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static final Logger logger = LoggerFactory.getLogger(SPARQL11SyntaxTest.class);

	private static final String SUBMANIFEST_QUERY, TESTCASE_QUERY;

	static {
		StringBuilder sb = new StringBuilder(512);

		sb.append("PREFIX mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> ");
		sb.append("PREFIX qt: <http://www.w3.org/2001/sw/DataAccess/tests/test-query#> ");
		sb.append("SELECT ?subManifest ");
		sb.append("WHERE { [] mf:include [ rdf:rest*/rdf:first ?subManifest ] . } ");
		SUBMANIFEST_QUERY = sb.toString();

		sb.setLength(0);
		sb.append("PREFIX mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> ");
		sb.append("PREFIX qt: <http://www.w3.org/2001/sw/DataAccess/tests/test-query#> ");
		sb.append("PREFIX dawgt: <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#> ");
		sb.append("SELECT ?TestURI ?Name ?Action ?Type ");
		sb.append("WHERE { [] rdf:first ?TestURI. ");
		sb.append("        ?TestURI a ?Type ; ");
		sb.append("                 mf:name ?Name ;");
		sb.append("                 mf:action ?Action ;");
		sb.append("                 dawgt:approval dawgt:Approved . ");
		sb.append("        FILTER(?Type IN (mf:PositiveSyntaxTest11, mf:NegativeSyntaxTest11, mf:PositiveUpdateSyntaxTest11, mf:NegativeUpdateSyntaxTest11)) ");
		sb.append(" } ");
		TESTCASE_QUERY = sb.toString();
	}

	/*-----------*
	 * Variables *
	 *-----------*/

	protected final String testURI;

	protected final String queryFileURL;

	protected final boolean positiveTest;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public SPARQL11SyntaxTest(String testURI, String name, String queryFileURL, boolean positiveTest) {
		super(name);
		this.testURI = testURI;
		this.queryFileURL = queryFileURL;
		this.positiveTest = positiveTest;
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected void runTest()
		throws Exception
	{
		InputStream stream = new URL(queryFileURL).openStream();
		String query = IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
		stream.close();

		try {
			ParsedOperation operation = parseOperation(query, queryFileURL);

			if (!positiveTest) {
				boolean dataBlockUpdate = false;
				if (operation instanceof ParsedUpdate) {
					for (UpdateExpr updateExpr : ((ParsedUpdate)operation).getUpdateExprs()) {
						if (updateExpr instanceof InsertData || updateExpr instanceof DeleteData) {
							// parsing for these operation happens during actual
							// execution, so try and execute.
							dataBlockUpdate = true;

							MemoryStore store = new MemoryStore();
							store.initialize();
							NotifyingSailConnection conn = store.getConnection();
							try {
								conn.begin();
								SailUpdateExecutor exec = new SailUpdateExecutor(conn, store.getValueFactory(), null);
								exec.executeUpdate(updateExpr, null, null, true, -1);
								conn.rollback();
								fail("Negative test case should have failed to parse");
							}
							catch (SailException e) {
								if (!(e.getCause() instanceof RDFParseException)) {
									logger.error("unexpected error in negative test case", e);
									fail("unexpected error in negative test case");
								}
								// fall through - a parse exception is expected for a
								// negative test case
								conn.rollback();
							}
							finally {
								conn.close();
							}
						}
					}
				}
				if (!dataBlockUpdate) {
					fail("Negative test case should have failed to parse");
				}
			}
		}
		catch (MalformedQueryException e) {
			if (positiveTest) {
				e.printStackTrace();
				fail("Positive test case failed: " + e.getMessage());
			}
		}
	}

	protected abstract ParsedOperation parseOperation(String operation, String fileURL)
		throws MalformedQueryException;

	public static Test suite()
		throws Exception
	{
		return new TestSuite();
	}

	public interface Factory {

		SPARQL11SyntaxTest createSPARQLSyntaxTest(String testURI, String testName, String testAction,
				boolean positiveTest);
	}

	public static Test suite(Factory factory, boolean useRemote)
		throws Exception
	{
		// manifest of W3C Data Access Working Group SPARQL syntax tests
		final File tmpDir;
		String host;
		if (useRemote) {
			host = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/";
			tmpDir = null;
		}
		else {
			URL url = SPARQL11SyntaxTest.class.getResource("/testcases-sparql-1.1-w3c/");
			if ("jar".equals(url.getProtocol())) {
				try {
					tmpDir = FileUtil.createTempDir("sparql-syntax");
					JarURLConnection con = (JarURLConnection)url.openConnection();
					JarFile jar = con.getJarFile();
					Enumeration<JarEntry> entries = jar.entries();
					while (entries.hasMoreElements()) {
						JarEntry file = entries.nextElement();
						File f = new File(tmpDir + File.separator + file.getName());
						if (file.isDirectory()) {
							f.mkdir();
							continue;
						}
						InputStream is = jar.getInputStream(file);
						FileOutputStream fos = new FileOutputStream(f);
						while (is.available() > 0) {
							fos.write(is.read());
						}
						fos.close();
						is.close();
					}
					File localFile = new File(tmpDir, con.getEntryName());
					host = localFile.toURI().toURL().toString();
				}
				catch (IOException e) {
					throw new AssertionError(e);
				}
			}
			else {
				host = url.toString();
				tmpDir = null;
			}
		}

		String manifestFile = host + "manifest-all.ttl";

		TestSuite suite = new TestSuite() {

			@Override
			public void run(TestResult result) {
				try {
					super.run(result);
				}
				finally {
					if (tmpDir != null) {
						try {
							FileUtil.deleteDir(tmpDir);
						}
						catch (IOException e) {
							System.err.println("Unable to clean up temporary directory '" + tmpDir + "': "
									+ e.getMessage());
						}
					}
				}
			}
		};

		// Read manifest and create declared test cases
		Repository manifestRep = new SailRepository(new MemoryStore());
		manifestRep.initialize();

		RepositoryConnection con = manifestRep.getConnection();

		logger.debug("Loading manifest data");
		URL manifest = new URL(manifestFile);
		ManifestTest.addTurtle(con, manifest, manifestFile);

		logger.info("Searching for sub-manifests");
		List<String> subManifestList = new ArrayList<String>();

		TupleQueryResult subManifests = con.prepareTupleQuery(QueryLanguage.SPARQL, SUBMANIFEST_QUERY).evaluate();
		while (subManifests.hasNext()) {
			BindingSet bindings = subManifests.next();
			subManifestList.add(bindings.getValue("subManifest").toString());
		}
		subManifests.close();

		logger.info("Found {} sub-manifests", subManifestList.size());

		for (String subManifest : subManifestList) {
			logger.info("Loading sub manifest {}", subManifest);
			con.clear();

			URL subManifestURL = new URL(subManifest);
			ManifestTest.addTurtle(con, subManifestURL, subManifest);

			TestSuite subSuite = new TestSuite(subManifest.substring(host.length()));

			logger.info("Creating test cases for {}", subManifest);
			TupleQueryResult tests = con.prepareTupleQuery(QueryLanguage.SPARQL, TESTCASE_QUERY).evaluate();
			while (tests.hasNext()) {
				BindingSet bindingSet = tests.next();

				String testURI = bindingSet.getValue("TestURI").toString();
				String testName = bindingSet.getValue("Name").toString();
				String testAction = bindingSet.getValue("Action").toString();

				String type = bindingSet.getValue("Type").toString();
				boolean positiveTest = type.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11")
						|| type.equals("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11");

				subSuite.addTest(factory.createSPARQLSyntaxTest(testURI, testName, testAction, positiveTest));
			}
			tests.close();

			suite.addTest(subSuite);
		}

		con.close();
		manifestRep.shutDown();

		logger.info("Added {} tests to suite ", suite.countTestCases());
		return suite;
	}
}
