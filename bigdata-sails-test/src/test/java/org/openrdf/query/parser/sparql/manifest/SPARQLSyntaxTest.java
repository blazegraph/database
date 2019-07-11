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
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

/**
 * A SPARQL syntax test, created by reading in a W3C working-group style manifest.  
 *
 * @author Jeen Broekstra
 */
public abstract class SPARQLSyntaxTest extends TestCase {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static final Logger logger = LoggerFactory.getLogger(SPARQLSyntaxTest.class);

	private static final boolean REMOTE = false;

	private static final String SUBMANIFEST_QUERY, TESTCASE_QUERY;

	static {
		StringBuilder sb = new StringBuilder(512);

		sb.append("SELECT subManifest ");
		sb.append("FROM {} rdf:first {subManifest} ");
		sb.append("USING NAMESPACE");
		sb.append("  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>,");
		sb.append("  qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>");
		SUBMANIFEST_QUERY = sb.toString();

		sb.setLength(0);
		sb.append("SELECT TestURI, Name, Action, Type ");
		sb.append("FROM {TestURI} rdf:type {Type};");
		sb.append("               mf:name {Name};");
		sb.append("               mf:action {Action} ");
		sb.append("WHERE Type = mf:PositiveSyntaxTest or Type = mf:NegativeSyntaxTest ");
		sb.append("USING NAMESPACE");
		sb.append("  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>,");
		sb.append("  qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>");
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

	public SPARQLSyntaxTest(String testURI, String name, String queryFileURL, boolean positiveTest) {
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
			parseQuery(query, queryFileURL);

			if (!positiveTest) {
				fail("Negative test case should have failed to parse");
			}
		}
		catch (MalformedQueryException e) {
			if (positiveTest) {
				e.printStackTrace();
				fail("Positive test case failed: " + e.getMessage());
			}
		}
	}

	protected abstract void parseQuery(String query, String queryFileURL)
		throws MalformedQueryException;

	public static Test suite()
		throws Exception
	{
		return new TestSuite();
	}

	public interface Factory {

		SPARQLSyntaxTest createSPARQLSyntaxTest(String testURI, String testName, String testAction,
				boolean positiveTest);
	}

	public static Test suite(Factory factory)
		throws Exception
	{
		// manifest of W3C Data Access Working Group SPARQL syntax tests
		final File tmpDir;
		String host;
		if (REMOTE) {
			host = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/";
			tmpDir = null;
		}
		else {
			URL url = SPARQLSyntaxTest.class.getResource("/testcases-dawg/data-r2/");
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

		String manifestFile = host + "manifest-syntax.ttl";

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
							System.err.println("Unable to clean up temporary directory '" + tmpDir + "': " + e.getMessage());
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

		TupleQueryResult subManifests = con.prepareTupleQuery(QueryLanguage.SERQL, SUBMANIFEST_QUERY).evaluate();
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
			TupleQueryResult tests = con.prepareTupleQuery(QueryLanguage.SERQL, TESTCASE_QUERY).evaluate();
			while (tests.hasNext()) {
				BindingSet bindingSet = tests.next();

				String testURI = bindingSet.getValue("TestURI").toString();
				String testName = bindingSet.getValue("Name").toString();
				String testAction = bindingSet.getValue("Action").toString();
				boolean positiveTest = bindingSet.getValue("Type").toString().equals(
						"http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest");

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
