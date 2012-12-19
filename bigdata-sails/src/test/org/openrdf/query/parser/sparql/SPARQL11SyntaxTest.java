/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2010.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

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
 * Note: Override is only to allow improved error reporting.
 * 
 * @openrdf
 */
public abstract class SPARQL11SyntaxTest extends TestCase {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static final Logger logger = LoggerFactory.getLogger(SPARQL11SyntaxTest.class);

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
		sb.append("               mf:action {Action};");
		sb.append("               dawgt:approval {dawgt:Approved} ");
		sb.append("WHERE Type = mf:PositiveSyntaxTest11 or Type = mf:NegativeSyntaxTest11 or Type = mf:PositiveUpdateSyntaxTest11 or Type = mf:NegativeUpdateSyntaxTest11 ");
		sb.append("USING NAMESPACE");
		sb.append("  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>,");
		sb.append("  qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>,");
		sb.append("  dawgt = <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>");
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
			parseOperation(query, queryFileURL);

			if (!positiveTest) {
			    // BBT : Override
                fail("Negative test case should have failed to parse: queryFileURL="
                        + queryFileURL + "\n" + query);
			}
		}
		catch (MalformedQueryException e) {
			if (positiveTest) {
				e.printStackTrace();
				// BBT : Override
                fail("Positive test case failed: " + e.getMessage()
                        + ", queryFileURL=" + queryFileURL + "\n" + query);
            }
		}
	}

	protected abstract void parseOperation(String operation, String fileURL)
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

	public static Test suite(Factory factory)
		throws Exception
	{
		// manifest of W3C Data Access Working Group SPARQL syntax tests
		final File tmpDir;
		String host;
		if (REMOTE) {
			host = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/";
			tmpDir = null;
		}
		else {
			URL url = SPARQL11SyntaxTest.class.getResource("/testcases-dawg-sparql-1.1/");
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
