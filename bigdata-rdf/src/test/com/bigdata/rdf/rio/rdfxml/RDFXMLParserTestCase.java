/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.sail.memory.MemoryStore;
import org.xml.sax.SAXException;

//import com.sun.org.apache.xml.internal.security.c14n.CanonicalizationException;
//import com.sun.org.apache.xml.internal.security.c14n.Canonicalizer;
//import com.sun.org.apache.xml.internal.security.c14n.InvalidCanonicalizerException;

/**
 * JUnit test for the RDF/XML parser that uses the test manifest that is
 * available <a
 * href="http://www.w3.org/2000/10/rdf-tests/rdfcore/Manifest.rdf">online</a>.
 * <p>
 * Note: This file was imported in order to remove the xmlsec library dependency
 * because it bundles encryption software. xmlsec was being used by the
 * {@link CanonXMLValueFactory}. One test fails with that dependency removed
 * <code>
 * http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xml-literal-namespaces/test001.rdf</code>
 * . The stack trace for that failure looks like this:
 * 
 * <pre>
 * (http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xml-literal-namespaces/test001.rdf#John_Smith, http://my.example.org/Name, "
 *       <html:h1 xmlns:html="http://NoHTML.example.org">
 *         <b xmlns="http://www.w3.org/1999/xhtml">John</b>
 *       </html:h1>
 *    "^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>)
 * Actual:
 * (http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xml-literal-namespaces/test001.rdf#John_Smith, http://my.example.org/Name, "
 *       <html:h1 xmlns:html="http://NoHTML.example.org" xmlns="http://www.w3.org/1999/xhtml">
 *         <b>John</b>
 *       </html:h1>
 *    "^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>)
 * </pre>
 * See the ticket below for more information on this.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/498"> Bring
 *      bigdata RDF/XML parser up to openrdf 2.6.3</a>
 */
public abstract class RDFXMLParserTestCase {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static String W3C_TESTS_DIR = "http://www.w3.org/2000/10/rdf-tests/rdfcore/";

	private static String LOCAL_TESTS_DIR = "/testcases/rdfxml/";

	private static String W3C_MANIFEST_FILE = W3C_TESTS_DIR + "Manifest.rdf";

	private static String OPENRDF_MANIFEST_FILE = LOCAL_TESTS_DIR + "openrdf/Manifest.rdf";

	/*--------------------*
	 * Static initializer *
	 *--------------------*/

	public TestSuite createTestSuite()
		throws Exception
	{
		// Create an RDF repository for the manifest data
		Repository repository = new SailRepository(new MemoryStore());
		repository.initialize();
		RepositoryConnection con = repository.getConnection();

		// Add W3C's manifest
		URL w3cManifest = resolveURL(W3C_MANIFEST_FILE);
		con.add(w3cManifest, base(W3C_MANIFEST_FILE), RDFFormat.RDFXML);

		// Add our own manifest
		URL localManifest = resolveURL(OPENRDF_MANIFEST_FILE);
		con.add(localManifest, base(localManifest.toString()), RDFFormat.RDFXML);

		// Create test suite
		TestSuite suite = new TestSuite(RDFXMLParserTestCase.class.getName());

		// Add all positive parser tests
		String query = "select TESTCASE, INPUT, OUTPUT "
				+ "from {TESTCASE} rdf:type {test:PositiveParserTest}; "
				+ "                test:inputDocument {INPUT}; "
				+ "                test:outputDocument {OUTPUT}; "
				+ "                test:status {\"APPROVED\"} "
				+ "using namespace test = <http://www.w3.org/2000/10/rdf-tests/rdfcore/testSchema#>";
		TupleQueryResult queryResult = con.prepareTupleQuery(QueryLanguage.SERQL, query).evaluate();
		while (queryResult.hasNext()) {
			BindingSet bindingSet = queryResult.next();
			String caseURI = bindingSet.getValue("TESTCASE").toString();
			String inputURL = bindingSet.getValue("INPUT").toString();
			String outputURL = bindingSet.getValue("OUTPUT").toString();
			suite.addTest(new PositiveParserTest(caseURI, inputURL, outputURL));
		}

		queryResult.close();

		// Add all negative parser tests
		query = "select TESTCASE, INPUT " + "from {TESTCASE} rdf:type {test:NegativeParserTest}; "
				+ "                test:inputDocument {INPUT}; " + "                test:status {\"APPROVED\"} "
				+ "using namespace test = <http://www.w3.org/2000/10/rdf-tests/rdfcore/testSchema#>";
		queryResult = con.prepareTupleQuery(QueryLanguage.SERQL, query).evaluate();
		while (queryResult.hasNext()) {
			BindingSet bindingSet = queryResult.next();
			String caseURI = bindingSet.getValue("TESTCASE").toString();
			String inputURL = bindingSet.getValue("INPUT").toString();
			suite.addTest(new NegativeParserTest(caseURI, inputURL));
		}

		queryResult.close();
		con.close();
		repository.shutDown();

		return suite;
	}

	private static URL resolveURL(String urlString)
		throws MalformedURLException
	{
		if (urlString.startsWith(W3C_TESTS_DIR)) {
			// resolve to local copy
			urlString = LOCAL_TESTS_DIR + "w3c-approved/" + urlString.substring(W3C_TESTS_DIR.length());
		}

		if (urlString.startsWith("/")) {
			return RDFXMLParserTestCase.class.getResource(urlString);
		}
		else {
			return url(urlString);
		}
	}

	protected abstract RDFParser createRDFParser();

	/*--------------------------------*
	 * Inner class PositiveParserTest *
	 *--------------------------------*/

	private class PositiveParserTest extends TestCase {

		/*-----------*
		 * Variables *
		 *-----------*/

		private String inputURL;

		private String outputURL;

		/*--------------*
		 * Constructors *
		 *--------------*/

		public PositiveParserTest(String caseURI, String inputURL, String outputURL) {
			super(caseURI);
			this.inputURL = inputURL;
			this.outputURL = outputURL;
		}

		/*---------*
		 * Methods *
		 *---------*/

		@Override
		protected void runTest()
			throws Exception
		{
			// Parse input data
			RDFParser rdfxmlParser = createRDFParser();
			rdfxmlParser.setValueFactory(new CanonXMLValueFactory());
			rdfxmlParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

			Set<Statement> inputCollection = new LinkedHashSet<Statement>();
			StatementCollector inputCollector = new StatementCollector(inputCollection);
			rdfxmlParser.setRDFHandler(inputCollector);

			InputStream in = resolveURL(inputURL).openStream();
			rdfxmlParser.parse(in, base(inputURL));
			in.close();

			// Parse expected output data
			NTriplesParser ntriplesParser = new NTriplesParser();
			ntriplesParser.setValueFactory(new CanonXMLValueFactory());
			ntriplesParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

			Set<Statement> outputCollection = new LinkedHashSet<Statement>();
			StatementCollector outputCollector = new StatementCollector(outputCollection);
			ntriplesParser.setRDFHandler(outputCollector);

			in = resolveURL(outputURL).openStream();
			ntriplesParser.parse(in, base(inputURL));
			in.close();

			// Check equality of the two models
			if (!ModelUtil.equals(inputCollection, outputCollection)) {
				StringBuilder sb = new StringBuilder(1024);
				sb.append("models not equal\n");
				sb.append("Expected:\n");
				for (Statement st : outputCollection) {
					sb.append(st).append("\n");
				}
				sb.append("Actual:\n");
				for (Statement st : inputCollection) {
					sb.append(st).append("\n");
				}

				fail(sb.toString());
			}
		}

	} // end inner class PositiveParserTest

	/*--------------------------------*
	 * Inner class NegativeParserTest *
	 *--------------------------------*/

	private class NegativeParserTest extends TestCase {

		/*-----------*
		 * Variables *
		 *-----------*/

		private String inputURL;

		/*--------------*
		 * Constructors *
		 *--------------*/

		public NegativeParserTest(String caseURI, String inputURL) {
			super(caseURI);
			this.inputURL = inputURL;
		}

		/*---------*
		 * Methods *
		 *---------*/

		@Override
		protected void runTest() {
			try {
				// Try parsing the input; this should result in an error being
				// reported.
				RDFParser rdfxmlParser = createRDFParser();
				rdfxmlParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

				rdfxmlParser.setRDFHandler(new StatementCollector());

				InputStream in = resolveURL(inputURL).openStream();
				rdfxmlParser.parse(in, base(inputURL));
				in.close();

				fail("Parser parses erroneous data without reporting errors");
			}
			catch (RDFParseException e) {
				// This is expected as the input file is incorrect RDF
			}
			catch (Exception e) {
				fail("Error: " + e.getMessage());
			}
		}

	} // end inner class NegativeParserTest

	private static class CanonXMLValueFactory extends ValueFactoryImpl {

//		private Canonicalizer c14n;

		public CanonXMLValueFactory()
//         throws InvalidCanonicalizerException, ParserConfigurationException
	       throws ParserConfigurationException
		{
//			org.apache.xml.security.Init.init();

//			c14n = Canonicalizer.getInstance(Canonicalizer.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
		}

		@Override
		public Literal createLiteral(String value, URI datatype) {
//			if (RDF.XMLLITERAL.equals(datatype)) {
//				// Canonicalize the literal value
//				try {
//					value = new String(c14n.canonicalize(value.getBytes("UTF-8")), "UTF-8");
//				}
//				catch (UnsupportedEncodingException e) {
//					throw new RuntimeException(e);
//				}
//				catch (CanonicalizationException e) {
//					// ignore
//				}
//				catch (ParserConfigurationException e) {
//					throw new RuntimeException(e);
//				}
//				catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//				catch (SAXException e) {
//					// ignore
//				}
//			}

			return super.createLiteral(value, datatype);
		}
	}

	private static URL url(String uri)
			throws MalformedURLException {
		if (!uri.startsWith("injar:"))
			return new URL(uri);
		int start = uri.indexOf(':') + 3;
		int end = uri.indexOf('/', start);
		String encoded = uri.substring(start, end);
		try {
			String jar = URLDecoder.decode(encoded, "UTF-8");
			return new URL("jar:" + jar + '!' + uri.substring(end));
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError(e);
		}
	}

	private static String base(String uri) {
		if (!uri.startsWith("jar:"))
			return uri;
		int start = uri.indexOf(':') + 1;
		int end = uri.lastIndexOf('!');
		String jar = uri.substring(start, end);
		try {
			String encoded = URLEncoder.encode(jar, "UTF-8");
			return "injar://" + encoded + uri.substring(end + 1);
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError(e);
		}
	}
}
