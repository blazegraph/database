/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestListener;
import junit.framework.TestResult;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQL11SyntaxTest;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLSyntaxTest;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateConformanceTest;
import com.bigdata.rdf.sail.tck.BigdataSparqlTest;

/**
 * Generate an Earl report for various test suites.
 * <p> 
 * Note: A variety of overrides have been made in order to run the test suite
 * against Bigdata.
 * 
 * @author Arjohn Kampman
 * @author Bryan Thompson
 */
public class EarlReport {

    private static final Logger log = Logger.getLogger(EarlReport.class); // BBT: Override (plus log statements).
    
	protected static Repository earlRepository;

	protected static ValueFactory vf;

	protected static RepositoryConnection con;

	protected static Resource projectNode;

	protected static Resource asserterNode;

	public static void main(String[] args)
		throws Exception
	{
	    // BBT : Override for BigdataSail.
	    final Properties properties = new Properties();
	    properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE, BufferMode.Transient.toString());
		earlRepository = new BigdataSailRepository(new BigdataSail(properties));
		earlRepository.initialize();
		vf = earlRepository.getValueFactory();
		con = earlRepository.getConnection();
		con.setAutoCommit(false);

		con.setNamespace("rdf", RDF.NAMESPACE);
		con.setNamespace("xsd", XMLSchema.NAMESPACE);
		con.setNamespace("doap", DOAP.NAMESPACE);
		con.setNamespace("earl", EARL.NAMESPACE);
		con.setNamespace("dc", DC.NAMESPACE);

		projectNode = vf.createBNode();
		BNode releaseNode = vf.createBNode();
		con.add(projectNode, RDF.TYPE, DOAP.PROJECT);
		con.add(projectNode, DOAP.NAME, vf.createLiteral("Bigdata")); // BBT : Override
		con.add(projectNode, DOAP.RELEASE, releaseNode);
		con.add(releaseNode, RDF.TYPE, DOAP.VERSION);
		con.add(releaseNode, DOAP.NAME, vf.createLiteral("Bigdata 1.2.2")); // FIXME BBT: Override each time we run this!
		SimpleDateFormat xsdDataFormat = new SimpleDateFormat("yyyy-MM-dd");
		String currentDate = xsdDataFormat.format(new Date());
		con.add(releaseNode, DOAP.CREATED, vf.createLiteral(currentDate, XMLSchema.DATE));

		asserterNode = vf.createBNode();
		con.add(asserterNode, RDF.TYPE, EARL.SOFTWARE);
		con.add(asserterNode, DC.TITLE, vf.createLiteral("OpenRDF SPARQL 1.1 compliance test"));

		TestResult testResult = new TestResult();
		EarlTestListener listener = new EarlTestListener();
		testResult.addListener(listener);

        log.info("running query evaluation tests..");
		BigdataSparqlTest.suite().run(testResult); // BBT : Override. FIXME RESTORE
		
		log.info("running syntax tests..");
        /*
         * FIXME Is this being run by our class?
         * 
         * CoreSPARQL11SyntaxTest.suite().run(testResult);
         */
        Bigdata2ASTSPARQLSyntaxTest.suite().run(testResult); // BBT : Override
        Bigdata2ASTSPARQL11SyntaxTest.suite().run(testResult); // BBT : Override

        log.info("running update tests...");
        BigdataSPARQLUpdateConformanceTest.suite().run(testResult);

		con.setAutoCommit(false); // BBT: Override

		RDFWriterFactory factory = RDFWriterRegistry.getInstance().get(RDFFormat.TURTLE);
		File outFile = File.createTempFile("sesame-sparql-compliance", "."
				+ RDFFormat.TURTLE.getDefaultFileExtension());
		FileOutputStream out = new FileOutputStream(outFile);
		try {
			con.export(factory.getWriter(out));
			con.commit(); // BBT : Override
		}
		finally {
			out.close();
		}

		con.close();
		earlRepository.shutDown();

		System.out.println("EARL output written to " + outFile);
	}

	protected static class EarlTestListener implements TestListener {

		private int errorCount;

		private int failureCount;

		public void startTest(Test test) {
			errorCount = failureCount = 0;
		}

		public void endTest(Test test) {
            
		    if (log.isInfoEnabled())
                log.info("test: " + test);

            String testURI = null;

			if (test instanceof SPARQLQueryTest) {
				testURI = ((SPARQLQueryTest)test).testURI;
			}
			// FIXME This version is gone in openrdf 2.6.10.
			else if (test instanceof SPARQLSyntaxTest) {
				testURI = ((SPARQLSyntaxTest)test).testURI;
			}
            else if (test instanceof SPARQL11SyntaxTest) {
                testURI = ((SPARQL11SyntaxTest)test).testURI;
            }
            else if (test instanceof SPARQLUpdateConformanceTest) {
                testURI = ((SPARQLUpdateConformanceTest)test).testURI;
            } else {
				throw new RuntimeException("Unexpected test type: " + test.getClass());
			}

			try {
				BNode testNode = vf.createBNode();
				BNode resultNode = vf.createBNode();
				con.add(testNode, RDF.TYPE, EARL.ASSERTION);
				con.add(testNode, EARL.ASSERTEDBY, asserterNode);
				con.add(testNode, EARL.MODE, EARL.AUTOMATIC);
				con.add(testNode, EARL.SUBJECT, projectNode);
				con.add(testNode, EARL.TEST, vf.createURI(testURI));
				con.add(testNode, EARL.RESULT, resultNode);
				con.add(resultNode, RDF.TYPE, EARL.TESTRESULT);

				if (errorCount > 0) {
					con.add(resultNode, EARL.OUTCOME, EARL.FAIL);
				}
				else if (failureCount > 0) {
					con.add(resultNode, EARL.OUTCOME, EARL.FAIL);
				}
				else {
					con.add(resultNode, EARL.OUTCOME, EARL.PASS);
				}
			}
			catch (RepositoryException e) {
				throw new RuntimeException(e);
			}
		}

		public void addError(Test test, Throwable t) {
		    log.warn("test: " + test + ", cause=" + t);
			errorCount++;
		}

		public void addFailure(Test test, AssertionFailedError error) {
            log.warn("test: " + test + ", error=" + error);
			failureCount++;
		}
	}
}
