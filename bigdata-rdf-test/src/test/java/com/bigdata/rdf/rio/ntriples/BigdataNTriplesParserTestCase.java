/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.ntriples;

import java.io.InputStream;

import junit.framework.TestCase2;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * JUnit test for the N-Triples parser.
 * 
 * @author Arjohn Kampman
 */
public abstract class BigdataNTriplesParserTestCase extends TestCase2 {

	/*-----------*
	 * Constants *
	 *-----------*/

	private static String NTRIPLES_TEST_URL = "http://www.w3.org/2000/10/rdf-tests/rdfcore/ntriples/test.nt";

//	private static String NTRIPLES_TEST_FILE = "/testcases/ntriples/test.nt";
	private static String NTRIPLES_TEST_FILE = "test.nt";

	private static String NTRIPLES_TEST_FILE_WITH_SIDS = "test-sids.nt";
	
	/**
	 * TODO This file is not legal NTRIPLES. It has "_" and "-" in the blank
	 * node IDs. Those characters are not legal in NTRIPLES within a blank node
	 * ID. The parser has been hacked to comply with this file. See the parser
	 * for what would need to be changed to remove this non-conforming behavior.
	 */
	private static String NTRIPLES_TEST_FILE_WITH_REIFICATION = "test-reify.nt";

	private static final transient Logger log = Logger.getLogger(BigdataNTriplesParserTestCase.class);
	
	/*---------*
	 * Methods *
	 *---------*/

	public void testNTriplesFile()
		throws Exception
	{
		RDFParser turtleParser = createRDFParser();
		turtleParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
		turtleParser.setRDFHandler(new RDFHandlerBase() {
			public void handleStatement(Statement st)
					throws RDFHandlerException {
				if (log.isInfoEnabled())
					log.info("Statement: " + st);
			}
		});

		// Note: This is a local copy.
		InputStream in = BigdataNTriplesParser.class.getResourceAsStream(NTRIPLES_TEST_FILE);
		try {
			turtleParser.parse(in, NTRIPLES_TEST_URL);
		}
		catch (RDFParseException e) {
			fail("Failed to parse N-Triples test document: " + e.getMessage());
		}
		finally {
			in.close();
		}
	}

	/*
	 * TODO Test with embedded statement in the object position (this only tests
	 * with embedded statements in the subject position).
	 * 
	 * TODO Test with double-embedding. Do we need to use a stack to get the
	 * resolution right?
	 * 
	 * TODO The StatementBuffer still needs to handle the bnode => SID
	 * resolution in order to produce the correct representation.
	 * 
	 * TODO Verify that the parser does not need to use the *same* bnode for
	 * each distinct statement about which a statement is being made. I think
	 * that it is Ok if we have different blank nodes in different places since
	 * they will be reconciled in the data when we look at the statement
	 * indices. An LRU hash map (LinkedHashMap with capacity bound) would be
	 * much more space efficient for a large data load such as dbpedia with SIDs
	 * than if we needed to wire each embedded statement into memory.
	 * 
	 * TODO We need to be able to output NTRIPLES as well (hack the writer).
	 * 
	 * TODO ntriples is a bulky format. Do N3, which is much nicer.
	 */
	public void testNTriplesFileWithSIDS() throws Exception {
		RDFParser turtleParser = createRDFParser();
		turtleParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
		turtleParser.setRDFHandler(new RDFHandlerBase() {
			public void handleStatement(final Statement st)
					throws RDFHandlerException {
				if (log.isInfoEnabled())
					log.info("Statement: " + st);
			}
		});

		final String fileUrl = BigdataNTriplesParser.class.getResource(
				NTRIPLES_TEST_FILE_WITH_SIDS).toExternalForm();

		// Note: This is a local copy.
		InputStream in = BigdataNTriplesParser.class
				.getResourceAsStream(NTRIPLES_TEST_FILE_WITH_SIDS);
		try {
			turtleParser.parse(in, fileUrl);
		} catch (RDFParseException e) {
			fail("Failed to parse N-Triples test document: " + e.getMessage(), e);
		} finally {
			in.close();
		}
	}

	public void testNTriplesFileWithReification() throws Exception {
		RDFParser turtleParser = createRDFParser();
		turtleParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
		turtleParser.setRDFHandler(new RDFHandlerBase() {
			public void handleStatement(final Statement st)
					throws RDFHandlerException {
				if (log.isInfoEnabled())
					log.info("Statement: " + st);
			}
		});

		final String fileUrl = BigdataNTriplesParser.class.getResource(
				NTRIPLES_TEST_FILE_WITH_REIFICATION).toExternalForm();

		// Note: This is a local copy.
		InputStream in = BigdataNTriplesParser.class
				.getResourceAsStream(NTRIPLES_TEST_FILE_WITH_REIFICATION);
		try {
			turtleParser.parse(in, fileUrl);
		} catch (RDFParseException e) {
			fail("Failed to parse N-Triples test document: " + e.getMessage(), e);
		} finally {
			in.close();
		}
	}

	protected abstract RDFParser createRDFParser();
}
