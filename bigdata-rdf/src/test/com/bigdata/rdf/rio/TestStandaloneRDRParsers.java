package com.bigdata.rdf.rio;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.rdf.rio.ntriples.BigdataNTriplesParser;
import com.bigdata.rdf.rio.turtle.BigdataTurtleParser;

/**
 * Test suite for standalone usage of Bigdata RDF Parsers.
 * See also https://jira.blazegraph.com/browse/BLZG-1322
 */
public class TestStandaloneRDRParsers {
	boolean bNodeFound;

	@Test
	public void testStandaloneBigdataTurtleParser() throws RDFParseException, RDFHandlerException, IOException {
		testStandaloneParser(new BigdataTurtleParser(), "com/bigdata/rdf/rio/rdr_test.ttlx");
	}
	
	@Test
	public void testStandaloneBigdataNTriplesParser() throws RDFParseException, RDFHandlerException, IOException {
		testStandaloneParser(new BigdataNTriplesParser(), "com/bigdata/rdf/rio/rdr_test.ntx");
	}
	
	private void testStandaloneParser(RDFParser parser, String resourceName) throws IOException,
			RDFParseException, RDFHandlerException {
		bNodeFound = false;
		parser.setRDFHandler(new RDFHandlerBase(){
			@Override
			public void handleStatement(Statement st)
					throws RDFHandlerException {
				if (st.getSubject() instanceof BNode) {
					bNodeFound = true;
				}
				super.handleStatement(st);
			}
		});
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
			parser.parse(is, "");
		}
		assertTrue(bNodeFound);
	}
}
