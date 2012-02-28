/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import junit.framework.Test;

import org.openrdf.rio.RDFParser;

/**
 * JUnit test for the RDF/XML parser that uses the test manifest that is
 * available <a
 * href="http://www.w3.org/2000/10/rdf-tests/rdfcore/Manifest.rdf">online</a>.
 */
public class RDFXMLParserTest extends RDFXMLParserTestCase {

	public static Test suite()
		throws Exception
	{
		return new RDFXMLParserTest().createTestSuite();
	}

    /*
     * Note: This method was modified to test the bigdata version of the RDF/XML
     * parser.
     */
	@Override
	protected RDFParser createRDFParser() {
        final BigdataRDFXMLParser rdfxmlParser = new BigdataRDFXMLParser();
        rdfxmlParser.setParseStandAloneDocuments(true);
        return rdfxmlParser;
    }
}
