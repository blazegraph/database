/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;


public class RDFXMLWriterTest extends RDFXMLWriterTestCase {

    /*
     * Note: Modified to test the Bigdata versions of the RDF/XML writer/parser
     * classes.
     */
	public RDFXMLWriterTest() {
		super(new BigdataRDFXMLWriterFactory(), new BigdataRDFXMLParserFactory());
	}
	
}
