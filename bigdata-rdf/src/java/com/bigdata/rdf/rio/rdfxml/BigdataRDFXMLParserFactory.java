/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;

/**
 * An {@link RDFParserFactory} for RDF/XML parsers.
 * 
 * @author Arjohn Kampman
 * @author Bryan Thompson
 * @openrdf
 */
public class BigdataRDFXMLParserFactory implements RDFParserFactory {

	/**
	 * Returns the RDF format for this factory.
	 */
	public RDFFormat getRDFFormat() {
		return RDFFormat.RDFXML;
	}

	/**
	 * Returns a new instance of RDFXMLParser.
	 */
	public RDFParser getParser() {
		return new BigdataRDFXMLParser();
	}
}
