/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.ntriples;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;

/**
 * An {@link RDFParserFactory} for N-Triples parsers.
 * 
 * @author Arjohn Kampman
 * @openrdf
 */
public class BigdataNTriplesParserFactory implements RDFParserFactory {

	/**
	 * Returns {@link RDFFormat#NTRIPLES}.
	 */
	public RDFFormat getRDFFormat() {
		return RDFFormat.NTRIPLES;
	}

	/**
	 * Returns a new instance of BigdataNTriplesParser.
	 */
	public RDFParser getParser() {
		return new BigdataNTriplesParser();
	}
}
