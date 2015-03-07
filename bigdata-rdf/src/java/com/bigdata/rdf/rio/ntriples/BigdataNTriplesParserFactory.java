/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.ntriples;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;

import com.bigdata.rdf.ServiceProviderHook;

/**
 * An RDR-aware {@link RDFParserFactory} for N-Triples parsers.
 * 
 * @author Arjohn Kampman
 * @openrdf
 * 
 * @see http://wiki.blazegraph.com/wiki/index.php/Reification_Done_Right
 */
public class BigdataNTriplesParserFactory implements RDFParserFactory {

	/**
	 * Returns {@link RDFFormat#NTRIPLES_RDR}.
	 */
	@Override
	public RDFFormat getRDFFormat() {
		return ServiceProviderHook.NTRIPLES_RDR;
	}

	/**
	 * Returns a new instance of BigdataNTriplesParser.
	 */
	@Override
	public RDFParser getParser() {
		return new BigdataNTriplesParser();
	}
}
