/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.turtle;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.turtle.TurtleParser;

/**
 * An {@link RDFParserFactory} for Turtle parsers.
 * 
 * @author Arjohn Kampman
 * @openrdf
 */
public class BigdataTurtleParserFactory implements RDFParserFactory {

	/**
	 * Returns {@link RDFFormat#TURTLE}.
	 */
	public RDFFormat getRDFFormat() {
		return RDFFormat.TURTLE;
	}

	/**
	 * Returns a new instance of {@link TurtleParser}.
	 */
	public RDFParser getParser() {
		return new BigdataTurtleParser();
	}
}
