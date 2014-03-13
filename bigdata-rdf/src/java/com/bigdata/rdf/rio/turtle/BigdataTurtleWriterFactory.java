/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.turtle;

import java.io.OutputStream;
import java.io.Writer;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

/**
 * An {@link RDFWriterFactory} for Turtle writers.
 * 
 * @author Arjohn Kampman
 */
public class BigdataTurtleWriterFactory implements RDFWriterFactory {

	/**
	 * Returns {@link RDFFormat#TURTLE}.
	 */
	public RDFFormat getRDFFormat() {
		return RDFFormat.TURTLE;
	}

	/**
	 * Returns a new instance of {@link BigdataTurtleWriter}.
	 */
	public RDFWriter getWriter(OutputStream out) {
		return new BigdataTurtleWriter(out);
	}

	/**
	 * Returns a new instance of {@link BigdataTurtleWriter}.
	 */
	public RDFWriter getWriter(Writer writer) {
		return new BigdataTurtleWriter(writer);
	}
}
