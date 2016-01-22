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

import com.bigdata.rdf.ServiceProviderHook;

/**
 * An RDR-aware {@link RDFWriterFactory} for Turtle writers.
 * 
 * @author Arjohn Kampman
 * @openrdf
 * 
 * @see http://wiki.blazegraph.com/wiki/index.php/Reification_Done_Right
 */
public class BigdataTurtleWriterFactory implements RDFWriterFactory {

	/**
	 * Returns {@link ServiceProviderHook#TURTLE_RDR}.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1038" >RDR RDF parsers not
	 *      always discovered </a>
	 */
	@Override
	public RDFFormat getRDFFormat() {
		return ServiceProviderHook.TURTLE_RDR;
	}

	/**
	 * Returns a new instance of {@link BigdataTurtleWriter}.
	 */
	@Override
	public RDFWriter getWriter(final OutputStream out) {
		return new BigdataTurtleWriter(out);
	}

	/**
	 * Returns a new instance of {@link BigdataTurtleWriter}.
	 */
	@Override
	public RDFWriter getWriter(final Writer writer) {
		return new BigdataTurtleWriter(writer);
	}
}
