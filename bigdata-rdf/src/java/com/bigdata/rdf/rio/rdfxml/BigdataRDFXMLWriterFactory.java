/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import java.io.OutputStream;
import java.io.Writer;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

/**
 * An {@link RDFWriterFactory} for RDF/XML writers with the bigdata specific
 * extensions.
 * 
 * @author Arjohn Kampman
 * @author Bryan Thompson
 * @openrdf
 */
public class BigdataRDFXMLWriterFactory implements RDFWriterFactory {

	/**
	 * Returns {@link RDFFormat#RDFXML}.
	 */
	public RDFFormat getRDFFormat() {
		return RDFFormat.RDFXML;
	}

	/**
	 * Returns a new instance of {@link RDFXMLWriter}.
	 */
	public RDFWriter getWriter(OutputStream out) {
		return new BigdataRDFXMLWriter(out);
	}

	/**
	 * Returns a new instance of {@link RDFXMLWriter}.
	 */
	public RDFWriter getWriter(Writer writer) {
		return new BigdataRDFXMLWriter(writer);
	}
}
