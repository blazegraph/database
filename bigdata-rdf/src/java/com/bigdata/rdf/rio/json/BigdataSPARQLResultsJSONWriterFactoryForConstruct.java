/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.json;

import java.io.OutputStream;
import java.io.Writer;

import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

/**
 * A {@link TupleQueryResultWriterFactory} for writers of SPARQL/JSON query
 * results.
 * 
 * @author Arjohn Kampman
 */
public class BigdataSPARQLResultsJSONWriterFactoryForConstruct implements RDFWriterFactory {

	/**
	 * Returns {@link TupleQueryResultFormat#JSON}.
	 */
	public RDFFormat getRDFFormat() {
		return BigdataSPARQLResultsJSONParserFactory.JSON;
	}

	/**
	 * Returns a new instance of SPARQLResultsJSONWriter.
	 */
	public RDFWriter getWriter(OutputStream out) {
		return new BigdataSPARQLResultsJSONWriter(out);
	}
	
	/**
	 * Returns a new instance of SPARQLResultsJSONWriter.
	 */
	public RDFWriter getWriter(Writer writer) {
		return new BigdataSPARQLResultsJSONWriter(writer);
	}
}
