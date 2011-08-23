/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.QueryParserFactory;

/**
 * A {@link QueryParserFactory} for SPARQL parsers.
 * 
 * @author Arjohn Kampman
 */
public class SPARQLParserFactory implements QueryParserFactory {

	private final SPARQLParser singleton = new SPARQLParser();

	/**
	 * Returns {@link QueryLanguage#SPARQL}.
	 */
	public QueryLanguage getQueryLanguage() {
		return QueryLanguage.SPARQL;
	}

	/**
	 * Returns a shared, thread-safe, instance of SPARQLParser.
	 */
	public QueryParser getParser() {
		return singleton;
	}
}
