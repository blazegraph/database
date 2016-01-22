/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.ntriples;

import org.openrdf.rio.RDFParser;

import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * JUnit test for the N-Triples parser.
 * 
 * @author Arjohn Kampman
 */
public class BigdataNTriplesParserTest extends BigdataNTriplesParserTestCase {

	private BigdataValueFactory valueFactory;
	
	protected void setUp() throws Exception {
		super.setUp();
		valueFactory = BigdataValueFactoryImpl.getInstance(getName());
	}

	protected void tearDown() throws Exception {
		if (valueFactory != null) {
			valueFactory.remove();
			valueFactory = null;
		}
		super.tearDown();
	}
	
	@Override
	protected RDFParser createRDFParser() {
		/*
		 * Note: Requires the BigdataValueFactory for SIDs support.
		 */
		return new BigdataNTriplesParser(valueFactory);
	}
}
