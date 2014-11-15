/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.bigdata.rdf.rio.json;

import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;

/**
 * A {@link TupleQueryResultParserFactory} for parsers of SPARQL-1.1 JSON Tuple
 * Query Results.
 * 
 * @author Peter Ansell
 * 
 * @deprecated We now use the openrdf versions of the JSON parser / writer. The
 *             bigdata specific versions will go away in the future.
 */
public class BigdataSPARQLResultsJSONParserFactory implements TupleQueryResultParserFactory {

	/**
	 * Returns {@link TupleQueryResultFormat#JSON}.
	 */
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return TupleQueryResultFormat.JSON;
	}

	/**
	 * Returns a new instance of {@link SPARQLResultsJSONParser}.
	 */
	public TupleQueryResultParser getParser() {
		return new BigdataSPARQLResultsJSONParser();
	}
}
