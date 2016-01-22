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

import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;

import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * A {@link TupleQueryResultParserFactory} for parsers of SPARQL-1.1 JSON Tuple
 * Query Results.
 * 
 * @author Peter Ansell
 */
public class BigdataSPARQLResultsJSONParserForConstructFactory implements RDFParserFactory {
    
	@Override
	public RDFParser getParser() {
		//TICKET 1284:  Quads needed. This is a workaround to create a value factory
		//in the default namespace.
		final String namespace = "kb";
		return new BigdataSPARQLResultsJSONParserForConstruct(BigdataValueFactoryImpl.getInstance(namespace));
	}
	
	public RDFParser getParser(String namespace) {
		return new BigdataSPARQLResultsJSONParserForConstruct(BigdataValueFactoryImpl.getInstance(namespace));
	}

	@Override
	public RDFFormat getRDFFormat() {
		return ServiceProviderHook.JSON_RDR;
	}

}
