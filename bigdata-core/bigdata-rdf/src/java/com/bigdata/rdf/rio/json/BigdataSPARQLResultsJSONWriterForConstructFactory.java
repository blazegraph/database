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

import java.io.OutputStream;
import java.io.Writer;

import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

import com.bigdata.rdf.ServiceProviderHook;


/**
 * A {@link TupleQueryResultWriterFactory} for writers of SPARQL/JSON query
 * results.
 * 
 * @author Arjohn Kampman
 */
public class BigdataSPARQLResultsJSONWriterForConstructFactory implements RDFWriterFactory {

//    public static final RDFFormat JSON = new RDFFormat("N-Triples", "text/plain",
//            Charset.forName("US-ASCII"), "nt", NO_NAMESPACES, NO_CONTEXTS);
    
    @Override
    public RDFFormat getRDFFormat() {
    	return ServiceProviderHook.JSON_RDR;
	}

    @Override
    public RDFWriter getWriter(final Writer writer) {
        return new BigdataSPARQLResultsJSONWriterForConstruct(writer);
    }

    @Override
    public RDFWriter getWriter(final OutputStream out) {
        return new BigdataSPARQLResultsJSONWriterForConstruct(out);
    }
    
}
