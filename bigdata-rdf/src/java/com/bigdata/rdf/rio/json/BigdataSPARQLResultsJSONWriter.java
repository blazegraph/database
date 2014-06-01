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

import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;

/**
 * A TupleQueryResultWriter that writes query results in the <a
 * href="http://www.w3.org/TR/rdf-sparql-json-res/">SPARQL Query Results JSON
 * Format</a>.
 */
public class BigdataSPARQLResultsJSONWriter extends SPARQLJSONWriterBase implements TupleQueryResultWriter {

	/*--------------*
	 * Constructors *
	 *--------------*/

	public BigdataSPARQLResultsJSONWriter(OutputStream out) {
		super(out);
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	public final TupleQueryResultFormat getTupleQueryResultFormat() {
		return TupleQueryResultFormat.JSON;
	}

	@Override
	public TupleQueryResultFormat getQueryResultFormat() {
		return getTupleQueryResultFormat();
	}
	
	@Override
    protected void writeValue(final Value value) throws IOException,
            QueryResultHandlerException {
        
        if (value instanceof BigdataBNode &&
                ((BigdataBNode) value).isStatementIdentifier()) {
            
            writeSid((BigdataBNode) value);
            
        } else {
            
            super.writeValue(value);
            
        }
        
    }
    
    protected void writeSid(final BigdataBNode sid) 
            throws IOException, QueryResultHandlerException {
        
        jg.writeStartObject();

        jg.writeStringField("type", "statement");
        
        final BigdataStatement stmt = sid.getStatement();
        
        jg.writeFieldName("subject");
        writeValue(stmt.getSubject());
        
        jg.writeFieldName("predicate");
        writeValue(stmt.getSubject());
        
        jg.writeFieldName("object");
        writeValue(stmt.getSubject());

        if (stmt.getContext() != null) {
            jg.writeFieldName("context");
            writeValue(stmt.getContext());
        }
        
        jg.writeEndObject();
        
    }
    
}
