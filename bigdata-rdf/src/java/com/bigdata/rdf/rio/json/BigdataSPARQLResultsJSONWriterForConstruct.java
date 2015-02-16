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
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;

import org.openrdf.model.Statement;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.WriterConfig;

import com.bigdata.rdf.ServiceProviderHook;

/**
 * A TupleQueryResultWriter that writes query results in the <a
 * href="http://www.w3.org/TR/rdf-sparql-json-res/">SPARQL Query Results JSON
 * Format</a>.
 */
public class BigdataSPARQLResultsJSONWriterForConstruct implements RDFWriter {

    private final BigdataSPARQLResultsJSONWriter writer;
    
	/*--------------*
	 * Constructors *
	 *--------------*/

	public BigdataSPARQLResultsJSONWriterForConstruct(final Writer writer) {
		this.writer = new BigdataSPARQLResultsJSONWriter(writer);
	}

    public BigdataSPARQLResultsJSONWriterForConstruct(final OutputStream out) {
        this.writer = new BigdataSPARQLResultsJSONWriter(out);
    }

    /*---------*
     * Methods *
     *---------*/

    @Override
    public RDFFormat getRDFFormat() {
    	return ServiceProviderHook.JSON_RDR;
    }


    @Override
    public void startRDF() throws RDFHandlerException {
        try {
            writer.startDocument();
            writer.startHeader();
            writer.startQueryResult(Arrays.asList(new String[] {
                    "subject", "predicate", "object", "context"
            }));
            writer.endHeader();
        } catch (QueryResultHandlerException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        try {
            writer.endQueryResult();
//            writer.endDocument();
//        } catch (IOException e) {
//            throw new RDFHandlerException(e);
        } catch (TupleQueryResultHandlerException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void handleNamespace(String prefix, String uri)
            throws RDFHandlerException {
        try {
            writer.handleNamespace(prefix, uri);
        } catch (QueryResultHandlerException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("subject", st.getSubject());
        bs.addBinding("predicate", st.getPredicate());
        bs.addBinding("object", st.getObject());
        if (st.getContext() != null)
            bs.addBinding("context", st.getContext());
        try {
            writer.handleSolution(bs);
        } catch (TupleQueryResultHandlerException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        // do nothing        
    }

    @Override
    public void setWriterConfig(WriterConfig config) {
        writer.setWriterConfig(config);
    }

    @Override
    public WriterConfig getWriterConfig() {
        return writer.getWriterConfig();
    }

    @Override
    public Collection<RioSetting<?>> getSupportedSettings() {
        return writer.getSupportedSettings();
    }

}
