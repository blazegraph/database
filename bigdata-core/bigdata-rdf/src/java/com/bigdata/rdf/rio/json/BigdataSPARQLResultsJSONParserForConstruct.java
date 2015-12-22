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
import java.io.InputStream;
import java.io.Reader;
import java.util.List;

import org.apache.commons.io.input.ReaderInputStream;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFParserBase;

import com.bigdata.rdf.ServiceProviderHook;

/**
 * Parser for SPARQL-1.1 JSON Results Format documents
 * 
 * @see <a href="http://www.w3.org/TR/sparql11-results-json/">SPARQL 1.1 Query
 *      Results JSON Format</a>
 * @author Peter Ansell
 */
public class BigdataSPARQLResultsJSONParserForConstruct extends RDFParserBase 
        implements RDFParser, TupleQueryResultHandler {

    private final BigdataSPARQLResultsJSONParser parser;
    
    public BigdataSPARQLResultsJSONParserForConstruct() {
        this.parser = new BigdataSPARQLResultsJSONParser();
    }
    
    public BigdataSPARQLResultsJSONParserForConstruct(final ValueFactory vf) {
        this.parser = new BigdataSPARQLResultsJSONParser(vf);
        this.parser.setQueryResultHandler(this);
    }
    
    @Override
    public RDFFormat getRDFFormat() {
        return ServiceProviderHook.JSON_RDR;
    }

    @Override
    public void parse(InputStream in, String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {
        try {
            parser.parseQueryResult(in);
        } catch (QueryResultParseException e) {
            throw new RDFParseException(e);
        } catch (QueryResultHandlerException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void parse(Reader reader, String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {
        parse(new ReaderInputStream(reader), baseURI);
    }

    @Override
    public void handleBoolean(boolean value) throws QueryResultHandlerException {
        // do nothing
    }

    @Override
    public void handleLinks(List<String> linkUrls)
            throws QueryResultHandlerException {
        // do nothing
    }

    @Override
    public void startQueryResult(List<String> bindingNames)
            throws TupleQueryResultHandlerException {
        try {
            getRDFHandler().startRDF();
        } catch (RDFHandlerException e) {
            throw new TupleQueryResultHandlerException(e);
        }
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        try {
            getRDFHandler().endRDF();
        } catch (RDFHandlerException e) {
            throw new TupleQueryResultHandlerException(e);
        }
    }

    @Override
    public void handleSolution(BindingSet bs)
            throws TupleQueryResultHandlerException {
        
        if (!bs.hasBinding("subject")) {
            throw new TupleQueryResultHandlerException("no subject: " + bs);
        }
        if (!bs.hasBinding("predicate")) {
            throw new TupleQueryResultHandlerException("no predicate: " + bs);
        }
        if (!bs.hasBinding("object")) {
            throw new TupleQueryResultHandlerException("no object: " + bs);
        }
        
        final Resource s = (Resource) bs.getValue("subject");
        final URI p = (URI) bs.getValue("predicate");
        final Value o = (Value) bs.getValue("object");
        final Resource c = bs.hasBinding("context") ? 
                (Resource) bs.getBinding("context") : null;
        
        final Statement stmt = valueFactory.createStatement(s, p, o, c);
        
        try {
            getRDFHandler().handleStatement(stmt);
        } catch (RDFHandlerException e) {
            throw new TupleQueryResultHandlerException(e);
        }
                
    }

    
    
}
