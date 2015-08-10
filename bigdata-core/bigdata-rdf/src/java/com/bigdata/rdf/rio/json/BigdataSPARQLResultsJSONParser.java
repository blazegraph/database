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
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultFormat;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Parser for SPARQL-1.1 JSON Results Format documents
 * 
 * @see <a href="http://www.w3.org/TR/sparql11-results-json/">SPARQL 1.1 Query
 *      Results JSON Format</a>
 * @author Peter Ansell
 */
public class BigdataSPARQLResultsJSONParser extends SPARQLJSONParserBase implements TupleQueryResultParser {

    public static final String SID = "sid";

    public static final String SUBJECT = "subject";

    public static final String PREDICATE = "predicate";

    public static final String OBJECT = "object";

    public static final String CONTEXT = "context";

	/**
	 * Default constructor.
	 */
	public BigdataSPARQLResultsJSONParser() {
		super();
	}

	/**
	 * Construct a parser with a specific {@link ValueFactory}.
	 * 
	 * @param valueFactory
	 *        The factory to use to create values.
	 */
	public BigdataSPARQLResultsJSONParser(ValueFactory valueFactory) {
		super(valueFactory);
	}

	@Override
	public QueryResultFormat getQueryResultFormat() {
		return getTupleQueryResultFormat();
	}

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return TupleQueryResultFormat.JSON;
	}

	@Override
	@Deprecated
	public void setTupleQueryResultHandler(TupleQueryResultHandler handler) {
		setQueryResultHandler(handler);
	}

	@Override
	@Deprecated
	public void parse(InputStream in)
		throws IOException, QueryResultParseException, TupleQueryResultHandlerException
	{
		try {
			parseQueryResultInternal(in, false, true);
		}
		catch (TupleQueryResultHandlerException e) {
			throw e;
		}
		catch (QueryResultHandlerException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}
	
    protected Value parseValue(final String bindingStr, final JsonParser jp) 
            throws QueryResultParseException, JsonParseException, IOException {
        
        String lang = null;
        String type = null;
        String datatype = null;
        String value = null;
        
        // added for Sids support
        final Map<String, Value> sid = new LinkedHashMap<String, Value>();

        while (jp.nextToken() != JsonToken.END_OBJECT) {

            if (jp.getCurrentToken() != JsonToken.FIELD_NAME) {
                throw new QueryResultParseException("Did not find value attribute under "
                        + bindingStr + " field", jp.getCurrentLocation().getLineNr(),
                        jp.getCurrentLocation().getColumnNr());
            }
            String fieldName = jp.getCurrentName();

            // move to the value token
            jp.nextToken();
            
            // set the appropriate state variable
            if (TYPE.equals(fieldName)) {
                type = jp.getText();
            }
            else if (XMLLANG.equals(fieldName)) {
                lang = jp.getText();
            }
            else if (DATATYPE.equals(fieldName)) {
                datatype = jp.getText();
            }
            else if (VALUE.equals(fieldName)) {
                value = jp.getText();
            }
            // added for Sids support
            else if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
                sid.put(fieldName, parseValue(bindingStr, jp));
            }
            else {
                throw new QueryResultParseException("Unexpected field name: " + fieldName,
                        jp.getCurrentLocation().getLineNr(),
                        jp.getCurrentLocation().getColumnNr());

            }
        }
        
        // added for Sids support
        if (type.equals(SID)) {
            
            final Resource s = (Resource) sid.get(SUBJECT);
            final URI p = (URI) sid.get(PREDICATE);
            final Value o = (Value) sid.get(OBJECT);
            final Resource c = (Resource) sid.get(CONTEXT);
            
            if (s == null) {
                throw new QueryResultParseException("Missing subject for statement: " + bindingStr,
                        jp.getCurrentLocation().getLineNr(),
                        jp.getCurrentLocation().getColumnNr());
            }
            
            if (p == null) {
                throw new QueryResultParseException("Missing predicate for statement: " + bindingStr,
                        jp.getCurrentLocation().getLineNr(),
                        jp.getCurrentLocation().getColumnNr());
            }
            
            if (o == null) {
                throw new QueryResultParseException("Missing object for statement: " + bindingStr,
                        jp.getCurrentLocation().getLineNr(),
                        jp.getCurrentLocation().getColumnNr());
            }
            
            final BigdataValueFactory valueFactory = 
                    (BigdataValueFactory) super.valueFactory;
            
            final BigdataStatement stmt = valueFactory.createStatement(s, p, o, c);
            
            return valueFactory.createBNode(stmt);
            
        }

        return parseValue(type, value, lang, datatype);
        
    }
    

}
