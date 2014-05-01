/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.rio.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.RDFParserBase;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * RDF parser for JSON SPARQL Results files that have the variables (s, p, o,
 * and optionally c) in the header.
 */
public class BigdataSPARQLResultsJSONParser extends RDFParserBase {

	protected static final transient Logger log = 
			Logger.getLogger(BigdataSPARQLResultsJSONParser.class);
	
	
	private LineNumberReader lineReader;
	
	private BigdataValueFactory vf;
	
	/**
	 * Default ctor uses a BigdataValueFactory with a namespace of "". Used
	 * for testing.
	 */
	public BigdataSPARQLResultsJSONParser() {
		this(BigdataValueFactoryImpl.getInstance(""));
	}
	
	/**
	 * Construct a parser with the supplied BigdataValueFactory.
	 */
	public BigdataSPARQLResultsJSONParser(final BigdataValueFactory vf) {
		super(vf);
		
		this.vf = vf;
	}
	
	/**
	 * Set the value factory.  Must be a BigdataValueFactory because of the
	 * RDR syntax support.
	 */
	public void setValueFactory(final ValueFactory vf) {
		if (vf instanceof BigdataValueFactory) {
			this.vf = (BigdataValueFactory) vf;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Returns {@link BigdataSPARQLResultsJSONParserFactory#JSON}.
	 */
	@Override
	public RDFFormat getRDFFormat() {
		
		return BigdataSPARQLResultsJSONParserFactory.JSON;
		
	}

	/**
	 * Parse the supplied input stream into RDF.
	 */
	@Override
	public void parse(final InputStream is, final String baseURI) throws IOException,
			RDFParseException, RDFHandlerException {
		
		parse(new InputStreamReader(is), baseURI);
		
	}

	/**
	 * Parse the supplied reader into RDF.
	 */
	@Override
	public void parse(final Reader r, final String baseURI) throws IOException,
			RDFParseException, RDFHandlerException {
		
		lineReader = new LineNumberReader(r);
		// Start counting lines at 1:
		lineReader.setLineNumber(1);
		
		// read graph from JSON in request

		final JsonFactory factory = new JsonFactory();
		
		final JsonParser parser = factory.createJsonParser(lineReader);
		
//		final JsonParser parser = Json.createParser(lineReader);

		JsonToken event = parser.nextToken();
		
		if (event != JsonToken.START_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("head"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.START_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("vars"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.START_ARRAY) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.VALUE_STRING && !(parser.getCurrentName().equals("s"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.VALUE_STRING && !(parser.getCurrentName().equals("p"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.VALUE_STRING && !(parser.getCurrentName().equals("o"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event == JsonToken.VALUE_STRING) {
			
			if (!(parser.getCurrentName().equals("c"))) {
				reportFatalError("unexpected parse event: " + event);
			}
			
			event = parser.nextToken();
			
		}

		if (event != JsonToken.END_ARRAY) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();

		if (event != JsonToken.END_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("results"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.START_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("bindings"))) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();
		
		if (event != JsonToken.START_ARRAY) {
			reportFatalError("unexpected parse event: " + event);
		}
		

//		boolean startingBindings = false;
//		boolean breakLoop = false;
//
//		while (parser.hasNext()) {
//			JsonToken event = parser.nextToken();
//			switch (event) {
//			case START_ARRAY:
//				if (startingBindings)
//					breakLoop = true;
//			case END_ARRAY:
//			case START_OBJECT:
//			case END_OBJECT:
//			case VALUE_FALSE:
//			case VALUE_NULL:
//			case VALUE_TRUE:
//				System.err.println(event.toString());
//				break;
//			case KEY_NAME:
//				if (parser.getString().equals("bindings"))
//					startingBindings = true;
//				System.err.println(event.toString() + " "
//						+ parser.getString());
//				break;
//			case VALUE_STRING:
//			case VALUE_NUMBER:
//				System.err.println(event.toString() + " "
//						+ parser.getString());
//				break;
//			}
//			if (breakLoop)
//				break;
//		}

		rdfHandler.startRDF();

		Statement stmt;
		while ((stmt = parseStatement(parser)) != null) {
		
			if (log.isDebugEnabled()) 
				log.debug(stmt);
		
			rdfHandler.handleStatement(stmt);
			
		}
		
		rdfHandler.endRDF();
			
	}
	
	/**
	 * Parse a statement from the JSON stream.
	 */
	private final BigdataStatement parseStatement(
			final JsonParser parser) 
					throws RDFParseException, JsonParseException, IOException {
		
		JsonToken event = parser.nextToken();
		
		if (event == null || event == JsonToken.END_ARRAY) {
			
			return null;
			
		}
		
		if (event != JsonToken.START_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("s"))) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		final Resource s = (Resource) parseValue(parser);
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("p"))) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		final URI p = (URI) parseValue(parser);
		
		event = parser.nextToken();
		
		if (event != JsonToken.FIELD_NAME && !(parser.getCurrentName().equals("o"))) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		final Value o = parseValue(parser);
		
		event = parser.nextToken();

		switch (event) {
		case END_OBJECT:
			return vf.createStatement(s, p, o);
		case FIELD_NAME:
			if (!(parser.getCurrentName().equals("c"))) {
				reportFatalError("unexpected parse event: " + event);
			}
			final Resource c = (Resource) parseValue(parser);
			event = parser.nextToken();
			if (event != JsonToken.END_OBJECT) {
				reportFatalError("unexpected parse event: " + event);
			}
			return vf.createStatement(s, p, o, c);
		default:
			reportFatalError("unexpected parse event: " + event);
		}
		
		// unreachable code
		return null;
		
	}
	
	/**
	 * Parse a value from the JSON stream.
	 */
	protected Value parseValue(final JsonParser parser) 
					throws RDFParseException, JsonParseException, IOException {
		
		JsonToken event = parser.nextToken();
		
		if (event != JsonToken.START_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("type")) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		event = parser.nextToken();

		if (event != JsonToken.VALUE_STRING) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		final String type = parser.getText();
		
		Value val = null;
		
		if ("sid".equals(type)) {

			val = parseSid(parser);
			
		} else if ("uri".equals(type)) {
			
			val = parseURI(parser);
			
		} else if ("bnode".equals(type)) {

			val = parseBNode(parser);
			
		} else if ("literal".equals(type)) {

			val = parseLiteral(parser);
			
		} else if ("typed-literal".equals(type)) {
			
			val = parseTypedLiteral(parser);
			
		} else {
			
			reportFatalError("unexpected parse event: " + event);
			
		}
		
		event = parser.nextToken();
		
		if (event != JsonToken.END_OBJECT) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		return val;

	}
	
	/**
	 * Parse a sid from the JSON stream.
	 */
	protected Value parseSid(final JsonParser parser) 
			throws RDFParseException, JsonParseException, IOException {
	
		JsonToken event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("value")) {
			reportFatalError("unexpected parse event: " + event);
		}

		final BigdataStatement stmt = parseStatement(parser);
		
		return vf.createBNode(stmt);
	
	}
	
	/**
	 * Parse a URI from the JSON stream.
	 */
	protected Value parseURI(final JsonParser parser) 
			throws RDFParseException, JsonParseException, IOException {
	
		JsonToken event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("value")) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();

		if (event != JsonToken.VALUE_STRING) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		return vf.createURI(parser.getText());
	
	}
	
	/**
	 * Parse a bnode from the JSON stream.
	 */
	protected Value parseBNode(final JsonParser parser) 
			throws RDFParseException, JsonParseException, IOException {
	
		JsonToken event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("value")) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();

		if (event != JsonToken.VALUE_STRING) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		return vf.createBNode(parser.getText());
	
	}
	
	/**
	 * Parse a plain literal or language-tagged literal from the JSON stream.
	 */
	protected Value parseLiteral(final JsonParser parser) 
			throws RDFParseException, JsonParseException, IOException {
	
		JsonToken event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME)
			reportFatalError("unexpected parse event: " + event);
		
		if (parser.getCurrentName().equals("xml:lang")) {
			
			event = parser.nextToken();

			if (event != JsonToken.VALUE_STRING) {
				reportFatalError("unexpected parse event: " + event);
			}

			final String lang = parser.getText();

			event = parser.nextToken();
			
			if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("value")) {
				reportFatalError("unexpected parse event: " + event);
			}

			event = parser.nextToken();

			if (event != JsonToken.VALUE_STRING) {
				reportFatalError("unexpected parse event: " + event);
			}
			
			return vf.createLiteral(parser.getText(), lang);
			
		} else if (parser.getCurrentName().equals("value")) {
			
			event = parser.nextToken();
	
			if (event != JsonToken.VALUE_STRING) {
				reportFatalError("unexpected parse event: " + event);
			}
			
			return vf.createLiteral(parser.getText());
			
		} else {
			
			reportFatalError("unexpected parse event: " + event);
			
			// unreachable code
			return null;

		}
	
	}
	
	/**
	 * Parse a typed literal from the JSON stream.
	 */
	protected Value parseTypedLiteral(final JsonParser parser) 
			throws RDFParseException, JsonParseException, IOException {
	
		JsonToken event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("datatype")) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();

		if (event != JsonToken.VALUE_STRING) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		final URI datatype = vf.createURI(parser.getText());
		
		event = parser.nextToken();

		if (event != JsonToken.FIELD_NAME && !parser.getCurrentName().equals("value")) {
			reportFatalError("unexpected parse event: " + event);
		}

		event = parser.nextToken();

		if (event != JsonToken.VALUE_STRING) {
			reportFatalError("unexpected parse event: " + event);
		}
		
		return vf.createLiteral(parser.getText(), datatype);
	
	}
	
	/**
	 * Overrides {@link RDFParserBase#reportFatalError(String)}, adding line
	 * number information to the error.
	 */
	@Override
	protected void reportFatalError(String msg) throws RDFParseException {
	
		reportFatalError(msg, lineReader.getLineNumber(), -1);
		
	}

	
}
