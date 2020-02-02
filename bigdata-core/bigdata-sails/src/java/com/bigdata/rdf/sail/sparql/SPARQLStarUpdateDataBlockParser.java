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
package com.bigdata.rdf.sail.sparql;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.helpers.SPARQLUpdateDataBlockParser;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * An extension of {@link SPARQLUpdateDataBlockParser} that processes data in the format
 * specified in the SPARQL* grammar. This format is almost completely compatible with
 * SPARQLUpdateDataBlockParser, except:
 * <ul>
 * <li>it allows statement to be used as subject or object in another statement
 * </ul>
 * 
 * @author Igor Kim (igor.kim@ms2w.com)
 * @openrdf
 */
public class SPARQLStarUpdateDataBlockParser extends SPARQLUpdateDataBlockParser {

	private String baseURI;

	/**
	 * Namespaces mapping, collected from PREFIX statements, should be passed in to recursive SPARQL* parsing.
	 * Original namespaceTable variable is private to {@link RDFParserBase} and could not be accessed from this class.
	 */
	private Map<String, String> namespaceTable;

	/*--------------*
	 * Constructors *
	 *--------------*/

	/**
	 * Creates a new parser that will use the supplied ValueFactory to create RDF
	 * model objects.
	 * 
	 * @param valueFactory
	 *        A ValueFactory.
	 */
	public SPARQLStarUpdateDataBlockParser(ValueFactory valueFactory) {
		super(valueFactory);
		this.namespaceTable = new HashMap<>();
	}

	/**
	 * Creates a new parser that will use the supplied ValueFactory and prefix mapping to create RDF
	 * model objects.
	 * 
	 * @param valueFactory
	 *        A ValueFactory.
	 * @param namespaces
	 *        Namespaces prefix mapping.
	 */
	public SPARQLStarUpdateDataBlockParser(ValueFactory valueFactory, Map<String, String> namespaces) {
		super(valueFactory);
		this.namespaceTable = namespaces;
		// fill in original {@link RDFParserBase}.namespaceTable to be used while parsing
		for (Entry<String, String> entry: namespaceTable.entrySet()) {
			super.setNamespace(entry.getKey(), entry.getValue());
		}
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected Value parseValue() throws IOException, RDFParseException {
		if (checkSparqlStarSyntax()) {
			return parseStmtValue();
		}
		try {
			return super.parseValue();
		} catch (RDFHandlerException e) {
			throw new IOException(e);
		}
	}
	
	private boolean checkSparqlStarSyntax() throws IOException {
		int c1 = read();
		int c2;
		try {
			c2 = read();
			unread(c2);
		} finally {
			unread(c1);
		}
		return (c1 == '<' && c2 == '<');
	}

	private Value parseStmtValue() throws IOException, RDFParseException
	{
		StringBuilder stmtBuf = new StringBuilder(100);

		// First 2 characters should be '<'
		int c = read();
		verifyCharacterOrFail(c, "<");
		c = read();
		verifyCharacterOrFail(c, "<");
		int recursiveCounter = 1;

		// Read up to the next ">>" characters combination
		while (true) {
			c = read();
			int c2 = peek();

			if (c == '<' && c2 =='<' ) {
				recursiveCounter++;
			} else  if (c == '>' && c2 == '>') {
				if (--recursiveCounter == 0) {
					c = read();
					break;
				}
			} else if (c == -1) {
				throwEOFException();
			}

			stmtBuf.append((char)c);

			if (c == '\\') {
				// This escapes the next character, which might be a '>'
				c = read();
				if (c == -1) {
					throwEOFException();
				}
				if (c != 'u' && c != 'U') {
					reportFatalError("IRI includes string escapes: '\\" + c + "'");
				}
				stmtBuf.append((char)c);
			}
		}

		// Use our own class in recursion.
		SPARQLStarUpdateDataBlockParser p = new SPARQLStarUpdateDataBlockParser(valueFactory, namespaceTable);
        final List<Statement> stmts = new LinkedList<Statement>();
        final StatementCollector sc = new StatementCollector(stmts);
		p.setRDFHandler(sc);
        p.setParserConfig(getParserConfig());
        try {
			p.parse(new StringReader(stmtBuf.toString()), baseURI);
		} catch (RDFHandlerException e) {
			throw new RDFParseException("Error parsing SPARQL* value", e);
		}
        
        if (stmts.size() != 1) {
        	throw new RDFParseException("Error parsing SPARQL* value, invalid number of statements");
        }
        if (valueFactory instanceof BigdataValueFactory && stmts.get(0) instanceof BigdataStatement) {
        	return ((BigdataValueFactory)valueFactory).createBNode((BigdataStatement)stmts.get(0));
        } else {
        	throw new RDFParseException("Error parsing SPARQL* value, incompatible valueFactory");
        }
	}	

	/**
	 * Overriding setBaseURI to keep base URI value for SPARQL* parsing as
	 * org.openrdf.rio.helpers.RDFParserBase.baseURI is a private field
	 */
	@Override
	protected void setBaseURI(String uri) {
		baseURI = uri;
		super.setBaseURI(uri);
	}

	@Override
	public void setNamespace(String prefix, String namespace) {
		namespaceTable.put(prefix, namespace);
		super.setNamespace(prefix, namespace);
	}

	/**
	 * BC method.
	 */
	protected int read() throws IOException {
		return readCodePoint();
	}

	/**
	 * BC method.
	 */
	protected int peek() throws IOException {
		return peekCodePoint();
	}
}
