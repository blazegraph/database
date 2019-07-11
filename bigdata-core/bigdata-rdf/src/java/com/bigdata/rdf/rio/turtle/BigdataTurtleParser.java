/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.turtle;

import info.aduna.text.ASCIIUtil;

import java.io.IOException;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.rio.turtle.TurtleUtil;

import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * RDF parser for <a href="http://www.dajobe.org/2004/01/turtle/">Turtle</a>
 * files. This parser is not thread-safe, therefore its public methods are
 * synchronized.
 * <p>
 * This implementation is based on the 2006/01/02 version of the Turtle
 * specification, with slight deviations:
 * <ul>
 * <li>Normalization of integer, floating point and boolean values is dependent
 * on the specified datatype handling. According to the specification, integers
 * and booleans should be normalized, but floats don't.</li>
 * <li>Comments can be used anywhere in the document, and extend to the end of
 * the line. The Turtle grammar doesn't allow comments to be used inside triple
 * constructs that extend over multiple lines, but the author's own parser
 * deviates from this too.</li>
 * </ul>
 * 
 * @author Arjohn Kampman
 * @openrdf
 */
public class BigdataTurtleParser extends TurtleParser {

	/**
	 * Standalone variant of the parser, should be used only
	 * when connection is not available.
	 */
	public BigdataTurtleParser() {
		super(BigdataValueFactoryImpl.getInstance(""));
	}
	
    /**
     * Parses an RDF value. This method parses uriref, qname, node ID, quoted
     * literal, integer, double and boolean.
     */
    protected Value parseValue()
        throws IOException, RDFParseException
    {
        int c = peek();

        if (c == '<') {
            // uriref, e.g. <foo://bar>
            return parseURIOrSid();
        }
        else if (c == ':' || TurtleUtil.isPrefixStartChar(c)) {
            // qname or boolean
            return parseQNameOrBoolean();
        }
        else if (c == '_') {
            // node ID, e.g. _:n1
            return parseNodeID();
        }
        else if (c == '"' || c == '\'') {
            // quoted literal, e.g. "foo" or """foo""" or 'foo' or '''foo'''
            try {
                return parseQuotedLiteral();
            } catch (RDFHandlerException e) {
                throw new IOException(e);
            }
        }
        else if (ASCIIUtil.isNumber(c) || c == '.' || c == '+' || c == '-') {
            // integer or double, e.g. 123 or 1.2e3
            return parseNumber();
        }
        else if (c == -1) {
            throwEOFException();
            return null;
        }
        else {
            reportFatalError("Expected an RDF value here, found '" + (char)c + "'");
            return null;
        }
    }


    protected Value parseURIOrSid()
        throws IOException, RDFParseException
    {
        // First character should be '<'
        int c = read();
        verifyCharacterOrFail(c, "<");
        
        int n = peek();
        if (n == '<') {
            read();
            if (this.valueFactory == null) {
                reportFatalError("must use a BigdataValueFactory to use the RDR syntax");
            }
            return parseSid();
        } else {
            unread(c);
            return parseURI();
        }
    }
    
    protected Value parseSid()
            throws IOException, RDFParseException
    {
        Resource s = (Resource) parseValue();
        
        skipWS();
        URI p = (URI) parseValue();
        
        skipWS();
        Value o = parseValue();

        int i = read();
        while (TurtleUtil.isWhitespace(i)) {
            i = read();
        }
        
        if (i == '>' && read() == '>') {
            if (valueFactory == null || 
                    valueFactory instanceof BigdataValueFactory == false) {
                /*
                 * The BigdataValueFactory has an extension to create a BNode
                 * from a Statement.  You need to specify that value factory
                 * when you create the parser using setValueFactory().
                 */
                throw new RDFParseException(
                        "You must use a BigdataValueFactory to use the RDR syntax");
            }
            
            try {
            	// write the RDR statement
            	reportStatement(s, p, o);
            } catch (RDFHandlerException ex) {
            	throw new IOException(ex);
            }
            
            final BigdataValueFactory valueFactory = (BigdataValueFactory)
                    super.valueFactory;
            
            return valueFactory.createBNode(
                    valueFactory.createStatement(s, p, o));
        } else {
            reportFatalError("expecting >> to close statement identifier");
            throw new IOException("expecting >> to close statement identifier");
        }
        
    }

    /**
     * Consumes any white space characters (space, tab, line feed, newline) and
     * comments (#-style) from <tt>reader</tt>. After this method has been
     * called, the first character that is returned by <tt>reader</tt> is either
     * a non-ignorable character, or EOF. For convenience, this character is also
     * returned by this method.
     * 
     * @return The next character that will be returned by <tt>reader</tt>.
     */
    protected int skipWS()
        throws IOException
    {
        int c = read();
        while (TurtleUtil.isWhitespace(c)) {
            c = read();
        }

        unread(c);

        return c;
    }
    
    /**
     * Parses a blank node ID, e.g. <tt>_:node1</tt>.
     */
    protected BNode parseNodeID()
        throws IOException, RDFParseException
    {
        // Node ID should start with "_:"
        verifyCharacterOrFail(read(), "_");
        verifyCharacterOrFail(read(), ":");

        // Read the node ID
        int c = read();
        if (c == -1) {
            throwEOFException();
        }
//        modified to allow fully numeric bnode ids 
//        else if (!TurtleUtil.isNameStartChar(c)) {
//            reportError("Expected a letter, found '" + (char)c + "'", BasicParserSettings.PRESERVE_BNODE_IDS);
//        }

        StringBuilder name = new StringBuilder(32);
        name.append((char)c);

        // Read all following letter and numbers, they are part of the name
        c = read();

        // If we would never go into the loop we must unread now
        if (!TurtleUtil.isNameChar(c)) {
            unread(c);
        }

        while (TurtleUtil.isNameChar(c)) {
            int previous = c;
            c = read();
            
            if (previous == '.' && (c == -1 || TurtleUtil.isWhitespace(c) || c == '<' || c == '_')) {
                unread(c);
                unread(previous);
                break;
            }
            name.append((char)previous);
            if(!TurtleUtil.isNameChar(c))
            {
                unread(c);
            }
        }

        return createBNode(name.toString());
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
