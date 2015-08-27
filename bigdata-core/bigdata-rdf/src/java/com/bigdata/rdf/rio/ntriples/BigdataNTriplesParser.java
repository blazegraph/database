/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.ntriples;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.io.input.BOMInputStream;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.ntriples.NTriplesUtil;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * RDF parser for N-Triples files. A specification of NTriples can be found in
 * <a href="http://www.w3.org/TR/rdf-testcases/#ntriples">this section</a> of
 * the RDF Test Cases document. This parser is not thread-safe, therefore its
 * public methods are synchronized.
 * <p>
 * This parser has been modified to support the inline notation for statements
 * about statements.
 * <p>
 * This parser has been modified to reuse the same {@link StringBuilder} in
 * order to minimize heap churn.
 * <p>
 * This parser has been modified to permit "-" and "_" in blank node IDs (they
 * are not allowed in that position for NTRIPLES). This was done to support a
 * demonstration use case. That change could (and should) be backed out. It is
 * documented by FIXMEs in the code. One of the test files would also have to be
 * fixed.
 * 
 * @author Arjohn Kampman
 * @author Bryan Thompson
 * @openrdf
 */
public class BigdataNTriplesParser extends RDFParserBase {

	/*-----------*
	 * Variables *
	 *-----------*/

	private PushbackReader reader;

	private int lineNo;

	private ValueFactory valueFactory;
	
	/**
	 * LRU collection of embedded statements and their associated blank nodes.
	 */
	private Map<Statement, BigdataBNode> sids = new LinkedHashMap<Statement, BigdataBNode>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(
				final Map.Entry<Statement, BigdataBNode> eldest) {

			return size() > 100;
			
		}
	};
	
	static private class State {

		private Resource subject;

		private URI predicate;

		private Value object;
		
		/**
		 * The SID corresponding to the most recently parsed embedded statement.
		 */
		private BigdataBNode lastSID;
		
	};
	
	private final Stack<State> stack = new Stack<State>();

	private void push(final State state) {
		stack.add(state);
	}
	private State pop() {
		return stack.pop();
	}
	private State peek() {
		return stack.peek();
	}
	
//	/**
//	 * Return a buffer of zero length and non-zero capacity. The same buffer is
//	 * reused for each thing which is parsed. This reduces the heap churn
//	 * substantially. However, you have to watch out for side-effects and
//	 * convert the buffer to a {@link String} before the buffer is reused.
//	 * 
//	 * @param capacityIsIgnored
//
//	 * @return
//	 */
//	private StringBuilder getBuffer() {
//		buffer.setLength(0);
//		return buffer;
//	}
//
//	private final StringBuilder buffer = new StringBuilder(100);
//
//	private StringBuilder getLanguageTagBuffer() {
//		languageTagBuffer.setLength(0);
//		return languageTagBuffer;
//	}
//
//	private final StringBuilder languageTagBuffer = new StringBuilder(8);
//
//	private StringBuilder getDatatypeUriBuffer() {
//		datatypeUriBuffer.setLength(0);
//		return datatypeUriBuffer;
//	}
//
//	private final StringBuilder datatypeUriBuffer = new StringBuilder(40);
	
	/*--------------*
	 * Constructors *
	 *--------------*/

	/**
	 * Creates a new NTriplesParser that will use a {@link ValueFactoryImpl} to
	 * create object for resources, bNodes and literals.
	 */
	public BigdataNTriplesParser() {
		// We are providing Bigdata-specific value factory to support parsing of RDR,
		// which require BigdataValueFactory instead of default Sesame implementation
		// See https://jira.blazegraph.com/browse/BLZG-1322
		super(BigdataValueFactoryImpl.getInstance(""));
	}

	/**
	 * Creates a new NTriplesParser that will use the supplied
	 * <tt>ValueFactory</tt> to create RDF model objects.
	 * 
	 * @param valueFactory
	 *        A ValueFactory.
	 */
	public BigdataNTriplesParser(BigdataValueFactory valueFactory) {
		super(valueFactory);
	}

	public void setValueFactory(final ValueFactory valueFactory) {
		super.setValueFactory(valueFactory);
		this.valueFactory = valueFactory;
	}

	/**
	 * Return the {@link BigdataValueFactory}.
	 * 
	 * @throws ClassCastException
	 *             if you have not set a {@link BigdataValueFactory}.
	 */
	protected BigdataValueFactory getValueFactory() {
		return (BigdataValueFactory) valueFactory;
	}

	/*---------*
	 * Methods *
	 *---------*/

	// implements RDFParser.getRDFFormat()
	public final RDFFormat getRDFFormat() {
		return RDFFormat.NTRIPLES;
	}

    /**
     * Implementation of the <tt>parse(InputStream, String)</tt> method defined
     * in the RDFParser interface.
     * 
     * @param in
     *        The InputStream from which to read the data, must not be
     *        <tt>null</tt>. The InputStream is supposed to contain 7-bit
     *        US-ASCII characters, as per the N-Triples specification.
     * @param baseURI
     *        The URI associated with the data in the InputStream, must not be
     *        <tt>null</tt>.
     * @throws IOException
     *         If an I/O error occurred while data was read from the InputStream.
     * @throws RDFParseException
     *         If the parser has found an unrecoverable parse error.
     * @throws RDFHandlerException
     *         If the configured statement handler encountered an unrecoverable
     *         error.
     * @throws IllegalArgumentException
     *         If the supplied input stream or base URI is <tt>null</tt>.
     */
    @Override
    public synchronized void parse(InputStream in, String baseURI)
        throws IOException, RDFParseException, RDFHandlerException
    {
        if (in == null) {
            throw new IllegalArgumentException("Input stream can not be 'null'");
        }
        // Note: baseURI will be checked in parse(Reader, String)

        try {
            parse(new InputStreamReader(new BOMInputStream(in, false), "US-ASCII"), baseURI);
        }
        catch (UnsupportedEncodingException e) {
            // Every platform should support the US-ASCII encoding...
            throw new RuntimeException(e);
        }
    }

    /**
     * Implementation of the <tt>parse(Reader, String)</tt> method defined in
     * the RDFParser interface.
     * 
     * @param reader
     *        The Reader from which to read the data, must not be <tt>null</tt>.
     * @param baseURI
     *        The URI associated with the data in the Reader, must not be
     *        <tt>null</tt>.
     * @throws IOException
     *         If an I/O error occurred while data was read from the InputStream.
     * @throws RDFParseException
     *         If the parser has found an unrecoverable parse error.
     * @throws RDFHandlerException
     *         If the configured statement handler encountered an unrecoverable
     *         error.
     * @throws IllegalArgumentException
     *         If the supplied reader or base URI is <tt>null</tt>.
     */
    public synchronized void parse(final Reader reader, final String baseURI)
        throws IOException, RDFParseException, RDFHandlerException
    {
        if (reader == null) {
            throw new IllegalArgumentException("Reader can not be 'null'");
        }
        if (baseURI == null) {
            throw new IllegalArgumentException("base URI can not be 'null'");
        }

        rdfHandler.startRDF();

        // We need pushback for '<<' versus '<'.
        this.reader = new PushbackReader(reader, 1/* size */);
        lineNo = 1;

        reportLocation(lineNo, 1);
        push(new State());
        try {
            int c = reader.read();
            c = skipWhitespace(c);

            while (c != -1) {
                if (c == '#') {
                    // Comment, ignore
                    c = skipLine(c);
                }
                else if (c == '\r' || c == '\n') {
                    // Empty line, ignore
                    c = skipLine(c);
                }
                else {
                    c = parseTriple(c, false/* embedded */);
                }

                c = skipWhitespace(c);
            }
        }
        finally {
            clear();
        }

        rdfHandler.endRDF();
    }

    /**
     * Reads characters from reader until it finds a character that is not a
     * space or tab, and returns this last character. In case the end of the
     * character stream has been reached, -1 is returned.
     */
    protected int skipWhitespace(int c)
        throws IOException
    {
        while (c == ' ' || c == '\t') {
            c = reader.read();
        }

        return c;
    }

    /**
     * Verifies that there is only whitespace until the end of the line.
     */
    protected int assertLineTerminates(int c)
        throws IOException, RDFParseException
    {
        c = reader.read();

        c = skipWhitespace(c);

        if (c != -1 && c != '\r' && c != '\n') {
            reportFatalError("Content after '.' is not allowed");
        }

        return c;
    }

    /**
     * Reads characters from reader until the first EOL has been read. The first
     * character after the EOL is returned. In case the end of the character
     * stream has been reached, -1 is returned.
     */
    protected int skipLine(int c)
        throws IOException
    {
        while (c != -1 && c != '\r' && c != '\n') {
            c = reader.read();
        }

        // c is equal to -1, \r or \n. In case of a \r, we should
        // check whether it is followed by a \n.

        if (c == '\n') {
            c = reader.read();

            lineNo++;

            reportLocation(lineNo, 1);
        }
        else if (c == '\r') {
            c = reader.read();

            if (c == '\n') {
                c = reader.read();
            }

            lineNo++;

            reportLocation(lineNo, 1);
        }

        return c;
    }

	private int parseTriple(int c,final boolean embedded)
		throws IOException, RDFParseException, RDFHandlerException
	{
		c = parseSubject(c);

		c = skipWhitespace(c);

		c = parsePredicate(c);

		c = skipWhitespace(c);

		c = parseObject(c);

		c = skipWhitespace(c);

		if (c == -1) {
			throwEOFException();
		}
		else if(embedded) {
			// Embedded.
			if (c != '>')
				reportFatalError("Expected '>', found: " + (char) c);
			c = reader.read();
			if (c != '>')
				reportFatalError("Expected '>', found: " + (char) c);
			// eat the >> and then skip to the next whitespace on the
			// same line.
			c = skipWhitespace(reader.read());
		} else {
			// Non-embedded.
			if (c != '.') {
				reportFatalError("Expected '.', found: " + (char) c);
			}
			c = skipLine(c);
		}

		final State state = peek();
		if (embedded) {
			// Create statement.
			BigdataStatement st = (BigdataStatement) createStatement(
					state.subject, state.predicate, state.object);
			
			// add the RDR statement inside the << >>.
			rdfHandler.handleStatement(st);
			
			state.lastSID = ((BigdataValueFactory) valueFactory).createBNode(st);
			
//			// Resolve against LRU map to blank node for statement.
//			BigdataBNode sid = sids.get(st);
//			if (sid != null) {
//				state.lastSID = sid;
//			} else {
//				/*
//				 * Not found.
//				 * 
//				 * TODO The use of the sid bnode in the context position should
//				 * go away when we migrate to sids support in both triples and
//				 * quads mode.
//				 */
//				// New blank node for "sid" of this statement.
//				state.lastSID = sid = (BigdataBNode) createBNode();
//				// New statement using that "sid" as its context position.
//				st = getValueFactory().createStatement(state.subject,
//						state.predicate, state.object, sid);
//				// cache it.
//				sids.put(st,sid);
//				// mark this blank node as a "sid".
//				// st.setStatementIdentifier(true);
//				((BigdataBNodeImpl) sid).setStatement(st);
//				// new statement so pass to the call back interface.
//				rdfHandler.handleStatement(st);
//			}
			
		} else {
			// simple statement (original code path).
			final Statement st = createStatement(
					state.subject, state.predicate, state.object);
			rdfHandler.handleStatement(st);
		}

//		state.clear();
//		subject = null;
//		predicate = null;
//		object = null;

		return c;
	}

	/**
	 * Return <code>true</code> if the next character is &lt;. This should only
	 * be invoked when the current character is known to be &lt;. It provides
	 * one character lookahead to differentiate between a URI and a Statement.
	 * For example, an embedded Statement in the subject position of another
	 * statement looks like this:
	 * 
	 * <pre>
	 * <<<http://dbpedia.org/resource/Anarchism> <http://dbpedia.org/property/v> "no"@en>> <http://example.com/rtst/absolute-line> "288" .
	 * </pre>
	 */
	private boolean isStatement(int c) throws RDFParseException, IOException {
		assert c == '<' : "Supplied char should be a '<', is: " + c;
		c = reader.read();
		if (c == -1) {
			throwEOFException();
		}
		reader.unread(c);
		return c == '<';
	}
	
	private int parseSubject(int c)
		throws IOException, RDFParseException, RDFHandlerException
	{
		final State state = peek();
		// subject is either an uriref (<foo://bar>) or a nodeID (_:node1)
		// OR a Statement.
		if (c == '<') {
			if (isStatement(c)) {
				// Embedded statement.
				c = reader.read(); // known '<'
				if (c != '<')
					reportFatalError("Expected '<', found: " + (char) c);
				// have '<<', so this is an embedded statement.
				c = reader.read(); // next character.
				c = skipWhitespace(c); // skip any WS characters.
				push(new State());
				c = parseTriple(c, true/* embedded */);
				state.subject = pop().lastSID;
			} else {
				// subject is an uriref
				final StringBuilder sb = getBuffer();
				c = parseUriRef(c, sb);
				state.subject = createURI(sb.toString());
			}
		}
		else if (c == '_') {
			// subject is a bNode
			final StringBuilder sb = getBuffer();
			c = parseNodeID(c, sb);
			state.subject = createBNode(sb.toString());
		}
		else if (c == -1) {
			throwEOFException();
		}
		else {
			reportFatalError("Expected '<' or '_', found: " + (char)c);
		}

		return c;
	}

	private int parsePredicate(int c)
		throws IOException, RDFParseException
	{
		// predicate must be an uriref (<foo://bar>)
		if (c == '<') {
			// predicate is an uriref
			final StringBuilder sb = getBuffer();
			c = parseUriRef(c, sb);
			peek().predicate = createURI(sb.toString());
		}
		else if (c == -1) {
			throwEOFException();
		}
		else {
			reportFatalError("Expected '<', found: " + (char)c);
		}

		return c;
	}

	private int parseObject(int c)
		throws IOException, RDFParseException, RDFHandlerException
	{

		final State state = peek();
		// object is either an uriref (<foo://bar>), a nodeID (_:node1) or a
		// literal ("foo"-en or "1"^^<xsd:integer>).
		// OR a Statement
		if (c == '<') {
			if (isStatement(c)) {
				// Embedded statement.
				c = reader.read(); // known '<'
				if (c != '<')
					reportFatalError("Expected '<', found: " + (char) c);
				// have '<<', so this is an embedded statement.
				c = reader.read(); // next character.
				c = skipWhitespace(c); // skip any WS characters.
				push(new State());
				c = parseTriple(c, true/* embedded */);
				state.object = pop().lastSID;
			} else {
				// object is an uriref
				final StringBuilder sb = getBuffer();
				c = parseUriRef(c, sb);
				state.object = createURI(sb.toString());
			}
		}
		else if (c == '_') {
			// object is a bNode
			final StringBuilder sb = getBuffer();
			c = parseNodeID(c, sb);
			state.object = createBNode(sb.toString());
		}
		else if (c == '"') {
			// object is a literal
			final StringBuilder sb = getBuffer();
			final StringBuilder lang = getLanguageTagBuffer();
			final StringBuilder datatype = getDatatypeUriBuffer();
			c = parseLiteral(c, sb, lang, datatype);
			state.object = createLiteral(sb.toString(), lang.toString(), datatype.toString());
		}
		else if (c == -1) {
			throwEOFException();
		}
		else {
			reportFatalError("Expected '<', '_' or '\"', found: " + (char)c);
		}

		return c;
	}

	private int parseUriRef(int c, StringBuilder uriRef)
		throws IOException, RDFParseException
	{
		assert c == '<' : "Supplied char should be a '<', is: " + c;

		// Read up to the next '>' character
		c = reader.read();
		while (c != '>') {
			if (c == -1) {
				throwEOFException();
			}
			uriRef.append((char)c);
			c = reader.read();
		}

		// c == '>', read next char
		c = reader.read();

		return c;
	}

	private int parseNodeID(int c, StringBuilder name)
		throws IOException, RDFParseException
	{
		assert c == '_' : "Supplied char should be a '_', is: " + c;

		c = reader.read();
		if (c == -1) {
			throwEOFException();
		}
		else if (c != ':') {
			reportError("Expected ':', found: " + (char)c, NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
		}

		c = reader.read();
		if (c == -1) {
			throwEOFException();
		}
//      modified to allow fully numeric bnode ids 
////		else if (!NTriplesUtil.isLetter(c)) {
//		else if (!/*NTriplesUtil.*/isLetter(c)) { 
//			reportError("Expected a letter, found: " + (char)c);
//		}
		name.append((char)c);

		// Read all following letter and numbers, they are part of the name
		c = reader.read();
//		while (c != -1 && NTriplesUtil.isLetterOrNumber(c)) {
		while (c != -1 && /*NTriplesUtil.*/isLetterOrNumber(c)) {
			name.append((char)c);
			c = reader.read();
		}

		return c;
	}
	
	/**
	 * Checks whether the supplied character is a letter or number according to
	 * the N-Triples specification.
	 * 
	 * @see #isLetter
	 * @see NTriplesUtil#isLetterOrNumber(int)
	 */
	public static boolean isLetterOrNumber(int c) {
		return isLetter(c) || NTriplesUtil.isNumber(c);
	}

	/** 
	 * Checks whether the supplied character is a letter according to
	 * the N-Triples specification. N-Triples letters are A - Z and a - z.
	 * 
	 * @see NTriplesUtil#isLetter(int)
	 */
	private static boolean isLetter(int c) {
		return (c >= 65 && c <= 90) || // A - Z
				(c >= 97 && c <= 122) || // a - z
				(EXPANDED_LETTERS && (c == '_' || c == '-'));
	}

	/** FIXME This is Hacked to allow both "_" and "-" in a bnode name. */
	private static final boolean EXPANDED_LETTERS = true;

	private int parseLiteral(int c, final StringBuilder value,
			final StringBuilder lang, final StringBuilder datatype)
			throws IOException, RDFParseException
	{
		assert c == '"' : "Supplied char should be a '\"', is: " + c;

		// Read up to the next '"' character
		c = reader.read();
		while (c != '"') {
			if (c == -1) {
				throwEOFException();
			}
			value.append((char)c);

			if (c == '\\') {
				// This escapes the next character, which might be a double quote
				c = reader.read();
				if (c == -1) {
					throwEOFException();
				}
				value.append((char)c);
			}

			c = reader.read();
		}

		// c == '"', read next char
		c = reader.read();

		if (c == '@') {
			// Read language
			c = reader.read();
			while (c != -1 && c != '.' && c != '^' && c != ' ' && c != '\t'
					&& c != '>' // End of Statement about Statement.
					) {
				lang.append((char)c);
				c = reader.read();
			}
		}
		else if (c == '^') {
			// Read datatype
			c = reader.read();

			// c should be another '^'
			if (c == -1) {
				throwEOFException();
			}
			else if (c != '^') {
				reportError("Expected '^', found: " + (char)c,
				        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
			}

			c = reader.read();

			// c should be a '<'
			if (c == -1) {
				throwEOFException();
			}
			else if (c != '<') {
				reportError("Expected '<', found: " + (char)c,
				        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
			}

			c = parseUriRef(c, datatype);
		}

		return c;
	}

	@Override
	protected URI createURI(String uri)
		throws RDFParseException
	{
		try {
			uri = NTriplesUtil.unescapeString(uri);
		}
		catch (IllegalArgumentException e) {
			reportError(e.getMessage(),  NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
		}

		return super.createURI(uri);
	}

	protected Literal createLiteral(String label, String lang, String datatype)
		throws RDFParseException
	{
		try {
			label = NTriplesUtil.unescapeString(label);
		}
		catch (IllegalArgumentException e) {
		    reportFatalError(e.getMessage());
		}

		if (lang.length() == 0) {
			lang = null;
		}

		if (datatype.length() == 0) {
			datatype = null;
		}

		URI dtURI = null;
		if (datatype != null) {
			dtURI = createURI(datatype);
		}

		return super.createLiteral(label, lang, dtURI);
	}

	/**
	 * Overrides {@link RDFParserBase#reportWarning(String)}, adding line number
	 * information to the error.
	 */
	@Override
	protected void reportWarning(String msg)
	{
		reportWarning(msg, lineNo, -1);
	}

    /**
     * Overrides {@link RDFParserBase#reportError(String)}, adding line number
     * information to the error.
     */
    @Override
    protected void reportError(String msg, RioSetting<Boolean> setting)
        throws RDFParseException
    {
        reportError(msg, lineNo, -1, setting);
    }

    protected void reportError(Exception e, RioSetting<Boolean> setting)
        throws RDFParseException
    {
        reportError(e, lineNo, -1, setting);
    }

	/**
	 * Overrides {@link RDFParserBase#reportFatalError(String)}, adding line
	 * number information to the error.
	 */
	@Override
	protected void reportFatalError(String msg)
		throws RDFParseException
	{
		reportFatalError(msg, lineNo, -1);
	}

	/**
	 * Overrides {@link RDFParserBase#reportFatalError(Exception)}, adding line
	 * number information to the error.
	 */
	@Override
	protected void reportFatalError(Exception e)
		throws RDFParseException
	{
		reportFatalError(e, lineNo, -1);
	}

	private void throwEOFException()
		throws RDFParseException
	{
		throw new RDFParseException("Unexpected end of file");
	}
	

	/**
	 * Return a buffer of zero length and non-zero capacity. The same buffer is
	 * reused for each thing which is parsed. This reduces the heap churn
	 * substantially. However, you have to watch out for side-effects and convert
	 * the buffer to a {@link String} before the buffer is reused.
	 * 
	 * @param capacityIsIgnored
	 * @return
	 */
	private StringBuilder getBuffer() {
		buffer.setLength(0);
		return buffer;
	}

	private final StringBuilder buffer = new StringBuilder(100);

	/**
	 * Return a buffer for the use of parsing literal language tags. The buffer
	 * is of zero length and non-zero capacity. The same buffer is reused for
	 * each tag which is parsed. This reduces the heap churn substantially.
	 * However, you have to watch out for side-effects and convert the buffer to
	 * a {@link String} before the buffer is reused.
	 * 
	 * @param capacityIsIgnored
	 * @return
	 */
	private StringBuilder getLanguageTagBuffer() {
		languageTagBuffer.setLength(0);
		return languageTagBuffer;
	}

	private final StringBuilder languageTagBuffer = new StringBuilder(8);

	/**
	 * Return a buffer for the use of parsing literal datatype URIs. The buffer
	 * is of zero length and non-zero capacity. The same buffer is reused for
	 * each datatype which is parsed. This reduces the heap churn substantially.
	 * However, you have to watch out for side-effects and convert the buffer to
	 * a {@link String} before the buffer is reused.
	 * 
	 * @param capacityIsIgnored
	 * @return
	 */
	private StringBuilder getDatatypeUriBuffer() {
		datatypeUriBuffer.setLength(0);
		return datatypeUriBuffer;
	}

	private final StringBuilder datatypeUriBuffer = new StringBuilder(40);

//	@Override
//	protected void clear() {
//		super.clear();
//		// get rid of anything large left in the buffers.
//		buffer.setLength(0);
//		buffer.trimToSize();
//		languageTagBuffer.setLength(0);
//		languageTagBuffer.trimToSize();
//		datatypeUriBuffer.setLength(0);
//		datatypeUriBuffer.trimToSize();
//	}

}
