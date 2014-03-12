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
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.rio.turtle.TurtleUtil;

import com.bigdata.rdf.model.BigdataValueFactory;

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

//  /*-----------*
//   * Variables *
//   *-----------*/
//
//  private LineNumberReader lineReader;
//
//  private PushbackReader reader;
//
//  private Resource subject;
//
//  private URI predicate;
//
//  private Value object;
//
//  /*--------------*
//   * Constructors *
//   *--------------*/
//
//  /**
//   * Creates a new TurtleParser that will use a {@link ValueFactoryImpl} to
//   * create RDF model objects.
//   */
//  public BigdataTurtleParser() {
//      this(null);
//  }

    private BigdataValueFactory valueFactory;
    
//  /**
//   * Creates a new TurtleParser that will use the supplied ValueFactory to
//   * create RDF model objects.
//   * 
//   * @param valueFactory
//   *        A ValueFactory.
//   */
//  public BigdataTurtleParser(ValueFactory valueFactory) {
//      super(valueFactory);
//      
//      if (valueFactory instanceof BigdataValueFactory) 
//          this.valueFactory = (BigdataValueFactory) valueFactory;
//      else
//          this.valueFactory = null;
//  }
    
    @Override
    public void setValueFactory(ValueFactory valueFactory) {
        super.setValueFactory(valueFactory);
        
        if (valueFactory instanceof BigdataValueFactory) 
            this.valueFactory = (BigdataValueFactory) valueFactory;
        else
            this.valueFactory = null;
    }
//
//  /*---------*
//   * Methods *
//   *---------*/
//
//  public RDFFormat getRDFFormat() {
//      return RDFFormat.TURTLE;
//  }
//
//  /**
//   * Implementation of the <tt>parse(InputStream, String)</tt> method defined
//   * in the RDFParser interface.
//   * 
//   * @param in
//   *        The InputStream from which to read the data, must not be
//   *        <tt>null</tt>. The InputStream is supposed to contain UTF-8 encoded
//   *        Unicode characters, as per the Turtle specification.
//   * @param baseURI
//   *        The URI associated with the data in the InputStream, must not be
//   *        <tt>null</tt>.
//   * @throws IOException
//   *         If an I/O error occurred while data was read from the InputStream.
//   * @throws RDFParseException
//   *         If the parser has found an unrecoverable parse error.
//   * @throws RDFHandlerException
//   *         If the configured statement handler encountered an unrecoverable
//   *         error.
//   * @throws IllegalArgumentException
//   *         If the supplied input stream or base URI is <tt>null</tt>.
//   */
//  public synchronized void parse(InputStream in, String baseURI)
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      if (in == null) {
//          throw new IllegalArgumentException("Input stream must not be 'null'");
//      }
//      // Note: baseURI will be checked in parse(Reader, String)
//
//      try {
//          parse(new InputStreamReader(in, "UTF-8"), baseURI);
//      }
//      catch (UnsupportedEncodingException e) {
//          // Every platform should support the UTF-8 encoding...
//          throw new RuntimeException(e);
//      }
//  }
//
//  /**
//   * Implementation of the <tt>parse(Reader, String)</tt> method defined in the
//   * RDFParser interface.
//   * 
//   * @param reader
//   *        The Reader from which to read the data, must not be <tt>null</tt>.
//   * @param baseURI
//   *        The URI associated with the data in the Reader, must not be
//   *        <tt>null</tt>.
//   * @throws IOException
//   *         If an I/O error occurred while data was read from the InputStream.
//   * @throws RDFParseException
//   *         If the parser has found an unrecoverable parse error.
//   * @throws RDFHandlerException
//   *         If the configured statement handler encountered an unrecoverable
//   *         error.
//   * @throws IllegalArgumentException
//   *         If the supplied reader or base URI is <tt>null</tt>.
//   */
//  public synchronized void parse(Reader reader, String baseURI)
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      if (reader == null) {
//          throw new IllegalArgumentException("Reader must not be 'null'");
//      }
//      if (baseURI == null) {
//          throw new IllegalArgumentException("base URI must not be 'null'");
//      }
//
//      rdfHandler.startRDF();
//
//      lineReader = new LineNumberReader(reader);
//      // Start counting lines at 1:
//      lineReader.setLineNumber(1);
//
//      // Allow at most 2 characters to be pushed back:
//      this.reader = new PushbackReader(lineReader, 2);
//
//      // Store normalized base URI
//      setBaseURI(baseURI);
//
//      reportLocation();
//
//      try {
//          int c = skipWSC();
//
//          while (c != -1) {
//              parseStatement();
//              c = skipWSC();
//          }
//      }
//      finally {
//          clear();
//      }
//
//      rdfHandler.endRDF();
//  }
//
//  protected void parseStatement()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      int c = peek();
//
//      if (c == '@') {
//          parseDirective();
//          skipWSC();
//          verifyCharacter(read(), ".");
//      }
//      else {
//          parseTriples();
//          skipWSC();
//          verifyCharacter(read(), ".");
//      }
//  }
//
//  protected void parseDirective()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      // Verify that the first characters form the string "prefix"
//      verifyCharacter(read(), "@");
//
//      StringBuilder sb = new StringBuilder(8);
//
//      int c = read();
//      while (c != -1 && !TurtleUtil.isWhitespace(c)) {
//          sb.append((char)c);
//          c = read();
//      }
//
//      String directive = sb.toString();
//      if (directive.equals("prefix")) {
//          parsePrefixID();
//      }
//      else if (directive.equals("base")) {
//          parseBase();
//      }
//      else if (directive.length() == 0) {
//          reportFatalError("Directive name is missing, expected @prefix or @base");
//      }
//      else {
//          reportFatalError("Unknown directive \"@" + directive + "\"");
//      }
//  }
//
//  protected void parsePrefixID()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      skipWSC();
//
//      // Read prefix ID (e.g. "rdf:" or ":")
//      StringBuilder prefixID = new StringBuilder(8);
//
//      while (true) {
//          int c = read();
//
//          if (c == ':') {
//              unread(c);
//              break;
//          }
//          else if (TurtleUtil.isWhitespace(c)) {
//              break;
//          }
//          else if (c == -1) {
//              throwEOFException();
//          }
//
//          prefixID.append((char)c);
//      }
//
//      skipWSC();
//
//      verifyCharacter(read(), ":");
//
//      skipWSC();
//
//      // Read the namespace URI
//      URI namespace = parseURI();
//
//      // Store and report this namespace mapping
//      String prefixStr = prefixID.toString();
//      String namespaceStr = namespace.toString();
//
//      setNamespace(prefixStr, namespaceStr);
//
//      rdfHandler.handleNamespace(prefixStr, namespaceStr);
//  }
//
//  protected void parseBase()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      skipWSC();
//
//      URI baseURI = parseURI();
//
//      setBaseURI(baseURI.toString());
//  }
//
//  protected void parseTriples()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      parseSubject();
//      skipWSC();
//      parsePredicateObjectList();
//
//      subject = null;
//      predicate = null;
//      object = null;
//  }
//
//  protected void parsePredicateObjectList()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      predicate = parsePredicate();
//
//      skipWSC();
//
//      parseObjectList();
//
//      while (skipWSC() == ';') {
//          read();
//
//          int c = skipWSC();
//
//          if (c == '.' || // end of triple
//                  c == ']') // end of predicateObjectList inside blank node
//          {
//              break;
//          }
//
//          predicate = parsePredicate();
//
//          skipWSC();
//
//          parseObjectList();
//      }
//  }
//
//  protected void parseObjectList()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      parseObject();
//
//      while (skipWSC() == ',') {
//          read();
//          skipWSC();
//          parseObject();
//      }
//  }
//
//  protected void parseSubject()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      int c = peek();
//
//      if (c == '(') {
//          subject = parseCollection();
//      }
//      else if (c == '[') {
//          subject = parseImplicitBlank();
//      }
//      else {
//          Value value = parseValue();
//
//          if (value instanceof Resource) {
//              subject = (Resource)value;
//          }
//          else {
//              reportFatalError("Illegal subject value: " + value);
//          }
//      }
//  }
//
//  protected URI parsePredicate()
//      throws IOException, RDFParseException
//  {
//      // Check if the short-cut 'a' is used
//      int c1 = read();
//
//      if (c1 == 'a') {
//          int c2 = read();
//
//          if (TurtleUtil.isWhitespace(c2)) {
//              // Short-cut is used, return the rdf:type URI
//              return RDF.TYPE;
//          }
//
//          // Short-cut is not used, unread all characters
//          unread(c2);
//      }
//      unread(c1);
//
//      // Predicate is a normal resource
//      Value predicate = parseValue();
//      if (predicate instanceof URI) {
//          return (URI)predicate;
//      }
//      else {
//          reportFatalError("Illegal predicate value: " + predicate);
//          return null;
//      }
//  }
//
//  protected void parseObject()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      int c = peek();
//
//      if (c == '(') {
//          object = parseCollection();
//      }
//      else if (c == '[') {
//          object = parseImplicitBlank();
//      }
//      else {
//          object = parseValue();
//      }
//
//      reportStatement(subject, predicate, object);
//  }
//
//  /**
//   * Parses a collection, e.g. <tt>( item1 item2 item3 )</tt>.
//   */
//  protected Resource parseCollection()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      verifyCharacter(read(), "(");
//
//      int c = skipWSC();
//
//      if (c == ')') {
//          // Empty list
//          read();
//          return RDF.NIL;
//      }
//      else {
//          BNode listRoot = createBNode();
//
//          // Remember current subject and predicate
//          Resource oldSubject = subject;
//          URI oldPredicate = predicate;
//
//          // generated bNode becomes subject, predicate becomes rdf:first
//          subject = listRoot;
//          predicate = RDF.FIRST;
//
//          parseObject();
//
//          BNode bNode = listRoot;
//
//          while (skipWSC() != ')') {
//              // Create another list node and link it to the previous
//              BNode newNode = createBNode();
//              reportStatement(bNode, RDF.REST, newNode);
//
//              // New node becomes the current
//              subject = bNode = newNode;
//
//              parseObject();
//          }
//
//          // Skip ')'
//          read();
//
//          // Close the list
//          reportStatement(bNode, RDF.REST, RDF.NIL);
//
//          // Restore previous subject and predicate
//          subject = oldSubject;
//          predicate = oldPredicate;
//
//          return listRoot;
//      }
//  }
//
//  /**
//   * Parses an implicit blank node. This method parses the token <tt>[]</tt>
//   * and predicateObjectLists that are surrounded by square brackets.
//   */
//  protected Resource parseImplicitBlank()
//      throws IOException, RDFParseException, RDFHandlerException
//  {
//      verifyCharacter(read(), "[");
//
//      BNode bNode = createBNode();
//
//      int c = read();
//      if (c != ']') {
//          unread(c);
//
//          // Remember current subject and predicate
//          Resource oldSubject = subject;
//          URI oldPredicate = predicate;
//
//          // generated bNode becomes subject
//          subject = bNode;
//
//          // Enter recursion with nested predicate-object list
//          skipWSC();
//
//          parsePredicateObjectList();
//
//          skipWSC();
//
//          // Read closing bracket
//          verifyCharacter(read(), "]");
//
//          // Restore previous subject and predicate
//          subject = oldSubject;
//          predicate = oldPredicate;
//      }
//
//      return bNode;
//  }
//
    /**
     * Parses an RDF value. This method parses uriref, qname, node ID, quoted
     * literal, integer, double and boolean.
     */
    protected Value parseValue()
        throws IOException, RDFParseException
    {
        int c = peek();
        
        if (c == '<') {
            // uriref, e.g. <foo://bar> or sidref <<a> <b> <c>>
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
        else if (c == '"') {
            // quoted literal, e.g. "foo" or """foo"""
            return parseQuotedLiteral();
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
            Thread.dumpStack();
            while (c != -1) System.err.print((char) (c = read()));
            reportFatalError("Expected an RDF value here, found '" + (char)c + "'");
            return null;
        }
    }
//
//  /**
//   * Parses a quoted string, optionally followed by a language tag or datatype.
//   */
//  protected Literal parseQuotedLiteral()
//      throws IOException, RDFParseException
//  {
//      String label = parseQuotedString();
//
//      // Check for presence of a language tag or datatype
//      int c = peek();
//
//      if (c == '@') {
//          read();
//
//          // Read language
//          StringBuilder lang = new StringBuilder(8);
//
//          c = read();
//          if (c == -1) {
//              throwEOFException();
//          }
//          if (!TurtleUtil.isLanguageStartChar(c)) {
//              reportError("Expected a letter, found '" + (char)c + "'");
//          }
//
//          lang.append((char)c);
//
//          c = read();
//          while (TurtleUtil.isLanguageChar(c)) {
//              lang.append((char)c);
//              c = read();
//          }
//
//          unread(c);
//
//          return createLiteral(label, lang.toString(), null);
//      }
//      else if (c == '^') {
//          read();
//
//          // next character should be another '^'
//          verifyCharacter(read(), "^");
//
//          // Read datatype
//          Value datatype = parseValue();
//          if (datatype instanceof URI) {
//              return createLiteral(label, null, (URI)datatype);
//          }
//          else {
//              reportFatalError("Illegal datatype value: " + datatype);
//              return null;
//          }
//      }
//      else {
//          return createLiteral(label, null, null);
//      }
//  }
//
//  /**
//   * Parses a quoted string, which is either a "normal string" or a """long
//   * string""".
//   */
//  protected String parseQuotedString()
//      throws IOException, RDFParseException
//  {
//      String result = null;
//
//      // First character should be '"'
//      verifyCharacter(read(), "\"");
//
//      // Check for long-string, which starts and ends with three double quotes
//      int c2 = read();
//      int c3 = read();
//
//      if (c2 == '"' && c3 == '"') {
//          // Long string
//          result = parseLongString();
//      }
//      else {
//          // Normal string
//          unread(c3);
//          unread(c2);
//
//          result = parseString();
//      }
//
//      // Unescape any escape sequences
//      try {
//          result = TurtleUtil.decodeString(result);
//      }
//      catch (IllegalArgumentException e) {
//          reportError(e.getMessage());
//      }
//
//      return result;
//  }
//
//  /**
//   * Parses a "normal string". This method assumes that the first double quote
//   * has already been parsed.
//   */
//  protected String parseString()
//      throws IOException, RDFParseException
//  {
//      StringBuilder sb = new StringBuilder(32);
//
//      while (true) {
//          int c = read();
//
//          if (c == '"') {
//              break;
//          }
//          else if (c == -1) {
//              throwEOFException();
//          }
//
//          sb.append((char)c);
//
//          if (c == '\\') {
//              // This escapes the next character, which might be a '"'
//              c = read();
//              if (c == -1) {
//                  throwEOFException();
//              }
//              sb.append((char)c);
//          }
//      }
//
//      return sb.toString();
//  }
//
//  /**
//   * Parses a """long string""". This method assumes that the first three
//   * double quotes have already been parsed.
//   */
//  protected String parseLongString()
//      throws IOException, RDFParseException
//  {
//      StringBuilder sb = new StringBuilder(1024);
//
//      int doubleQuoteCount = 0;
//      int c;
//
//      while (doubleQuoteCount < 3) {
//          c = read();
//
//          if (c == -1) {
//              throwEOFException();
//          }
//          else if (c == '"') {
//              doubleQuoteCount++;
//          }
//          else {
//              doubleQuoteCount = 0;
//          }
//
//          sb.append((char)c);
//
//          if (c == '\\') {
//              // This escapes the next character, which might be a '"'
//              c = read();
//              if (c == -1) {
//                  throwEOFException();
//              }
//              sb.append((char)c);
//          }
//      }
//
//      return sb.substring(0, sb.length() - 3);
//  }
//
//  protected Literal parseNumber()
//      throws IOException, RDFParseException
//  {
//      StringBuilder value = new StringBuilder(8);
//      URI datatype = XMLSchema.INTEGER;
//
//      int c = read();
//
//      // read optional sign character
//      if (c == '+' || c == '-') {
//          value.append((char)c);
//          c = read();
//      }
//
//      while (ASCIIUtil.isNumber(c)) {
//          value.append((char)c);
//          c = read();
//      }
//
//      if (c == '.' || c == 'e' || c == 'E') {
//          // We're parsing a decimal or a double
//          datatype = XMLSchema.DECIMAL;
//
//          // read optional fractional digits
//          if (c == '.') {
//              value.append((char)c);
//
//              c = read();
//              while (ASCIIUtil.isNumber(c)) {
//                  value.append((char)c);
//                  c = read();
//              }
//
//              if (value.length() == 1) {
//                  // We've only parsed a '.'
//                  reportFatalError("Object for statement missing");
//              }
//          }
//          else {
//              if (value.length() == 0) {
//                  // We've only parsed an 'e' or 'E'
//                  reportFatalError("Object for statement missing");
//              }
//          }
//
//          // read optional exponent
//          if (c == 'e' || c == 'E') {
//              datatype = XMLSchema.DOUBLE;
//              value.append((char)c);
//
//              c = read();
//              if (c == '+' || c == '-') {
//                  value.append((char)c);
//                  c = read();
//              }
//
//              if (!ASCIIUtil.isNumber(c)) {
//                  reportError("Exponent value missing");
//              }
//
//              value.append((char)c);
//
//              c = read();
//              while (ASCIIUtil.isNumber(c)) {
//                  value.append((char)c);
//                  c = read();
//              }
//          }
//      }
//
//      // Unread last character, it isn't part of the number
//      unread(c);
//
//      // String label = value.toString();
//      // if (datatype.equals(XMLSchema.INTEGER)) {
//      // try {
//      // label = XMLDatatypeUtil.normalizeInteger(label);
//      // }
//      // catch (IllegalArgumentException e) {
//      // // Note: this should never happen because of the parse constraints
//      // reportError("Illegal integer value: " + label);
//      // }
//      // }
//      // return createLiteral(label, null, datatype);
//
//      // Return result as a typed literal
//      return createLiteral(value.toString(), null, datatype);
//  }
//
    protected Value parseURIOrSid()
        throws IOException, RDFParseException
    {
        // First character should be '<'
        int c = read();
        verifyCharacter(c, "<");
        
        int n = peek();
        if (n == '<') {
            read();
            if (this.valueFactory == null) {
                reportError("must use a BigdataValueFactory to use the RDR syntax");
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
            if (valueFactory == null) {
                /*
                 * The BigdataValueFactory has an extension to create a BNode
                 * from a Statement.  You need to specify that value factory
                 * when you create the parser using setValueFactory().
                 */
                throw new RDFParseException(
                        "You must set a ValueFactory to use the RDR syntax");
            }
            
            try {
            	// write the RDR statement
            	reportStatement(s, p, o);
            } catch (RDFHandlerException ex) {
            	throw new IOException(ex);
            }
            
            return valueFactory.createBNode(valueFactory.createStatement(s, p, o));
        } else {
            reportError("expecting >> to close statement identifier");
            throw new IOException("expecting >> to close statement identifier");
        }
        
    }
//
//  protected URI parseURI()
//          throws IOException, RDFParseException
//  {
//      StringBuilder uriBuf = new StringBuilder(100);
//
//      // First character should be '<'
//      int c = read();
//      verifyCharacter(c, "<");
//
//      // Read up to the next '>' character
//      while (true) {
//          c = read();
//
//          if (c == '>') {
//              break;
//          }
//          else if (c == -1) {
//              throwEOFException();
//          }
//
//          uriBuf.append((char)c);
//
//          if (c == '\\') {
//              // This escapes the next character, which might be a '>'
//              c = read();
//              if (c == -1) {
//                  throwEOFException();
//              }
//              uriBuf.append((char)c);
//          }
//      }
//
//      String uri = uriBuf.toString();
//
//      // Unescape any escape sequences
//      try {
//          uri = TurtleUtil.decodeString(uri);
//      }
//      catch (IllegalArgumentException e) {
//          reportError(e.getMessage());
//      }
//
//      return super.resolveURI(uri);
//  }
//
//  /**
//   * Parses qnames and boolean values, which have equivalent starting
//   * characters.
//   */
//  protected Value parseQNameOrBoolean()
//      throws IOException, RDFParseException
//  {
//      // First character should be a ':' or a letter
//      int c = read();
//      if (c == -1) {
//          throwEOFException();
//      }
//      if (c != ':' && !TurtleUtil.isPrefixStartChar(c)) {
//          reportError("Expected a ':' or a letter, found '" + (char)c + "'");
//      }
//
//      String namespace = null;
//
//      if (c == ':') {
//          // qname using default namespace
//          namespace = getNamespace("");
//          if (namespace == null) {
//              reportError("Default namespace used but not defined");
//          }
//      }
//      else {
//          // c is the first letter of the prefix
//          StringBuilder prefix = new StringBuilder(8);
//          prefix.append((char)c);
//
//          c = read();
//          while (TurtleUtil.isPrefixChar(c)) {
//              prefix.append((char)c);
//              c = read();
//          }
//
//          if (c != ':') {
//              // prefix may actually be a boolean value
//              String value = prefix.toString();
//
//              if (value.equals("true") || value.equals("false")) {
//                  return createLiteral(value, null, XMLSchema.BOOLEAN);
//              }
//          }
//
//          verifyCharacter(c, ":");
//
//          namespace = getNamespace(prefix.toString());
//          if (namespace == null) {
//              reportError("Namespace prefix '" + prefix.toString() + "' used but not defined");
//          }
//      }
//
//      // c == ':', read optional local name
//      StringBuilder localName = new StringBuilder(16);
//      c = read();
//      if (TurtleUtil.isNameStartChar(c)) {
//          localName.append((char)c);
//
//          c = read();
//          while (TurtleUtil.isNameChar(c)) {
//              localName.append((char)c);
//              c = read();
//          }
//      }
//
//      // Unread last character
//      unread(c);
//
//      // Note: namespace has already been resolved
//      return createURI(namespace + localName.toString());
//  }
//
    /**
     * Parses a blank node ID, e.g. <tt>_:node1</tt>.
     */
    protected BNode parseNodeID()
        throws IOException, RDFParseException
    {
        // Node ID should start with "_:"
        verifyCharacter(read(), "_");
        verifyCharacter(read(), ":");

        // Read the node ID
        int c = read();
        if (c == -1) {
            throwEOFException();
        }
//      modified to allow fully numeric bnode ids 
//      else if (!TurtleUtil.isNameStartChar(c)) {
//          reportError("Expected a letter, found '" + (char)c + "'");
//      }

        StringBuilder name = new StringBuilder(32);
        name.append((char)c);

        // Read all following letter and numbers, they are part of the name
        c = read();
        while (TurtleUtil.isNameChar(c)) {
            name.append((char)c);
            c = read();
        }

        unread(c);

        return createBNode(name.toString());
    }
//
//  protected void reportStatement(Resource subj, URI pred, Value obj)
//      throws RDFParseException, RDFHandlerException
//  {
//      Statement st = createStatement(subj, pred, obj);
//      rdfHandler.handleStatement(st);
//  }
//
//  /**
//   * Verifies that the supplied character <tt>c</tt> is one of the expected
//   * characters specified in <tt>expected</tt>. This method will throw a
//   * <tt>ParseException</tt> if this is not the case.
//   */
//  protected void verifyCharacter(int c, String expected)
//      throws RDFParseException
//  {
//      if (c == -1) {
//          throwEOFException();
//      }
//      else if (expected.indexOf((char)c) == -1) {
//          StringBuilder msg = new StringBuilder(32);
//          msg.append("Expected ");
//          for (int i = 0; i < expected.length(); i++) {
//              if (i > 0) {
//                  msg.append(" or ");
//              }
//              msg.append('\'');
//              msg.append(expected.charAt(i));
//              msg.append('\'');
//          }
//          msg.append(", found '");
//          msg.append((char)c);
//          msg.append("'");
//
//          reportError(msg.toString());
//      }
//  }
//
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
//
//  /**
//   * Consumes characters from reader until the first EOL has been read. This
//   * line of text is then passed to the {@link #rdfHandler} as a comment.
//   */
//  protected void processComment()
//      throws IOException, RDFHandlerException
//  {
//      StringBuilder comment = new StringBuilder(64);
//      int c = read();
//      while (c != -1 && c != 0xD && c != 0xA) {
//          comment.append((char)c);
//          c = read();
//      }
//
//      // c is equal to -1, \r or \n.
//      // In case c is equal to \r, we should also read a following \n.
//      if (c == 0xD) {
//          c = read();
//
//          if (c != 0xA) {
//              unread(c);
//          }
//      }
//      rdfHandler.handleComment(comment.toString());
//      reportLocation();
//  }
//
//  protected int read()
//      throws IOException
//  {
//      return reader.read();
//  }
//
//  protected void unread(int c)
//      throws IOException
//  {
//      if (c != -1) {
//          reader.unread(c);
//      }
//  }
//
//  protected int peek()
//      throws IOException
//  {
//      int result = read();
//      unread(result);
//      return result;
//  }
//
//  protected void reportLocation() {
//      reportLocation(lineReader.getLineNumber(), -1);
//  }
//
//  /**
//   * Overrides {@link RDFParserBase#reportWarning(String)}, adding line number
//   * information to the error.
//   */
//  @Override
//  protected void reportWarning(String msg) {
//      reportWarning(msg, lineReader.getLineNumber(), -1);
//  }
//
//  /**
//   * Overrides {@link RDFParserBase#reportError(String)}, adding line number
//   * information to the error.
//   */
//  @Override
//  protected void reportError(String msg)
//      throws RDFParseException
//  {
//      reportError(msg, lineReader.getLineNumber(), -1);
//  }
//
//  /**
//   * Overrides {@link RDFParserBase#reportFatalError(String)}, adding line
//   * number information to the error.
//   */
//  @Override
//  protected void reportFatalError(String msg)
//      throws RDFParseException
//  {
//      reportFatalError(msg, lineReader.getLineNumber(), -1);
//  }
//
//  /**
//   * Overrides {@link RDFParserBase#reportFatalError(Exception)}, adding line
//   * number information to the error.
//   */
//  @Override
//  protected void reportFatalError(Exception e)
//      throws RDFParseException
//  {
//      reportFatalError(e, lineReader.getLineNumber(), -1);
//  }
//
//  protected void throwEOFException()
//      throws RDFParseException
//  {
//      throw new RDFParseException("Unexpected end of file");
//  }
}
