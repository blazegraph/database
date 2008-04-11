/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.rio.rdfxml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import info.aduna.net.ParsedURI;
import info.aduna.xml.XMLUtil;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.ParseLocationListener;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

/**
 * A filter on SAX events to make life easier on the RDF parser itself. This
 * filter does things like combining a call to startElement() that is directly
 * followed by a call to endElement() to a single call to emptyElement().
 */
class SAXFilter implements ContentHandler {

	/*-----------*
	 * Variables *
	 *-----------*/

	/**
	 * The RDF parser to supply the filtered SAX events to.
	 */
	private RDFXMLParser rdfParser;

	/**
	 * A Locator indicating a position in the text that is currently being parsed
	 * by the SAX parser.
	 */
	private Locator locator;

	/**
	 * Stack of ElementInfo objects.
	 */
	private Stack<ElementInfo> elInfoStack = new Stack<ElementInfo>();

	/**
	 * StringBuilder used to collect text during parsing.
	 */
	private StringBuilder charBuf = new StringBuilder(512);

	/**
	 * The document's URI.
	 */
	private ParsedURI documentURI;

	/**
	 * Flag indicating whether the parser parses stand-alone RDF documents. In
	 * stand-alone documents, the rdf:RDF element is optional if it contains just
	 * one element.
	 */
	private boolean parseStandAloneDocuments = true;

	/**
	 * Variable used to defer reporting of start tags. Reporting start tags is
	 * deferred to be able to combine a start tag and an immediately following
	 * end tag to a single call to emptyElement().
	 */
	private ElementInfo deferredElement = null;

	/**
	 * New namespace mappings that have been reported for the next start tag by
	 * the SAX parser, but that are not yet assigned to an ElementInfo object.
	 */
	private Map<String, String> newNamespaceMappings = new LinkedHashMap<String, String>();

	/**
	 * Flag indicating whether we're currently parsing RDF elements.
	 */
	private boolean inRDFContext;

	/**
	 * The number of elements on the stack that are in the RDF context.
	 */
	private int rdfContextStackHeight;

	/**
	 * Flag indicating whether we're currently parsing an XML literal.
	 */
	private boolean parseLiteralMode = false;

	/**
	 * The number of elements on the stack that are part of an XML literal.
	 */
	private int xmlLiteralStackHeight;

	/**
	 * The prefixes that are defined in the XML literal itself (this in contrast
	 * to the namespaces from the XML literal's context).
	 */
	private List<String> xmlLiteralPrefixes = new ArrayList<String>();

	/**
	 * The prefixes that were used in an XML literal, but that were not defined
	 * in it (but rather in the XML literal's context).
	 */
	private List<String> unknownPrefixesInXMLLiteral = new ArrayList<String>();

	/*--------------*
	 * Constructors *
	 *--------------*/

	public SAXFilter(RDFXMLParser rdfParser) {
		this.rdfParser = rdfParser;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public Locator getLocator() {
		return locator;
	}

	public void clear() {
		locator = null;
		elInfoStack.clear();
		charBuf.setLength(0);
		documentURI = null;
		deferredElement = null;

		newNamespaceMappings.clear();

		inRDFContext = false;
		rdfContextStackHeight = 0;

		parseLiteralMode = false;
		xmlLiteralStackHeight = 0;

		xmlLiteralPrefixes.clear();
		unknownPrefixesInXMLLiteral.clear();
	}

	public void setDocumentURI(String documentURI) {
		this.documentURI = createBaseURI(documentURI);
	}

	public void setParseStandAloneDocuments(boolean standAloneDocs) {
		parseStandAloneDocuments = standAloneDocs;
	}

	public boolean getParseStandAloneDocuments() {
		return parseStandAloneDocuments;
	}

	/*---------------------------------------*
	 * Methods from interface ContentHandler *
	 *---------------------------------------*/

	public void setDocumentLocator(Locator loc) {
		locator = loc;

		ParseLocationListener pll = rdfParser.getParseLocationListener();
		if (pll != null) {
			pll.parseLocationUpdate(loc.getLineNumber(), loc.getColumnNumber());
		}
	}

	public void startDocument() {
		// ignore
	}

	public void endDocument() {
		// ignore
	}

	public void startPrefixMapping(String prefix, String uri)
		throws SAXException
	{
		try {
			if (deferredElement != null) {
				// This new prefix mapping must come from a new start tag
				reportDeferredStartElement();
			}

			newNamespaceMappings.put(prefix, uri);

			if (parseLiteralMode) {
				// This namespace is introduced inside an XML literal
				xmlLiteralPrefixes.add(prefix);
			}

			rdfParser.getRDFHandler().handleNamespace(prefix, uri);
		}
		catch (RDFParseException e) {
			throw new SAXException(e);
		}
		catch (RDFHandlerException e) {
			throw new SAXException(e);
		}
	}

	public void endPrefixMapping(String prefix) {
		if (parseLiteralMode) {
			xmlLiteralPrefixes.remove(prefix);
		}
	}

	public void startElement(String namespaceURI, String localName, String qName, Attributes attributes)
		throws SAXException
	{
		try {
			if (deferredElement != null) {
				// The next call could set parseLiteralMode to true!
				reportDeferredStartElement();
			}

			if (parseLiteralMode) {
				appendStartTag(qName, attributes);
				xmlLiteralStackHeight++;
			}
			else {
				ElementInfo parent = peekStack();
				ElementInfo elInfo = new ElementInfo(parent, qName, namespaceURI, localName);

				elInfo.setNamespaceMappings(newNamespaceMappings);
				newNamespaceMappings.clear();

				if (!inRDFContext && parseStandAloneDocuments
						&& (!localName.equals("RDF") || !namespaceURI.equals(RDF.NAMESPACE)))
				{
					// Stand-alone document that does not start with an rdf:RDF root
					// element. Assume this root element is omitted.
					inRDFContext = true;
				}

				if (!inRDFContext) {
					// Check for presence of xml:base and xlm:lang attributes.
					for (int i = 0; i < attributes.getLength(); i++) {
						String attQName = attributes.getQName(i);

						if ("xml:base".equals(attQName)) {
							elInfo.setBaseURI(attributes.getValue(i));
						}
						else if ("xml:lang".equals(attQName)) {
							elInfo.xmlLang = attributes.getValue(i);
						}
					}

					elInfoStack.push(elInfo);

					// Check if we are entering RDF context now.
					if (localName.equals("RDF") && namespaceURI.equals(RDF.NAMESPACE)) {
						inRDFContext = true;
						rdfContextStackHeight = 0;
					}
				}
				else {
					// We're parsing RDF elements.
					checkAndCopyAttributes(attributes, elInfo);

					// Don't report the new element to the RDF parser just yet.
					deferredElement = elInfo;
				}

				charBuf.setLength(0);
			}
		}
		catch (RDFParseException e) {
			throw new SAXException(e);
		}
		catch (RDFHandlerException e) {
			throw new SAXException(e);
		}
	}

	private void reportDeferredStartElement()
		throws RDFParseException, RDFHandlerException
	{

		// Only useful for debugging.
		// if (deferredElement == null) {
		// throw new RuntimeException("no deferred start element available");
		// }

		elInfoStack.push(deferredElement);
		rdfContextStackHeight++;

		rdfParser.setBaseURI(deferredElement.baseURI);
		rdfParser.setXMLLang(deferredElement.xmlLang);

		rdfParser.startElement(deferredElement.namespaceURI, deferredElement.localName, deferredElement.qName,
				deferredElement.atts);

		deferredElement = null;
	}

	public void endElement(String namespaceURI, String localName, String qName)
		throws SAXException
	{
		try {
			// FIXME: in parseLiteralMode we should also check if start- and
			// end-tags match but these start tags are not tracked yet.

			if (rdfParser.verifyData() && !parseLiteralMode) {
				// Verify that the end tag matches the start tag.
				ElementInfo elInfo;

				if (deferredElement != null) {
					elInfo = deferredElement;
				}
				else {
					elInfo = peekStack();
				}

				if (!qName.equals(elInfo.qName)) {
					rdfParser.reportFatalError("expected end tag </'" + elInfo.qName + ">, found </" + qName + ">");
				}
			}

			if (!inRDFContext) {
				elInfoStack.pop();
				charBuf.setLength(0);
				return;
			}

			if (deferredElement == null && rdfContextStackHeight == 0) {
				// This end tag removes the element that signaled the start
				// of the RDF context (i.e. <rdf:RDF>) from the stack.
				inRDFContext = false;

				elInfoStack.pop();
				charBuf.setLength(0);
				return;
			}

			// We're still in RDF context.

			if (parseLiteralMode && xmlLiteralStackHeight > 0) {
				appendEndTag(qName);
				xmlLiteralStackHeight--;
				return;
			}

			// Check for any deferred start elements
			if (deferredElement != null) {
				// Start element still deferred, this is an empty element
				rdfParser.setBaseURI(deferredElement.baseURI);
				rdfParser.setXMLLang(deferredElement.xmlLang);

				rdfParser.emptyElement(deferredElement.namespaceURI, deferredElement.localName,
						deferredElement.qName, deferredElement.atts);

				deferredElement = null;
			}
			else {
				if (parseLiteralMode) {
					// Insert any used namespace prefixes from the XML literal's
					// context that are not defined in the XML literal itself.
					insertUsedContextPrefixes();

					rdfParser.text(charBuf.toString());

					parseLiteralMode = false;
				}
				else {
					String s = charBuf.toString().trim();

					if (s.length() > 0) {
						rdfParser.text(s);
					}
				}

				charBuf.setLength(0);

				// Handle the end tag
				elInfoStack.pop();
				rdfContextStackHeight--;

				rdfParser.endElement(namespaceURI, localName, qName);
			}
		}
		catch (RDFParseException e) {
			throw new SAXException(e);
		}
		catch (RDFHandlerException e) {
			throw new SAXException(e);
		}
	}

	public void characters(char[] ch, int start, int length)
		throws SAXException
	{
		try {
			if (inRDFContext) {
				if (deferredElement != null) {
					reportDeferredStartElement();
				}

				if (parseLiteralMode) {
					// Characters like '<', '>', and '&' must be escaped to
					// prevent breaking the XML text.
					String s = new String(ch, start, length);
					s = XMLUtil.escapeCharacterData(s);
					charBuf.append(s);
				}
				else {
					charBuf.append(ch, start, length);
				}
			}
		}
		catch (RDFParseException e) {
			throw new SAXException(e);
		}
		catch (RDFHandlerException e) {
			throw new SAXException(e);
		}
	}

	public void ignorableWhitespace(char[] ch, int start, int length) {
		if (parseLiteralMode) {
			charBuf.append(ch, start, length);
		}
	}

	public void processingInstruction(String target, String data) {
		// ignore
	}

	public void skippedEntity(String name) {
		// ignore
	}

	private void checkAndCopyAttributes(Attributes attributes, ElementInfo elInfo)
		throws SAXException, RDFParseException
	{
		Atts atts = new Atts(attributes.getLength());

		int attCount = attributes.getLength();
		for (int i = 0; i < attCount; i++) {
			String qName = attributes.getQName(i);
			String value = attributes.getValue(i);

			// attributes starting with "xml" should be ignored, except for the
			// ones that are handled by this parser (xml:lang and xml:base).
			if (qName.startsWith("xml")) {
				if (qName.equals("xml:lang")) {
					elInfo.xmlLang = value;
				}
				else if (qName.equals("xml:base")) {
					elInfo.setBaseURI(value);
				}
			}
			else {
				String namespace = attributes.getURI(i);
				String localName = attributes.getLocalName(i);

				// A limited set of unqualified attributes must be supported by
				// parsers, as is specified in section 6.1.4 of the spec
				if ("".equals(namespace)) {
					if (localName.equals("ID") || localName.equals("about") || localName.equals("resource")
							|| localName.equals("parseType") || localName.equals("type"))
					{
						rdfParser.reportWarning("use of unqualified attribute " + localName
								+ " has been deprecated");
						namespace = RDF.NAMESPACE;
					}
				}

				if (rdfParser.verifyData()) {
					if ("".equals(namespace)) {
						rdfParser.reportError("unqualified attribute '" + qName + "' not allowed");
					}
				}

				Att att = new Att(namespace, localName, qName, value);
				atts.addAtt(att);
			}
		}

		elInfo.atts = atts;
	}

	public void setParseLiteralMode() {
		parseLiteralMode = true;
		xmlLiteralStackHeight = 0;

		// All currently known namespace prefixes are
		// new for this XML literal.
		xmlLiteralPrefixes.clear();
		unknownPrefixesInXMLLiteral.clear();
	}

	private ParsedURI createBaseURI(String uriString) {
		if (uriString.length() > 4 && uriString.substring(0, 4).equalsIgnoreCase("jar:")) {
			// uriString is e.g.
			// jar:http://www.foo.com/bar/baz.jar!/COM/foo/Quux.class
			// Treat the part up to and including the exclamation mark as the
			// scheme and
			// the rest as the path to enable 'correct' resolving of relative URIs
			int idx = uriString.indexOf('!');
			if (idx != -1) {
				String scheme = uriString.substring(0, idx + 1);
				String path = uriString.substring(idx + 1);
				return new ParsedURI(scheme, null, path, null, null);
			}
		}

		ParsedURI uri = new ParsedURI(uriString);
		uri.normalize();
		return uri;
	}

	/*---------------------------------*
	 * Methods related to XML literals *
	 *---------------------------------*/

	/**
	 * Appends a start tag to charBuf. This method is used during the parsing of
	 * an XML Literal.
	 */
	private void appendStartTag(String qName, Attributes attributes) {
		// Write start of start tag
		charBuf.append("<" + qName);

		// Write any new namespace prefix definitions
		for (Map.Entry<String, String> entry : newNamespaceMappings.entrySet()) {
			String prefix = entry.getKey();
			String namespace = entry.getValue();
			appendNamespaceDecl(charBuf, prefix, namespace);
		}

		// Write attributes
		int attCount = attributes.getLength();
		for (int i = 0; i < attCount; i++) {
			appendAttribute(charBuf, attributes.getQName(i), attributes.getValue(i));
		}

		// Write end of start tag
		charBuf.append(">");

		// Check for any used prefixes that are not
		// defined in the XML literal itself
		int colonIdx = qName.indexOf(':');
		String prefix = (colonIdx > 0) ? qName.substring(0, colonIdx) : "";

		if (!xmlLiteralPrefixes.contains(prefix) && !unknownPrefixesInXMLLiteral.contains(prefix)) {
			unknownPrefixesInXMLLiteral.add(prefix);
		}
	}

	/**
	 * Appends an end tag to charBuf. This method is used during the parsing of
	 * an XML Literal.
	 */
	private void appendEndTag(String qName) {
		charBuf.append("</" + qName + ">");
	}

	/**
	 * Inserts prefix mappings from an XML Literal's context for all prefixes
	 * that are used in the XML Literal and that are not defined in the XML
	 * Literal itself.
	 */
	private void insertUsedContextPrefixes() {
		int unknownPrefixesCount = unknownPrefixesInXMLLiteral.size();

		if (unknownPrefixesCount > 0) {
			// Create a String with all needed context prefixes
			StringBuilder contextPrefixes = new StringBuilder(1024);
			ElementInfo topElement = peekStack();

			for (int i = 0; i < unknownPrefixesCount; i++) {
				String prefix = unknownPrefixesInXMLLiteral.get(i);
				String namespace = topElement.getNamespace(prefix);
				if (namespace != null) {
					appendNamespaceDecl(contextPrefixes, prefix, namespace);
				}
			}

			// Insert this String before the first '>' character
			int endOfFirstStartTag = charBuf.indexOf(">");
			charBuf.insert(endOfFirstStartTag, contextPrefixes.toString());
		}

		unknownPrefixesInXMLLiteral.clear();
	}

	private void appendNamespaceDecl(StringBuilder sb, String prefix, String namespace) {
		String attName = "xmlns";

		if (!"".equals(prefix)) {
			attName += ":" + prefix;
		}

		appendAttribute(sb, attName, namespace);
	}

	private void appendAttribute(StringBuilder sb, String name, String value) {
		sb.append(" ");
		sb.append(name);
		sb.append("=\"");
		sb.append(XMLUtil.escapeDoubleQuotedAttValue(value));
		sb.append("\"");
	}

	/*------------------------------------------*
	 * Methods related to the ElementInfo stack *
	 *------------------------------------------*/

	private ElementInfo peekStack() {
		ElementInfo result = null;

		if (!elInfoStack.empty()) {
			result = elInfoStack.peek();
		}

		return result;
	}

	/*----------------------------*
	 * Internal class ElementInfo *
	 *----------------------------*/

	private class ElementInfo {

		public String qName;

		public String namespaceURI;

		public String localName;

		public Atts atts;

		public ElementInfo parent;

		private Map<String, String> namespaceMap;

		public ParsedURI baseURI;

		public String xmlLang;

		public ElementInfo(String qName, String namespaceURI, String localName) {
			this(null, qName, namespaceURI, localName);
		}

		public ElementInfo(ElementInfo parent, String qName, String namespaceURI, String localName) {
			this.parent = parent;
			this.qName = qName;
			this.namespaceURI = namespaceURI;
			this.localName = localName;

			if (parent != null) {
				// Inherit baseURI and xmlLang from parent
				this.baseURI = parent.baseURI;
				this.xmlLang = parent.xmlLang;
			}
			else {
				this.baseURI = documentURI;
				this.xmlLang = "";
			}
		}

		public void setBaseURI(String uriString) {
			// Resolve the specified base URI against the inherited base URI
			baseURI = baseURI.resolve(createBaseURI(uriString));
		}

		public void setNamespaceMappings(Map<String, String> namespaceMappings) {
			if (namespaceMappings.isEmpty()) {
				namespaceMap = null;
			}
			else {
				namespaceMap = new HashMap<String, String>(namespaceMappings);
			}
		}

		public String getNamespace(String prefix) {
			String result = null;

			if (namespaceMap != null) {
				result = namespaceMap.get(prefix);
			}

			if (result == null && parent != null) {
				result = parent.getNamespace(prefix);
			}

			return result;
		}
	}
}
