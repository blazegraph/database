/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.rio.rdfxml;

import info.aduna.xml.XMLUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.BD;

/**
 * An implementation of the RDFWriter interface that writes RDF documents in
 * XML-serialized RDF format using a private extension of designed to support
 * the interchange of statement-level provenance.
 * 
 * @see BD#SID
 */
public class RDFXMLWriter implements RDFWriter {

	/*-----------*
	 * Variables *
	 *-----------*/

	protected Writer writer;

	protected Map<String, String> namespaceTable;

	protected boolean writingStarted;

	protected boolean headerWritten;

	protected Resource lastWrittenSubject;

	/*--------------*
	 * Constructors *
	 *--------------*/

	/**
	 * Creates a new RDFXMLWriter that will write to the supplied OutputStream.
	 * 
	 * @param out
	 *        The OutputStream to write the RDF/XML document to.
	 */
	public RDFXMLWriter(OutputStream out) {
		this(new OutputStreamWriter(out, Charset.forName("UTF-8")));
	}

	/**
	 * Creates a new RDFXMLWriter that will write to the supplied Writer.
	 * 
	 * @param writer
	 *        The Writer to write the RDF/XML document to.
	 */
	public RDFXMLWriter(Writer writer) {
		this.writer = writer;
		namespaceTable = new LinkedHashMap<String, String>();
		writingStarted = false;
		headerWritten = false;
		lastWrittenSubject = null;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public RDFFormat getRDFFormat() {
		return RDFFormat.RDFXML;
	}

	public void startRDF() {
		if (writingStarted) {
			throw new RuntimeException("Document writing has already started");
		}
		writingStarted = true;
	}

	protected void writeHeader()
		throws IOException
	{
		try {
			// This export format needs the RDF namespace to be defined, add a
			// prefix for it if there isn't one yet.
			setNamespace("rdf", RDF.NAMESPACE, false);

            // Namespace the [graph] attribute.
            setNamespace("bigdata", BD.NAMESPACE, false);

			writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

			writeStartOfStartTag(RDF.NAMESPACE, "RDF");

			for (Map.Entry<String, String> entry : namespaceTable.entrySet()) {
				String name = entry.getKey();
				String prefix = entry.getValue();

				writeNewLine();
				writeIndent();
				writer.write("xmlns");
				if (prefix.length() > 0) {
					writer.write(':');
					writer.write(prefix);
				}
				writer.write("=\"");
				writer.write(XMLUtil.escapeDoubleQuotedAttValue(name));
				writer.write("\"");
			}

			writeEndOfStartTag();

			writeNewLine();
		}
		finally {
			headerWritten = true;
		}
	}

	public void endRDF()
		throws RDFHandlerException
	{
		if (!writingStarted) {
			throw new RuntimeException("Document writing has not yet started");
		}

		try {
			if (!headerWritten) {
				writeHeader();
			}

			flushPendingStatements();

			writeNewLine();
			writeEndTag(RDF.NAMESPACE, "RDF");

			writer.flush();
		}
		catch (IOException e) {
			throw new RDFHandlerException(e);
		}
		finally {
			writingStarted = false;
			headerWritten = false;
		}
	}

	public void handleNamespace(String prefix, String name) {
		setNamespace(prefix, name, false);
	}

	protected void setNamespace(String prefix, String name, boolean fixedPrefix) {
		if (headerWritten) {
			// Header containing namespace declarations has already been written
			return;
		}

		if (!namespaceTable.containsKey(name)) {
			// Namespace not yet mapped to a prefix, try to give it the specified
			// prefix
			
			boolean isLegalPrefix = prefix.length() == 0 || XMLUtil.isNCName(prefix);
			
			if (!isLegalPrefix || namespaceTable.containsValue(prefix)) {
				// Specified prefix is not legal or the prefix is already in use,
				// generate a legal unique prefix

				if (fixedPrefix) {
					if (isLegalPrefix) {
						throw new IllegalArgumentException("Prefix is already in use: " + prefix);
					}
					else {
						throw new IllegalArgumentException("Prefix is not a valid XML namespace prefix: " + prefix);
					}
				}

				if (prefix.length() == 0 || !isLegalPrefix) {
					prefix = "ns";
				}

				int number = 1;

				while (namespaceTable.containsValue(prefix + number)) {
					number++;
				}

				prefix += number;
			}

			namespaceTable.put(name, prefix);
		}
	}

	public void handleStatement(Statement st)
		throws RDFHandlerException
	{
		if (!writingStarted) {
			throw new RuntimeException("Document writing has not yet been started");
		}

		Resource subj = st.getSubject();
		URI pred = st.getPredicate();
		Value obj = st.getObject();
        Resource context = st.getContext();

		// Verify that an XML namespace-qualified name can be created for the
		// predicate
		String predString = pred.toString();
		int predSplitIdx = XMLUtil.findURISplitIndex(predString);
		if (predSplitIdx == -1) {
			throw new RDFHandlerException("Unable to create XML namespace-qualified name for predicate: "
					+ predString);
		}

		String predNamespace = predString.substring(0, predSplitIdx);
		String predLocalName = predString.substring(predSplitIdx);

		try {
			if (!headerWritten) {
				writeHeader();
			}

			// SUBJECT
			if (!subj.equals(lastWrittenSubject)) {
				flushPendingStatements();

				// Write new subject:
				writeNewLine();
				writeStartOfStartTag(RDF.NAMESPACE, "Description");
				if (subj instanceof BNode) {
					BNode bNode = (BNode)subj;
					writeAttribute(RDF.NAMESPACE, "nodeID", bNode.getID());
				}
				else {
					URI uri = (URI)subj;
					writeAttribute(RDF.NAMESPACE, "about", uri.toString());
				}
				writeEndOfStartTag();
				writeNewLine();

				lastWrittenSubject = subj;
			}

			// PREDICATE
			writeIndent();
			writeStartOfStartTag(predNamespace, predLocalName);

            // CONTEXT
            if(context != null) {
                /*
                 * Vendor specific extension interprets the context as a
                 * statement identifier and serializes it as a blank node using
                 * a custom XML attribute.
                 */
                if (context instanceof BNode) {
                    BNode bNode = (BNode)context;
                    writeAttribute(BD.NAMESPACE, BD.SID.getLocalName(), bNode.getID());
                }
                else {
                    URI uri = (URI)context;
                    writeAttribute(BD.NAMESPACE, BD.SID.getLocalName(), uri.toString());
                }
            }
            
            // AXIOM, INFERRED, or EXPLICIT
            if( st instanceof BigdataStatement ) {
                
                StatementEnum type = ((BigdataStatement)st).getStatementType();
                
                if(type != null) {
                    
                    writeAttribute(BD.NAMESPACE, BD.STATEMENT_TYPE.getLocalName(), type.toString());
                    
                }
                
            }
            
			// OBJECT
			if (obj instanceof Resource) {
				Resource objRes = (Resource)obj;

				if (objRes instanceof BNode) {
					BNode bNode = (BNode)objRes;
					writeAttribute(RDF.NAMESPACE, "nodeID", bNode.getID());
				}
				else {
					URI uri = (URI)objRes;
					writeAttribute(RDF.NAMESPACE, "resource", uri.toString());
				}

				writeEndOfEmptyTag();
			}
			else if (obj instanceof Literal) {
				Literal objLit = (Literal)obj;

				// language attribute
				if (objLit.getLanguage() != null) {
					writeAttribute("xml:lang", objLit.getLanguage());
				}

				// datatype attribute
				boolean isXMLLiteral = false;
				URI datatype = objLit.getDatatype();
				if (datatype != null) {
					// Check if datatype is rdf:XMLLiteral
					isXMLLiteral = datatype.equals(RDF.XMLLITERAL);

					if (isXMLLiteral) {
						writeAttribute(RDF.NAMESPACE, "parseType", "Literal");
					}
					else {
						writeAttribute(RDF.NAMESPACE, "datatype", datatype.toString());
					}
				}

				writeEndOfStartTag();

				// label
				if (isXMLLiteral) {
					// Write XML literal as plain XML
					writer.write(objLit.getLabel());
				}
				else {
					writeCharacterData(objLit.getLabel());
				}

				writeEndTag(predNamespace, predLocalName);
			}

			writeNewLine();

			// Don't write </rdf:Description> yet, maybe the next statement
			// has the same subject.
		}
		catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	public void handleComment(String comment)
		throws RDFHandlerException
	{
		try {
			if (!headerWritten) {
				writeHeader();
			}

			flushPendingStatements();

			writer.write("<!-- ");
			writer.write(comment);
			writer.write(" -->");
			writeNewLine();
		}
		catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	protected void flushPendingStatements()
		throws IOException
	{
		if (lastWrittenSubject != null) {
			// The last statement still has to be closed:
			writeEndTag(RDF.NAMESPACE, "Description");
			writeNewLine();

			lastWrittenSubject = null;
		}
	}

	protected void writeStartOfStartTag(String namespace, String localName)
		throws IOException
	{
		String prefix = namespaceTable.get(namespace);

		if (prefix == null) {
			writer.write("<");
			writer.write(localName);
			writer.write(" xmlns=\"");
			writer.write(XMLUtil.escapeDoubleQuotedAttValue(namespace));
			writer.write("\"");
		}
		else if (prefix.length() == 0) {
			// default namespace
			writer.write("<");
			writer.write(localName);
		}
		else {
			writer.write("<");
			writer.write(prefix);
			writer.write(":");
			writer.write(localName);
		}
	}

	protected void writeAttribute(String attName, String value)
		throws IOException
	{
		writer.write(" ");
		writer.write(attName);
		writer.write("=\"");
		writer.write(XMLUtil.escapeDoubleQuotedAttValue(value));
		writer.write("\"");
	}

	protected void writeAttribute(String namespace, String attName, String value)
		throws IOException
	{
		String prefix = namespaceTable.get(namespace);

		if (prefix == null || prefix.length() == 0) {
			throw new RuntimeException("No prefix has been declared for the namespace used in this attribute: "
					+ namespace);
		}

		writer.write(" ");
		writer.write(prefix);
		writer.write(":");
		writer.write(attName);
		writer.write("=\"");
		writer.write(XMLUtil.escapeDoubleQuotedAttValue(value));
		writer.write("\"");
	}

	protected void writeEndOfStartTag()
		throws IOException
	{
		writer.write(">");
	}

	protected void writeEndOfEmptyTag()
		throws IOException
	{
		writer.write("/>");
	}

	protected void writeEndTag(String namespace, String localName)
		throws IOException
	{
		String prefix = namespaceTable.get(namespace);

		if (prefix == null || prefix.length() == 0) {
			writer.write("</");
			writer.write(localName);
			writer.write(">");
		}
		else {
			writer.write("</");
			writer.write(prefix);
			writer.write(":");
			writer.write(localName);
			writer.write(">");
		}
	}

	protected void writeCharacterData(String chars)
		throws IOException
	{
		writer.write(XMLUtil.escapeCharacterData(chars));
	}

	protected void writeIndent()
		throws IOException
	{
		writer.write("\t");
	}

	protected void writeNewLine()
		throws IOException
	{
		writer.write("\n");
	}
}
