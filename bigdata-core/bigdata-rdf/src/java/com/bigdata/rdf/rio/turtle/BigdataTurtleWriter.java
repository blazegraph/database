/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.turtle;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import org.openrdf.model.BNode;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.turtle.TurtleWriter;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;

/**
 * An implementation of the RDFWriter interface that writes RDF documents in
 * Turtle format. The Turtle format is defined in <a
 * href="http://www.dajobe.org/2004/01/turtle/">in this document</a>.
 * @openrdf
 */
public class BigdataTurtleWriter extends TurtleWriter implements RDFWriter {

	/**
	 * Creates a new TurtleWriter that will write to the supplied OutputStream.
	 * 
	 * @param out
	 *        The OutputStream to write the Turtle document to.
	 */
	public BigdataTurtleWriter(OutputStream out) {
		super(out);
	}

	/**
	 * Creates a new TurtleWriter that will write to the supplied Writer.
	 * 
	 * @param writer
	 *        The Writer to write the Turtle document to.
	 */
	public BigdataTurtleWriter(Writer writer) {
		super(writer);
	}

	protected void writeBNode(BNode bNode)
		throws IOException
	{
		if (bNode instanceof BigdataBNode && 
				((BigdataBNode) bNode).isStatementIdentifier()) {
			writeSid((BigdataBNode) bNode);
		} else {
			super.writeBNode(bNode);
		}
	}

	protected void writeSid(final BigdataBNode sid)
		throws IOException
	{
		final BigdataStatement stmt = sid.getStatement();
		writer.write("<< ");
		writeValue(stmt.getSubject());
		writer.write(", ");
		writeValue(stmt.getPredicate());
		writer.write(", ");
		writeValue(stmt.getObject());
		if (stmt.getContext() != null) {
			writer.write(", ");
			writeValue(stmt.getContext());
		}
		writer.write(" >>");
	}

}
