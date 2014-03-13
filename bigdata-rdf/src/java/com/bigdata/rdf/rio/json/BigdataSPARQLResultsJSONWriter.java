/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.json;

import info.aduna.io.IndentingWriter;
import info.aduna.text.StringUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;

/**
 * A TupleQueryResultWriter that writes query results in the <a
 * href="http://www.w3.org/TR/rdf-sparql-json-res/">SPARQL Query Results JSON
 * Format</a>.
 */
public class BigdataSPARQLResultsJSONWriter implements TupleQueryResultWriter, RDFWriter {

	/*-----------*
	 * Variables *
	 *-----------*/

	private IndentingWriter writer;

	private boolean firstTupleWritten;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public BigdataSPARQLResultsJSONWriter(OutputStream out) {
		this(new OutputStreamWriter(out, Charset.forName("UTF-8")));
	}

	public BigdataSPARQLResultsJSONWriter(Writer w) {
		w = new BufferedWriter(w, 1024);
		writer = new IndentingWriter(w);
	}

	/*---------*
	 * Methods *
	 *---------*/

	/**
	 * This is the only method that is different from the OpenRDF version.
	 * I could not subclass their implementation because the IndentingWriter
	 * is private.
	 */
	private void writeValue(Value value)
			throws IOException, TupleQueryResultHandlerException
	{
		writer.write("{ ");

		if (value instanceof URI) {
			writeKeyValue("type", "uri");
			writer.write(", ");
			writeKeyValue("value", ((URI)value).toString());
		}
		else if (value instanceof BigdataBNode &&
				 ((BigdataBNode) value).isStatementIdentifier()) {
			
//			    "bindings": [
//			                 {
//			                   "book": { "type": "uri" , "value": "http://example.org/book/book6" } ,
//			                   "title": { "type": "literal" , "value": "Harry Potter and the Half-Blood Prince" }
//			                 } ,
//			                 {
//			                   "book": { "type": "sid" , "value": 
//				                           { 
//				                              "s": { "type": "uri" , "value": "<s>" } , 
//				                              "p": { "type": "uri" , "value": "<p>" } ,
//	                                        "o": { "type": "uri" , "value": "<o>" }
//			                               }
//				                        }
//			                   "title": { "type": "literal" , "value": "Harry Potter and the Deathly Hallows" }
//			                 } ,			
			
			final BigdataBNode bnode = (BigdataBNode) value;
			final BigdataStatement stmt = bnode.getStatement(); 
			writeKeyValue("type", "sid");
			writer.write(", ");
			writeKey("value");
			openBraces();
			writeKeyValue("sid-s", stmt.getSubject());
			writeComma();
			writeKeyValue("sid-p", stmt.getPredicate());
			writeComma();
			writeKeyValue("sid-o", stmt.getObject());
			
			if (stmt.getContext() != null) {
				writeComma();
				writeKeyValue("sid-c", stmt.getContext());
			}
			closeBraces();
			
		}
		else if (value instanceof BNode) {
			writeKeyValue("type", "bnode");
			writer.write(", ");
			writeKeyValue("value", ((BNode)value).getID());
		}
		else if (value instanceof Literal) {
			Literal lit = (Literal)value;

			if (lit.getDatatype() != null) {
				writeKeyValue("type", "typed-literal");
				writer.write(", ");
				writeKeyValue("datatype", lit.getDatatype().toString());
			}
			else {
				writeKeyValue("type", "literal");
				if (lit.getLanguage() != null) {
					writer.write(", ");
					writeKeyValue("xml:lang", lit.getLanguage());
				}
			}

			writer.write(", ");
			writeKeyValue("value", lit.getLabel());
		}
		else {
			throw new TupleQueryResultHandlerException("Unknown Value object type: " + value.getClass());
		}

		writer.write(" }");
	}
	
	
	public final TupleQueryResultFormat getTupleQueryResultFormat() {
		return TupleQueryResultFormat.JSON;
	}

	public void startRDF() {
		
		try {
			
			startQueryResult(Arrays.asList(new String[] {
				"s", "p", "o", "c"	
			}));
			
		} catch (TupleQueryResultHandlerException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public void startQueryResult(List<String> columnHeaders)
		throws TupleQueryResultHandlerException
	{
		try {
			openBraces();

			// Write header
			writeKey("head");
			openBraces();
			writeKeyValue("vars", columnHeaders);
			closeBraces();

			writeComma();

			// Write results
			writeKey("results");
			openBraces();

			writeKey("bindings");
			openArray();

			firstTupleWritten = false;
		}
		catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	public void endRDF() {
		
		try {
			
			endQueryResult();
			
		} catch (TupleQueryResultHandlerException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public void endQueryResult()
		throws TupleQueryResultHandlerException
	{
		try {
			closeArray(); // bindings array
			closeBraces(); // results braces
			closeBraces(); // root braces
			writer.flush();
		}
		catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	public void handleStatement(final Statement stmt)
	{
		try {
			if (firstTupleWritten) {
				writeComma();
			}
			else {
				firstTupleWritten = true;
			}

			openBraces(); // start of new solution

			writeKeyValue("s", stmt.getSubject());
			writeComma();
			writeKeyValue("p", stmt.getPredicate());
			writeComma();
			writeKeyValue("o", stmt.getObject());
			if (stmt.getContext() != null) {
				writeComma();
				writeKeyValue("c", stmt.getContext());
			}
			
//			Iterator<Binding> bindingIter = bindingSet.iterator();
//			while (bindingIter.hasNext()) {
//				Binding binding = bindingIter.next();
//
//				writeKeyValue(binding.getName(), binding.getValue());
//
//				if (bindingIter.hasNext()) {
//					writeComma();
//				}
//			}

			closeBraces(); // end solution

			writer.flush();
		}
		catch (TupleQueryResultHandlerException e) {
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void handleSolution(BindingSet bindingSet)
		throws TupleQueryResultHandlerException
	{
		try {
			if (firstTupleWritten) {
				writeComma();
			}
			else {
				firstTupleWritten = true;
			}

			openBraces(); // start of new solution

			Iterator<Binding> bindingIter = bindingSet.iterator();
			while (bindingIter.hasNext()) {
				Binding binding = bindingIter.next();

				writeKeyValue(binding.getName(), binding.getValue());

				if (bindingIter.hasNext()) {
					writeComma();
				}
			}

			closeBraces(); // end solution

			writer.flush();
		}
		catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	private void writeKeyValue(String key, String value)
		throws IOException
	{
		writeKey(key);
		writeString(value);
	}

	private void writeKeyValue(String key, Value value)
		throws IOException, TupleQueryResultHandlerException
	{
		writeKey(key);
		writeValue(value);
	}

	private void writeKeyValue(String key, Iterable<String> array)
		throws IOException
	{
		writeKey(key);
		writeArray(array);
	}

	private void writeKey(String key)
		throws IOException
	{
		writeString(key);
		writer.write(": ");
	}

//	private void writeValue(Value value)
//		throws IOException, TupleQueryResultHandlerException
//	{
//		writer.write("{ ");
//
//		if (value instanceof URI) {
//			writeKeyValue("type", "uri");
//			writer.write(", ");
//			writeKeyValue("value", ((URI)value).toString());
//		}
//		else if (value instanceof BNode) {
//			writeKeyValue("type", "bnode");
//			writer.write(", ");
//			writeKeyValue("value", ((BNode)value).getID());
//		}
//		else if (value instanceof Literal) {
//			Literal lit = (Literal)value;
//
//			if (lit.getDatatype() != null) {
//				writeKeyValue("type", "typed-literal");
//				writer.write(", ");
//				writeKeyValue("datatype", lit.getDatatype().toString());
//			}
//			else {
//				writeKeyValue("type", "literal");
//				if (lit.getLanguage() != null) {
//					writer.write(", ");
//					writeKeyValue("xml:lang", lit.getLanguage());
//				}
//			}
//
//			writer.write(", ");
//			writeKeyValue("value", lit.getLabel());
//		}
//		else {
//			throw new TupleQueryResultHandlerException("Unknown Value object type: " + value.getClass());
//		}
//
//		writer.write(" }");
//	}

	private void writeString(String value)
		throws IOException
	{
		// Escape special characters
		value = StringUtil.gsub("\\", "\\\\", value);
		value = StringUtil.gsub("\"", "\\\"", value);
		value = StringUtil.gsub("/", "\\/", value);
		value = StringUtil.gsub("\b", "\\b", value);
		value = StringUtil.gsub("\f", "\\f", value);
		value = StringUtil.gsub("\n", "\\n", value);
		value = StringUtil.gsub("\r", "\\r", value);
		value = StringUtil.gsub("\t", "\\t", value);

		writer.write("\"");
		writer.write(value);
		writer.write("\"");
	}

	private void writeArray(Iterable<String> array)
		throws IOException
	{
		writer.write("[ ");

		Iterator<String> iter = array.iterator();
		while (iter.hasNext()) {
			String value = iter.next();

			writeString(value);

			if (iter.hasNext()) {
				writer.write(", ");
			}
		}

		writer.write(" ]");
	}

	private void openArray()
		throws IOException
	{
		writer.write("[");
		writer.writeEOL();
		writer.increaseIndentation();
	}

	private void closeArray()
		throws IOException
	{
		writer.writeEOL();
		writer.decreaseIndentation();
		writer.write("]");
	}

	private void openBraces()
		throws IOException
	{
		writer.write("{");
		writer.writeEOL();
		writer.increaseIndentation();
	}

	private void closeBraces()
		throws IOException
	{
		writer.writeEOL();
		writer.decreaseIndentation();
		writer.write("}");
	}

	private void writeComma()
		throws IOException
	{
		writer.write(", ");
		writer.writeEOL();
	}

	@Override
	public void handleComment(String arg0) throws RDFHandlerException {
		// TODO Implement me
		
	}

	@Override
	public void handleNamespace(String arg0, String arg1)
			throws RDFHandlerException {
		// TODO Implement me
		
	}

	@Override
	public RDFFormat getRDFFormat() {
		
		return null;
		
	}
}
