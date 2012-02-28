/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import junit.framework.TestCase;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.helpers.StatementCollector;

/**
 * @author Arjohn Kampman
 */
/*
 * FIXME Drop this when we migrate to a modern junit. It exists because the
 * RDFWriterTest class does not extend TestCase in openrdf.
 */
public abstract class RDFWriterTest extends TestCase {

	protected RDFWriterFactory rdfWriterFactory;

	protected RDFParserFactory rdfParserFactory;

	protected RDFWriterTest(RDFWriterFactory writerF, RDFParserFactory parserF) {
		rdfWriterFactory = writerF;
		rdfParserFactory = parserF;
	}

//	@Test
	public void testRoundTrip()
		throws RDFHandlerException, IOException, RDFParseException
	{
		String ex = "http://example.org/";

		ValueFactory vf = new ValueFactoryImpl();
		BNode bnode = vf.createBNode("anon");
		URI uri1 = vf.createURI(ex, "uri1");
		URI uri2 = vf.createURI(ex, "uri2");
		Literal plainLit = vf.createLiteral("plain");
		Literal dtLit = vf.createLiteral(1);
		Literal langLit = vf.createLiteral("test", "en");
		Literal litWithNewline = vf.createLiteral("literal with newline\n");

		Statement st1 = vf.createStatement(bnode, uri1, plainLit);
		Statement st2 = vf.createStatement(uri1, uri2, langLit, uri2);
		Statement st3 = vf.createStatement(uri1, uri2, dtLit);
		Statement st4 = vf.createStatement(uri1, uri2, litWithNewline);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		RDFWriter rdfWriter = rdfWriterFactory.getWriter(out);
		rdfWriter.handleNamespace("ex", ex);
		rdfWriter.startRDF();
		rdfWriter.handleStatement(st1);
		rdfWriter.handleStatement(st2);
		rdfWriter.handleStatement(st3);
		rdfWriter.handleStatement(st4);
		rdfWriter.endRDF();

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		RDFParser rdfParser = rdfParserFactory.getParser();
		rdfParser.setValueFactory(vf);
		StatementCollector stCollector = new StatementCollector();
		rdfParser.setRDFHandler(stCollector);

		rdfParser.parse(in, "foo:bar");

		Collection<Statement> statements = stCollector.getStatements();
		assertEquals("Unexpected number of statements", 4, statements.size());
//		assertTrue(statements.contains(st1));
		assertTrue(statements.contains(st2));
		assertTrue(statements.contains(st3));
		assertTrue("missing statement with literal ending on newline", statements.contains(st4));
	}

//	@Test
	public void testPrefixRedefinition()
		throws RDFHandlerException, RDFParseException, IOException
	{
		String ns1 = "a:";
		String ns2 = "b:";
		String ns3 = "c:";

		ValueFactory vf = new ValueFactoryImpl();
		URI uri1 = vf.createURI(ns1, "r1");
		URI uri2 = vf.createURI(ns2, "r2");
		URI uri3 = vf.createURI(ns3, "r3");
		Statement st = vf.createStatement(uri1, uri2, uri3);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		RDFWriter rdfWriter = rdfWriterFactory.getWriter(out);
		rdfWriter.handleNamespace("", ns1);
		rdfWriter.handleNamespace("", ns2);
		rdfWriter.handleNamespace("", ns3);
		rdfWriter.startRDF();
		rdfWriter.handleStatement(st);
		rdfWriter.endRDF();

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		RDFParser rdfParser = rdfParserFactory.getParser();
		rdfParser.setValueFactory(vf);
		StatementCollector stCollector = new StatementCollector();
		rdfParser.setRDFHandler(stCollector);

		rdfParser.parse(in, "foo:bar");

		Collection<Statement> statements = stCollector.getStatements();
		assertEquals("Unexpected number of statements", 1, statements.size());

		Statement parsedSt = statements.iterator().next();
		assertEquals("Written and parsed statements are not equal", st, parsedSt);
	}

//	@Test
	public void testIllegalPrefix()
		throws RDFHandlerException, RDFParseException, IOException
	{
		String ns1 = "a:";
		String ns2 = "b:";
		String ns3 = "c:";

		ValueFactory vf = new ValueFactoryImpl();
		URI uri1 = vf.createURI(ns1, "r1");
		URI uri2 = vf.createURI(ns2, "r2");
		URI uri3 = vf.createURI(ns3, "r3");
		Statement st = vf.createStatement(uri1, uri2, uri3);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		RDFWriter rdfWriter = rdfWriterFactory.getWriter(out);
		rdfWriter.handleNamespace("1", ns1);
		rdfWriter.handleNamespace("_", ns2);
		rdfWriter.handleNamespace("a%", ns3);
		rdfWriter.startRDF();
		rdfWriter.handleStatement(st);
		rdfWriter.endRDF();

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		RDFParser rdfParser = rdfParserFactory.getParser();
		rdfParser.setValueFactory(vf);
		StatementCollector stCollector = new StatementCollector();
		rdfParser.setRDFHandler(stCollector);

		rdfParser.parse(in, "foo:bar");

		Collection<Statement> statements = stCollector.getStatements();
		assertEquals("Unexpected number of statements", 1, statements.size());

		Statement parsedSt = statements.iterator().next();
		assertEquals("Written and parsed statements are not equal", st, parsedSt);
	}

//	@Test
	public void testDefaultNamespace()
		throws Exception
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		RDFWriter rdfWriter = rdfWriterFactory.getWriter(out);
		rdfWriter.handleNamespace("", RDF.NAMESPACE);
		rdfWriter.handleNamespace("rdf", RDF.NAMESPACE);
		rdfWriter.startRDF();
		rdfWriter.handleStatement(new StatementImpl(new URIImpl(RDF.NAMESPACE), RDF.TYPE, OWL.ONTOLOGY));
		rdfWriter.endRDF();
	}
}
