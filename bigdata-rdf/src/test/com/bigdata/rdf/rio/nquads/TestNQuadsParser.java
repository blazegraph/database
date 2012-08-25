/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
/*
 * Created on Feb 27, 2012
 */

package com.bigdata.rdf.rio.nquads;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase2;

import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * Test suite for the {@link NQuadsParser}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestNQuadsParserFactory.java 6045 2012-02-27 17:33:44Z thompsonbry $
 */
public class TestNQuadsParser extends TestCase2 {

    /**
     * 
     */
    public TestNQuadsParser() {
    }

    /**
     * @param name
     */
    public TestNQuadsParser(String name) {
        super(name);
    }

    private RDFParser parser;

    protected void setUp() throws Exception {

        super.setUp();

        parser = (NQuadsParser) RDFParserRegistry.getInstance()
                .get(NQuadsParser.nquads).getParser();

        assertNotNull(parser);

    }
    
    protected void tearDown() throws Exception {
        
        super.tearDown();
        
        parser = null;
        
    }

    public void test_plainLiteral() throws RDFParseException, RDFHandlerException,
            IOException {

        final String baseUri = "http://www.w3.org/People/Berners-Lee/card.rdf";

        final StringBuilder data = new StringBuilder();

        data.append("<http://www.w3.org/People/Berners-Lee/card#i> "
                + "<http://xmlns.com/foaf/0.1/name> "
                + "\"Timothy Berners-Lee\" .\n");

        final Graph g = new GraphImpl();
        
        parser.setRDFHandler(new RDFHandlerBase() {
            public void handleStatement(Statement st)
                    throws RDFHandlerException {
                g.add(st);
            }
        });
        
        parser.parse(new StringReader(data.toString()), baseUri);

        assertEquals(1, g.size());
        
        final ValueFactory f = new ValueFactoryImpl();
        
        assertTrue(g.contains(new StatementImpl(//
                f.createURI("http://www.w3.org/People/Berners-Lee/card#i"),
                f.createURI("http://xmlns.com/foaf/0.1/name"),
                f.createLiteral("Timothy Berners-Lee")
                )));

    }

    public void test_plainLiteralWithEscapes() throws RDFParseException,
            RDFHandlerException, IOException {

        final String baseUri = "http://www.w3.org/People/Berners-Lee/card.rdf";

        final StringBuilder data = new StringBuilder();

        data.append("<http://www.w3.org/People/Berners-Lee/card#i> "
                + "<http://xmlns.com/foaf/0.1/name> "
                + "\"Timothy Berners-\\'Lee\" .\n");

        final Graph g = new GraphImpl();

        parser.setRDFHandler(new RDFHandlerBase() {
            public void handleStatement(Statement st)
                    throws RDFHandlerException {
                g.add(st);
            }
        });

        parser.parse(new StringReader(data.toString()), baseUri);

        assertEquals(1, g.size());

        final ValueFactory f = new ValueFactoryImpl();

        assertTrue(g.contains(new StatementImpl(//
                f.createURI("http://www.w3.org/People/Berners-Lee/card#i"), f
                        .createURI("http://xmlns.com/foaf/0.1/name"), f
                        .createLiteral("Timothy Berners-'Lee"))));

    }

    public void test_datatypeLiteral() throws RDFParseException,
            RDFHandlerException, IOException {

        final String baseUri = "http://www.w3.org/People/Berners-Lee/card.rdf";

        final StringBuilder data = new StringBuilder();

        data.append("<http://www.w3.org/People/Berners-Lee/card#i> "
                + "<http://xmlns.com/foaf/0.1/name> "
                + "\"Timothy Berners-Lee\"^^<http://www.w3.org/2001/XMLSchema#string> .\n");

        final Graph g = new GraphImpl();

        parser.setRDFHandler(new RDFHandlerBase() {
            public void handleStatement(Statement st)
                    throws RDFHandlerException {
                g.add(st);
            }
        });

        parser.parse(new StringReader(data.toString()), baseUri);

        assertEquals(1, g.size());

        final ValueFactory f = new ValueFactoryImpl();

        assertTrue(g
                .contains(new StatementImpl(
                        //
                        f.createURI("http://www.w3.org/People/Berners-Lee/card#i"),
                        f.createURI("http://xmlns.com/foaf/0.1/name"),
                        f.createLiteral(
                                "Timothy Berners-Lee",
                                f.createURI("http://www.w3.org/2001/XMLSchema#string")))));

    }

    /**
     * Unit test to verify uppercase language tag literals.
     * <p>
     * Note: nxparser 1.2.2 has a bug with the handling of upper case language
     * tags such as <code>"Up to 11.9"@FR</code> which motivated this unit test.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/590">
     *      nxparser fails with uppercase language tag </a>
     */
    public void test_languageTagLiteral() throws RDFParseException,
            RDFHandlerException, IOException {

        final String baseUri = "http://www.w3.org/People/Berners-Lee/card.rdf";

        final StringBuilder data = new StringBuilder();

        data.append("<http://www.w3.org/People/Berners-Lee/card#i> "
                + "<http://xmlns.com/foaf/0.1/name> "
                + "\"Timothy Berners-Lee\"@en .\n");

        final Graph g = new GraphImpl();

        parser.setRDFHandler(new RDFHandlerBase() {
            public void handleStatement(Statement st)
                    throws RDFHandlerException {
                g.add(st);
            }
        });

        parser.parse(new StringReader(data.toString()), baseUri);

        assertEquals(1, g.size());

        final ValueFactory f = new ValueFactoryImpl();

        assertTrue(g
                .contains(new StatementImpl(
                        //
                        f.createURI("http://www.w3.org/People/Berners-Lee/card#i"),
                        f.createURI("http://xmlns.com/foaf/0.1/name"),
                        f.createLiteral(
                                "Timothy Berners-Lee","EN"))));

    }

}
