/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 11, 2008
 */

package com.bigdata.rdf.rio;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Properties;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
//import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for correct handling of blank nodes and statement identifiers
 * during interchange of RDF/XML using the custom extensions described by
 * {@link BD#SID}. When statement identifers are NOT enabled, the test suite
 * instead verifies that blank nodes are handled propertly (that is, that the
 * RDF/XML parser assigns term identifiers to blank nodes instead of unifying
 * them with statement identifiers in the KB).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRDFXMLInterchangeWithStatementIdentifiers extends
        AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestRDFXMLInterchangeWithStatementIdentifiers() {
    }

    /**
     * @param name
     */
    public TestRDFXMLInterchangeWithStatementIdentifiers(String name) {
        super(name);
    }
    
    /**
     * Test case builds up a graph from Sesame {@link Value} objects, using
     * {@link BNode}s to create statements about statements. The state of the
     * graph is verified. The explicit statements in the graph are then
     * serialized using a vendor specific RDF/XML extension and de-serialized
     * into a {@link TempTripleStore}. The state of the de-serialized graph is
     * then verified to confirm that the statements about statements were
     * correctly re-constructed.
     * 
     * @throws SailException
     * @throws RDFHandlerException
     * @throws IOException
     */
    public void test_rdfXmlInterchange() throws RDFHandlerException,
            IOException {

        final AbstractTripleStore store = getStore();

        try {

            if (!store.getStatementIdentifiers()) {

                log.warn("Statement identifiers not enabled - skipping test");
                
                return;
                
            }
            
            doStatementIdentifiersTest(store);

        } finally {

            store.__tearDownUnitTest();

        }

    }
    
    /**
     * Test verifies the correct handling of blank nodes appearing in an RDF/XML
     * document regardless of whether or not statement identifers are enabled.
     * Instances of the same blank node identifier in the scope of the
     * interchanged document are unified and then assigned a unique term
     * identifier by the knowledge base.
     * 
     * @throws SailException
     * @throws RDFHandlerException
     * @throws IOException
     */
    public void test_blankNodeHandling() throws RDFHandlerException,
            IOException {

        final AbstractTripleStore store = getStore();

        try {

            new BlankNodeTester(store).doTest();
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }

    /**
     * Test helper.
     */
    private class BlankNodeTester {
        
        private final AbstractTripleStore store;
        
        public BlankNodeTester(AbstractTripleStore store) {

            this.store = store;
            
        }
        
        /*
         * Note: These are Sesame Value impls. The term identifiers assigned by
         * the KB are NOT set on these objects as a side-effect.
         */

        final URI x = new URIImpl("http://www.foo.org/x");
        final URI Software = new URIImpl("http://www.foo.org/Software");

        final URI rdfType = RDF.TYPE;
        final URI rdfsLabel = RDFS.LABEL;
        final URI dcCreator = new URIImpl("http://purl.org/dc/terms/creator");

        final Literal SYSTAP = new LiteralImpl("SYSTAP, LLC");
        final Literal bigdata = new LiteralImpl("bigdata");
        final Literal java = new LiteralImpl("java");

        final BNode _b1 = new BNodeImpl("_S1");
        final BNode _systap = new BNodeImpl("_systap");
        final BNode _bigdata = new BNodeImpl("_bigdata");

        /**
         * Accepts a triple pattern and returns the matching statement. Throws
         * an exception if there is no matching statement or if there is more
         * than one matching statement.
         * 
         * @param store
         * @param s
         * @param p
         * @param o
         * @return
         * @throws SailException
         */
        public BigdataStatement getOnlyStatement(AbstractTripleStore store,
                Resource s, URI p, Value o) {

            final BigdataStatementIterator itr = store.getStatements(s, p, o);

            try {

                if (!itr.hasNext()) {

                    fail("Expecting statement: <"+s+", "+p+", "+o+">");

                }

                final BigdataStatement stmt = itr.next();
                
                log.info("Found: " + stmt + " given  <" + s + ", " + p + ", " + o
                        + ">");

                if(itr.hasNext()) {
                
                    final BigdataStatement stmt2 = itr.next();
                        
                    fail("Not expecting more statements: <" + s + ", " + p + ", "
                            + o + ">" + " : have " + stmt
                            + ", but will also visit " + stmt2);
                    
                }
                
                return stmt;

            } finally {

                itr.close();

            }

        }
        
        /**
         * Accepts a triple pattern and returns the matching statement. Throws
         * an exception if there is no matching statement or if there is more
         * than one matching statement.
         * 
         * @param store
         * @param s
         * @param p
         * @param o
         * @return
         * @throws SailException
         */
        private BigdataStatement getOnlyStatement(AbstractTripleStore store,
                long s, long p, long o) {

            final IChunkedOrderedIterator<ISPO> itr = store.getAccessPath(s, p,
                    o).iterator();

            try {

                if (!itr.hasNext()) {

                    fail("Expecting statement: <" + s + ", " + p + ", " + o + ">");

                }

                final BigdataStatement stmt = store.asStatement(itr.next());

                log.info("Found: " + stmt + " given  <" + s + ", " + p + ", " + o
                        + ">");
                
                if (itr.hasNext()) {

                    final BigdataStatement stmt2 = store.asStatement(itr.next());

                    fail("Not expecting more statements: <" + s + ", " + p + ", "
                            + o + ">" + ", have " + stmt + ", but will also visit "
                            + stmt2);

                }

                return stmt;

            } finally {

                itr.close();

            }

        }

       /**
        * Load the KB.
        */
       private void loadData(AbstractTripleStore store) {
           
           StatementBuffer buf = new StatementBuffer(store, 100/* capacity */);

           // fully grounded.
           buf.add(x, rdfType, Software); // stmt1
           
           // statements using [_bigdata] and/or [_systap] blank nodes.
           buf.add(_systap, rdfsLabel, SYSTAP);   // stmt2
           buf.add(_systap, dcCreator, _bigdata); // stmt3
           buf.add(_bigdata, rdfType, Software);  // stmt4 
           buf.add(_bigdata, rdfsLabel, bigdata); // stmt5

           // statement using a distinct blank node [_b1].
           buf.add(_b1, rdfsLabel, java);         // stmt6
           buf.add(_b1, rdfType, Software);       // stmt7

           // Note: (?,rdfType,Software) has THREE (3) solutions !

           /*
            * Flush to the database, resolving statement identifiers as
            * necessary.
            */
           buf.flush();
           
           if (log.isInfoEnabled())
                log.info("after load:\n" + store.dumpStore());

       }
       
       /**
         * Verify the structure of the graph.
         * 
         * @throws SailException
         */
       private void verifyGraph(AbstractTripleStore store) {

            /*
             * First, verify the fully grounded stmt.
             */

            final BigdataStatement stmt1 = store.getStatement(x, rdfType,
                    Software);

            assertNotNull(stmt1);

            /*
             * Now, do some queries and verify the statements that were using
             * blank nodes. As we go we figure out the term identifers assigned
             * to the blank nodes and make certain that the right blank nodes
             * have been unified (they have been assigned the same term
             * identifiers).
             */

            /*
             * There should be only one stmt matching this triple pattern.
             * 
             * stmt2.s gives us the term identifer for [_systap].
             */
            final BigdataStatement stmt2 = getOnlyStatement(store, null,
                    rdfsLabel, SYSTAP);

            /*
             * Verify that there are two solutions for (?, rdf:type, Software).
             */
            {
                
                final IAccessPath<ISPO> ap = store.getAccessPath(null, rdfType,
                        Software);
                
                log.info(store.dumpStatements(ap).toString());
                
                assertEquals("rangeCount", 3L, ap.rangeCount(true/* exact */));
                
            }
            
            /*
             * again, only one matching statement. Note that we are using the
             * term identifiers here, including the one discovered for stmt2.s
             * above. this gives us the term identifier for [_bigdata] as
             * stmt3.o.
             */
            final BigdataStatement stmt3 = getOnlyStatement(store, stmt2
                    .getSubject().getTermId(), store.getTermId(dcCreator), NULL);

            /*
             * only one matching statement using the term identifier for
             * [_bigdata].
             */
            final BigdataStatement stmt4 = getOnlyStatement(store, stmt3
                    .getObject().getTermId(), store.getTermId(rdfType), store
                    .getTermId(Software));

            /*
             * only one match statement using an independent query that gives us
             * the term identifier for [_bigdata] (stmt5.s).
             */
            final BigdataStatement stmt5 = getOnlyStatement(store, NULL, store
                    .getTermId(rdfsLabel), store.getTermId(bigdata));

            // verify term identifier.
            assertEquals(stmt5.getSubject().getTermId(), stmt3.getObject()
                    .getTermId());

            // verify term identifier.
            assertEquals(stmt5.getSubject().getTermId(), stmt4.getSubject()
                    .getTermId());

            /*
             * Query has only one solution and gives us the term identifier for
             * the [_b1] blank node.
             */
            final BigdataStatement stmt6 = getOnlyStatement(store, null,
                    rdfsLabel, java);

            /*
             * Query using the discovered term identifier for [_b1]. Note that
             * there are two statements matching (?, rdf:type, Software), but
             * only one each with each of the distinct blank nodes.
             */
            final BigdataStatement stmt7 = getOnlyStatement(store, stmt6
                    .getSubject().getTermId(), store.getTermId(rdfType), store
                    .getTermId(Software));

        }

        /**
         * Run the test.
         * 
         * @throws SailException
         * @throws RDFHandlerException
         * @throws IOException
         */
        protected void doTest() throws RDFHandlerException, IOException {

            loadData(store);

            verifyGraph(store);

            /*
             * Serialize as RDF/XML using a vendor specific extension to
             * represent the statement identifiers and statements about
             * statements.
             */
            final BigdataStatementIterator itr = store.getStatements(null,
                    null, null);
            final String rdfXml;
            try {

                Writer w = new StringWriter();

                RDFXMLWriter rdfWriter = new RDFXMLWriter(w);

                rdfWriter.startRDF();

                while (itr.hasNext()) {

                    Statement stmt = itr.next();

                    rdfWriter.handleStatement(stmt);

                }

                rdfWriter.endRDF();

                rdfXml = w.toString();

            } finally {

                itr.close();
                
            }

            // write the rdf/xml on the console.
            System.err.println(rdfXml);

            /*
             * Deserialize the RDF/XML into a temporary store and verify
             * read-back of the graph.
             */
            final TempTripleStore tempStore;
            {

                Properties properties = new Properties(store.getProperties());

                /*
                 * turn off closure so that the graph that we read back in will
                 * correspond exactly to the graph that we write out.
                 */
                properties.setProperty(DataLoader.Options.CLOSURE,
                        DataLoader.ClosureEnum.None.toString());

                tempStore = new TempTripleStore(properties);

            }

            try {

                log.info("Reading RDF/XML into temp store.");

                tempStore.getDataLoader().loadData(new StringReader(rdfXml),
                        ""/* baseURL */, RDFFormat.RDFXML);

                /*
                 * Verify the structure of the graph.
                 */
                verifyGraph(tempStore);

            } finally {

                tempStore.__tearDownUnitTest();

            }

        }
        
    }
   
    /**
     * Test verifies the correct unification of blank nodes appearing in an
     * RDF/XML document with the statement identifiers represented in the same
     * document using the custom {@value BD#SID} attribute.
     * 
     * @param store
     * 
     * @throws SailException
     * @throws RDFHandlerException
     * @throws IOException
     */
    protected void doStatementIdentifiersTest(final AbstractTripleStore store)
            throws RDFHandlerException, IOException {

        assert store.getStatementIdentifiers() == true;

        {

            final BigdataValueFactory valueFactory = store.getValueFactory();
            
            final BigdataURI x = valueFactory.createURI("http://www.foo.org/x");
            final BigdataURI y = valueFactory.createURI("http://www.foo.org/y");
            final BigdataURI z = valueFactory.createURI("http://www.foo.org/z");

            final BigdataURI A = valueFactory.createURI("http://www.foo.org/A");
            final BigdataURI B = valueFactory.createURI("http://www.foo.org/B");
            final BigdataURI C = valueFactory.createURI("http://www.foo.org/C");

            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());

            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/terms/creator");

            final BigdataLiteral bryan = valueFactory.createLiteral("bryan");
            final BigdataLiteral mike = valueFactory.createLiteral("mike");

            final BigdataBNode sid1 = valueFactory.createBNode("_sid1");
            final BigdataBNode sid2 = valueFactory.createBNode("_sid2");
            final BigdataBNode sid3 = valueFactory.createBNode("_sid3");

            {
             
                final StatementBuffer buf = new StatementBuffer(store, 10/* capacity */);

                // ground statements using BNodes for statement identifiers.
                buf.add(x, rdfType, A, sid1);
                buf.add(y, rdfType, B, sid2);
                buf.add(z, rdfType, C, sid3);

                // statements about statements using statement identifiers.
                buf.add(sid1, dcCreator, bryan);
                buf.add(sid2, dcCreator, bryan);
                buf.add(sid2, dcCreator, mike);
                buf.add(sid3, dcCreator, mike);

                /*
                 * Flush to the database, resolving statement identifiers as
                 * necessary.
                 */
                buf.flush();

            }

            /*
             * Verify the structure of the graph, at least those aspects
             * that are dealing with the statements about statements since
             * we do not care about TM here.
             */
            {

                final ISPO spo1 = store.getStatement(x.getTermId(), rdfType
                        .getTermId(), A.getTermId());
                final ISPO spo2 = store.getStatement(y.getTermId(), rdfType
                        .getTermId(), B.getTermId());
                final ISPO spo3 = store.getStatement(z.getTermId(), rdfType
                        .getTermId(), C.getTermId());

                assertNotNull(spo1);
                assertNotNull(spo2);
                assertNotNull(spo3);

                assertEquals(sid1.getTermId(), spo1.getStatementIdentifier());
                assertEquals(sid2.getTermId(), spo2.getStatementIdentifier());
                assertEquals(sid3.getTermId(), spo3.getStatementIdentifier());

                assertNotNull(store.getStatement(sid1.getTermId(), dcCreator
                        .getTermId(), bryan.getTermId()));
                assertNotNull(store.getStatement(sid2.getTermId(), dcCreator
                        .getTermId(), bryan.getTermId()));
                assertNotNull(store.getStatement(sid2.getTermId(), dcCreator
                        .getTermId(), mike.getTermId()));
                assertNotNull(store.getStatement(sid3.getTermId(), dcCreator
                        .getTermId(), mike.getTermId()));

            }
        }
        
        /*
         * Serialize as RDF/XML using a vendor specific extension to
         * represent the statement identifiers and statements about
         * statements.
         */
        final BigdataStatementIterator itr = store.getStatements(null, null, null);
        final String rdfXml;
        try {

            Writer w = new StringWriter();

            RDFXMLWriter rdfWriter = new RDFXMLWriter(w);

            rdfWriter.startRDF();

            while (itr.hasNext()) {

                Statement stmt = itr.next();

                rdfWriter.handleStatement(stmt);

            }

            rdfWriter.endRDF();

            rdfXml = w.toString();
            
        } finally {

            itr.close();

        }

        // write the rdf/xml on the console.
        System.err.println(rdfXml);

        /*
         * Deserialize the RDF/XML into a temporary store and verify read-back
         * of the graph with statement-level provenance metadata.
         */
        final TempTripleStore tempStore;
        {

            Properties properties = new Properties(store.getProperties());

            /*
             * turn off closure so that the graph that we read back in will
             * correspond exactly to the graph that we write out.
             */
            properties.setProperty(DataLoader.Options.CLOSURE,
                    DataLoader.ClosureEnum.None.toString());

            tempStore = new TempTripleStore(properties);

        }

        try {
            
            log.info("Reading RDF/XML into temp store.");
            
            tempStore.getDataLoader().loadData(new StringReader(rdfXml),
                ""/* baseURL */, RDFFormat.RDFXML);

            /*
             * Re-define the vocabulary so that it does not use the term
             * identifiers from the other database.
             * 
             * @todo since we use the BigdataValueFactory then we SHOULD NOT
             * need to do this. We SHOULD automatically get new value instances
             * when we write on a store with a lexicon in a different namespace.
             */
            final BigdataValueFactory valueFactory = store.getValueFactory();
            
            final BigdataURI x = valueFactory.createURI("http://www.foo.org/x");
            final BigdataURI y = valueFactory.createURI("http://www.foo.org/y");
            final BigdataURI z = valueFactory.createURI("http://www.foo.org/z");

            final BigdataURI A = valueFactory.createURI("http://www.foo.org/A");
            final BigdataURI B = valueFactory.createURI("http://www.foo.org/B");
            final BigdataURI C = valueFactory.createURI("http://www.foo.org/C");

            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());

            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/terms/creator");

            final BigdataLiteral bryan = valueFactory.createLiteral("bryan");
            final BigdataLiteral mike = valueFactory.createLiteral("mike");

            BigdataValue[] terms = new BigdataValue[] {
                    x,y,z,//
                    A,B,C,//
                    rdfType,//
                    dcCreator,//
                    bryan, mike//
            };
            
            // resolve term identifiers for the terms of interest.
            tempStore.getLexiconRelation()
                    .addTerms(terms, terms.length, true/*readOnly*/);
            
            /*
             * Verify the structure of the graph, at least those aspects that
             * are dealing with the statements about statements since we do not
             * care about TM here.
             */
            
            final ISPO spo1 = tempStore.getStatement(x.getTermId(), rdfType
                    .getTermId(), A.getTermId());
            final ISPO spo2 = tempStore.getStatement(y.getTermId(), rdfType
                    .getTermId(), B.getTermId());
            final ISPO spo3 = tempStore.getStatement(z.getTermId(), rdfType
                    .getTermId(), C.getTermId());

            assertNotNull(spo1);
            assertNotNull(spo2);
            assertNotNull(spo3);

            final long sid1 = spo1.getStatementIdentifier();
            final long sid2 = spo2.getStatementIdentifier();
            final long sid3 = spo3.getStatementIdentifier();
            
            assertEquals(sid1, spo1.getStatementIdentifier());
            assertEquals(sid2, spo2.getStatementIdentifier());
            assertEquals(sid3, spo3.getStatementIdentifier());

            assertNotNull(tempStore.getStatement(sid1, dcCreator.getTermId(),
                    bryan.getTermId()));
            assertNotNull(tempStore.getStatement(sid2, dcCreator.getTermId(),
                    bryan.getTermId()));
            assertNotNull(tempStore.getStatement(sid2, dcCreator.getTermId(),
                    mike.getTermId()));
            assertNotNull(tempStore.getStatement(sid3, dcCreator.getTermId(),
                    mike.getTermId()));
            
        } finally {
            
            tempStore.__tearDownUnitTest();
            
        }

    }
    
}
