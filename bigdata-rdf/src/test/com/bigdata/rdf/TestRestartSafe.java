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
 * Created on Feb 5, 2007
 */

package com.bigdata.rdf;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.scaleup.MasterJournal.Options;

/**
 * Test restart safety for the various indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends AbstractTripleStoreTestCase {

    public TestRestartSafe() {
    }

    public TestRestartSafe(String name) {
        super(name);
    }

    public Properties getProperties() {
        
        Properties properties = super.getProperties();
        
        // force migration of all indices on overflow.
        properties.setProperty(Options.MIGRATION_THRESHOLD, "0");
        
        return properties;
        
    }
    
    /**
     * Note: We must use a stable store for these tests.
     */
    protected BufferMode getBufferMode() {
        
        return BufferMode.Direct;
        
    }

    public void test_restartSafe() throws IOException {

        /*
         * setup the database.
         */
        _URI x = new _URI("http://www.foo.org/x");
        _URI y = new _URI("http://www.foo.org/y");
        _URI z = new _URI("http://www.foo.org/z");

        _URI A = new _URI("http://www.foo.org/A");
        _URI B = new _URI("http://www.foo.org/B");
        _URI C = new _URI("http://www.foo.org/C");

        _URI rdfType = new _URI(RDF.TYPE);

        _URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);

        _Literal lit1 = new _Literal("abc");
        _Literal lit2 = new _Literal("abc",A);
        _Literal lit3 = new _Literal("abc","en");

        _BNode bn1 = new _BNode();
        _BNode bn2 = new _BNode("a12");
        
        store.addStatement(x, rdfType, C);
        store.addStatement(y, rdfType, B);
        store.addStatement(z, rdfType, A);

        store.addStatement(B, rdfsSubClassOf, A);
        store.addStatement(C, rdfsSubClassOf, B);

        final long x_id = store.getTermId(x); assertTrue(x_id!=0L);
        final long y_id = store.getTermId(y); assertTrue(y_id!=0L);
        final long z_id = store.getTermId(z); assertTrue(z_id!=0L);
        final long A_id = store.getTermId(A); assertTrue(A_id!=0L);
        final long B_id = store.getTermId(B); assertTrue(B_id!=0L);
        final long C_id = store.getTermId(C); assertTrue(C_id!=0L);
        final long rdfType_id = store.getTermId(rdfType); assertTrue(rdfType_id!=0L);
        final long rdfsSubClassOf_id = store.getTermId(rdfsSubClassOf); assertTrue(rdfsSubClassOf_id!=0L);
        
        final long lit1_id = store.addTerm(lit1); assertTrue(lit1_id!=0L);
        final long lit2_id = store.addTerm(lit2); assertTrue(lit2_id!=0L);
        final long lit3_id = store.addTerm(lit3); assertTrue(lit3_id!=0L);
        
        final long bn1_id = store.addTerm(bn1); assertTrue(bn1_id!=0L);
        final long bn2_id = store.addTerm(bn2); assertTrue(bn2_id!=0L);
        
        /*
         * Counter for the term:id index.
         */
        final long counter = store.getCounter().getCounter();

        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

        store.commit();
        
        /*
         * verify that the counter for the term:id index was not modified.
         */
        assertEquals("counter", counter, store.getCounter().getCounter());

        assertEquals(x_id,store.getTermId(x));
        assertEquals(y_id,store.getTermId(y));
        assertEquals(z_id,store.getTermId(z));
        assertEquals(A_id,store.getTermId(A));
        assertEquals(B_id,store.getTermId(B));
        assertEquals(C_id,store.getTermId(C));
        assertEquals(rdfType_id,store.getTermId(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTermId(rdfsSubClassOf));
        
        assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,null));
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));
        
        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

        /* force overflow -- if using partitioned indices then this will build
         * index segments IFF you have also set the threshold to zero in the
         * properties.
         */
        store.overflow();
        
        /*
         * verify that the counter for the term:id index was not modified.
         */
        assertEquals("counter", counter, store.getCounter().getCounter());

        assertEquals(x_id,store.getTermId(x));
        assertEquals(y_id,store.getTermId(y));
        assertEquals(z_id,store.getTermId(z));
        assertEquals(A_id,store.getTermId(A));
        assertEquals(B_id,store.getTermId(B));
        assertEquals(C_id,store.getTermId(C));
        assertEquals(rdfType_id,store.getTermId(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTermId(rdfsSubClassOf));
        
        assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,null));
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));
        
        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

        store.close();
        
        store = new TripleStore(properties);
        
        assertEquals(x_id,store.getTermId(x));
        assertEquals(y_id,store.getTermId(y));
        assertEquals(z_id,store.getTermId(z));
        assertEquals(A_id,store.getTermId(A));
        assertEquals(B_id,store.getTermId(B));
        assertEquals(C_id,store.getTermId(C));
        assertEquals(rdfType_id,store.getTermId(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTermId(rdfsSubClassOf));

        assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,null));
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));

        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

        /*
         * verify that counter for the term:id index was correctly restored.
         */
        assertEquals("counter", counter, store.getCounter().getCounter());

        /*
         * verify the terms can be recovered.
         */
        assertEquals(x,store.getTerm(x_id));
        assertEquals(y,store.getTerm(y_id));
        assertEquals(z,store.getTerm(z_id));
        assertEquals(A,store.getTerm(A_id));
        assertEquals(B,store.getTerm(B_id));
        assertEquals(C,store.getTerm(C_id));
        
        assertEquals(rdfType,store.getTerm(rdfType_id));
        assertEquals(rdfsSubClassOf,store.getTerm(rdfsSubClassOf_id));
        
        assertEquals(lit1,store.getTerm(lit1_id));
        assertEquals(lit2,store.getTerm(lit2_id));
        assertEquals(lit3,store.getTerm(lit3_id));

        assertEquals(bn1,store.getTerm(bn1_id));
        assertEquals(bn2,store.getTerm(bn2_id));

        store.closeAndDelete();
        
    }

}
