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

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;

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

        store.addStatement(x, rdfType, C);
        store.addStatement(y, rdfType, B);
        store.addStatement(z, rdfType, A);

        store.addStatement(B, rdfsSubClassOf, A);
        store.addStatement(C, rdfsSubClassOf, B);

        final long x_id = store.getTerm(x);
        final long y_id = store.getTerm(y);
        final long z_id = store.getTerm(z);
        final long A_id = store.getTerm(A);
        final long B_id = store.getTerm(B);
        final long C_id = store.getTerm(C);
        final long rdfType_id = store.getTerm(rdfType);
        final long rdfsSubClassOf_id = store.getTerm(rdfsSubClassOf);
        
        /*
         * Fields used to verify restart safety of additional metadata for 
         * the term:id index.
         */
        final long counter = store.getCounter().getCounter();

        store.commit();
        
        /*
         * verify that extension metadata for the term:id index was not modified.
         */
        assertEquals("counter", counter, store.getCounter().getCounter());

        assertEquals(x_id,store.getTerm(x));
        assertEquals(y_id,store.getTerm(y));
        assertEquals(z_id,store.getTerm(z));
        assertEquals(A_id,store.getTerm(A));
        assertEquals(B_id,store.getTerm(B));
        assertEquals(C_id,store.getTerm(C));
        assertEquals(rdfType_id,store.getTerm(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTerm(rdfsSubClassOf));
        
//        assertEquals("statementCount", 5, store.getSPOIndex().getEntryCount());
//        assertEquals("statementCount", 5, store.getPOSIndex().getEntryCount());
//        assertEquals("statementCount", 5, store.getOSPIndex().getEntryCount());
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));

        store.close();
        
        store = new TripleStore(properties);
        
        assertEquals(x_id,store.getTerm(x));
        assertEquals(y_id,store.getTerm(y));
        assertEquals(z_id,store.getTerm(z));
        assertEquals(A_id,store.getTerm(A));
        assertEquals(B_id,store.getTerm(B));
        assertEquals(C_id,store.getTerm(C));
        assertEquals(rdfType_id,store.getTerm(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTerm(rdfsSubClassOf));

//        assertEquals("statementCount", 5, store.getSPOIndex().getEntryCount());
//        assertEquals("statementCount", 5, store.getPOSIndex().getEntryCount());
//        assertEquals("statementCount", 5, store.getOSPIndex().getEntryCount());
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));

        /*
         * verify that extension metadata for the term:id index was correctly
         * restored.
         */
        assertEquals("counter", counter, store.getCounter().getCounter());
        
    }

}
