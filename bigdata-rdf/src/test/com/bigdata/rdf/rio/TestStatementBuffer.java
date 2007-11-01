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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import java.util.Iterator;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.StatementBuffer.StatementIterator;
import com.bigdata.rdf.rio.StatementBuffer.TermArrayIterator;
import com.bigdata.rdf.rio.StatementBuffer.TermIterator;
import com.bigdata.rdf.rio.StatementBuffer.UnknownStatementIterator;
import com.bigdata.rdf.rio.StatementBuffer.UnknownTermIterator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test suite for {@link StatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStatementBuffer extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestStatementBuffer() {
    }

    /**
     * @param name
     */
    public TestStatementBuffer(String name) {
        super(name);
    }

    public void test_ctor() {
        
        final int capacity = 27;
        
        AbstractTripleStore store = getStore();
        
        try {
            
            StatementBuffer buffer = new StatementBuffer(store,capacity,false);
            
            assertEquals(store,buffer.getDatabase());
            assertFalse(buffer.distinct);
            assertEquals(capacity,buffer.capacity);
            assertEquals(capacity*IRawTripleStore.N,buffer.values.length);
//            assertEquals(capacity,buffer.literals.length);
//            assertEquals(capacity,buffer.bnodes.length);
            assertEquals(capacity,buffer.stmts.length);
            assertEquals(0,buffer.numURIs);
            assertEquals(0,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(0,buffer.numStmts);

        } finally {

            store.closeAndDelete();
            
        }
        
    }

    public void test_handleStatement() {

        final int capacity = 5;
        
        AbstractTripleStore store = getStore();
        
        try {
        
            StatementBuffer buffer = new StatementBuffer(store,capacity,false);
            
            /*
             * add a statement.
             */
            
            _URI s1 = new _URI("http://www.foo.org");
            _URI p1 = new _URI( RDF.TYPE);
            _URI o1 =  new _URI(RDFS.RESOURCE);
    
            buffer.handleStatement(s1, p1, o1, StatementEnum.Explicit );
            
            assertEquals(3,buffer.numURIs);
            assertEquals(0,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(1,buffer.numStmts);
    
            /*
             * verify term class (URI, Literal or BNode) iterators.
             */
            assertSameIterator(new Object[] { /*uris*/s1, p1, o1 },
                    new TermArrayIterator(buffer.values, buffer.numValues));
            
//            assertSameIterator(new Object[] { s1, p1, o1 },
//                    new TermClassIterator(buffer.uris, buffer.numURIs));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.literals, buffer.numLiterals));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.bnodes, buffer.numBNodes));
    
            /*
             * verify all terms iterator.
             */
            assertSameIterator(new Object[] { s1, p1, o1 },
                    new TermIterator(buffer));
    
            /*
             * verify statement iterator.
             */
            assertSameIterator(new Object[] { buffer.stmts[0] },
                    new StatementIterator(KeyOrder.SPO, buffer));
            
            /*
             * add another statement.
             */
            
            _URI s2 = new _URI("http://www.foo.org");
            _URI p2 = new _URI( RDFS.LABEL);
            _Literal o2 =  new _Literal("test uri.");
            
            buffer.handleStatement(s2, p2, o2, StatementEnum.Explicit );
            
            assertEquals(5,buffer.numURIs);
            assertEquals(1,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(2,buffer.numStmts);
    
            /*
             * verify term class (URI, Literal or BNode) iterators.
             */
            assertSameIterator(new Object[] { /*uris*/s1, p1, o1, s2, p2, /*literals*/o2 },
                    new TermArrayIterator(buffer.values, buffer.numValues));

//            assertSameIterator(new Object[] { s1, p1, o1, s2, p2 },
//                    new TermClassIterator(buffer.uris, buffer.numURIs));
//    
//            assertSameIterator(new Object[] { o2 },
//                    new TermClassIterator(buffer.literals, buffer.numLiterals));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.bnodes, buffer.numBNodes));
    
            /*
             * verify all terms iterator.
             */
            assertSameIterator(new Object[] { s1, p1, o1, s2, p2, o2 },
                    new TermIterator(buffer));
    
            /*
             * verify statement iterator.
             */
            assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1] },
                    new StatementIterator(KeyOrder.SPO, buffer));
            
            /*
             * verify sort keys NOT assigned for terms.
             */
            {
                Iterator<_Value> itr = new TermIterator(buffer);
                while(itr.hasNext()) {
                    assertNull(itr.next().key);
                }
            }
    
            /*
             * assign sort keys for terms.
             */
            buffer.generateTermSortKeys(store.getKeyBuilder());
    
            /*
             * verify sort keys assigned for terms.
             */
            {
                Iterator<_Value> itr = new TermIterator(buffer);
                while(itr.hasNext()) {
                    assertNotNull(itr.next().key);
                }
            }
    
            /*
             * sort terms by the assigned sort keys.
             */
            buffer.sortTermsBySortKeys();
    
            /*
             * show terms in sorted order.
             */
            {
                Iterator<_Value> itr = new TermIterator(buffer);
                while(itr.hasNext()) {
                    System.err.println(itr.next().toString());
                }
            }
            
            /* verify term iterator
             * 
             * Note: order URIs < Literals < BNodes.
             * 
             * Note: Arrays.sort() is stable.
             * 
             * Note: The sort order depends on the sort keys.  If you use an ASCII
             * sort keys then the sort order is different from treating the strings
             * as US-English Unicode data.  So this test can flunk depending on how
             * you are encoding unicode strings to keys. 
             */
            assertSameIterator(new Object[] { s1, s2, p1, p2, o1, o2 },
                    new TermIterator(buffer));
        
        } finally {
        
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test with the <code>distinct</code> terms feature is enabled to uniquify
     * the terms in the buffer.
     */
    public void test_handleStatement_distinct() {
        
        final int capacity = 5;

        AbstractTripleStore store = getStore();
        
        try {
        
            StatementBuffer buffer = new StatementBuffer(store,capacity,true);
            
            assertTrue(buffer.distinct);
            
            /*
             * add a statement.
             */
            
            _URI s1 = new _URI("http://www.foo.org");
            _URI p1 = new _URI( RDF.TYPE);
            _URI o1 =  new _URI(RDFS.RESOURCE);
    
            buffer.handleStatement(s1, p1, o1, StatementEnum.Explicit );
            
            assertEquals(3,buffer.numURIs);
            assertEquals(0,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(1,buffer.numStmts);
            
            assertEquals(1,s1.count);
            assertEquals(1,p1.count);
            assertEquals(1,o1.count);
    
            /*
             * verify term class (URI, Literal or BNode) iterators.
             */
            assertSameIterator(new Object[] { /*uris*/s1, p1, o1 },
                    new TermArrayIterator(buffer.values, buffer.numValues));
            
//            assertSameIterator(new Object[] { s1, p1, o1 },
//                    new TermClassIterator(buffer.uris, buffer.numURIs));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.literals, buffer.numLiterals));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.bnodes, buffer.numBNodes));
    
            /*
             * verify all terms iterator.
             */
            assertSameIterator(new Object[] { s1, p1, o1 },
                    new TermIterator(buffer));
    
            /*
             * verify statement iterator.
             */
            assertSameIterator(new Object[] { buffer.stmts[0] },
                    new StatementIterator(KeyOrder.SPO, buffer));
            
            /*
             * add another statement.
             */
            
            _URI s2 = new _URI("http://www.foo.org"); // duplicate term!
            _URI p2 = new _URI( RDFS.LABEL);
            _Literal o2 =  new _Literal("test uri.");
            
            buffer.handleStatement(s2, p2, o2, StatementEnum.Explicit );
    
            assertEquals(4,buffer.numURIs); // only 4 since one is a duplicate.
            assertEquals(1,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(2,buffer.numStmts);
    
            assertEquals(2,s1.count);
            assertEquals(1,p1.count);
            assertEquals(1,o1.count);
            assertEquals(0,s2.count);
            assertEquals(1,p2.count);
            assertEquals(1,o2.count);
    
            /*
             * verify term class (URI, Literal or BNode) iterators.
             */
            // Note: s2 was a duplicate.
            assertSameIterator(new Object[] { /*uris*/s1, p1, o1, /*s2,*/ p2, /*literals*/o2 },
                    new TermArrayIterator(buffer.values, buffer.numValues));

//            assertSameIterator(new Object[] { s1, p1, o1, /*s2,*/ p2 }, // s2 was a duplicate.
//                    new TermClassIterator(buffer.uris, buffer.numURIs));
//    
//            assertSameIterator(new Object[] { o2 },
//                    new TermClassIterator(buffer.literals, buffer.numLiterals));
//    
//            assertSameIterator(new Object[] {},
//                    new TermClassIterator(buffer.bnodes, buffer.numBNodes));
    
            /*
             * verify all terms iterator.
             */
            assertSameIterator(new Object[] { s1, p1, o1, /*s2,*/ p2, o2 },
                    new TermIterator(buffer));
            
            /*
             * verify statement iterator.
             */
            assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1] },
                    new StatementIterator(KeyOrder.SPO, buffer));
    
            /*
             * add a duplicate statement.
             */
            
            _URI s3 = new _URI("http://www.foo.org"); // duplicate term!
            _URI p3 = new _URI( RDFS.LABEL);
            _Literal o3 =  new _Literal("test uri.");
            
            buffer.handleStatement(s3, p3, o3, StatementEnum.Explicit );
    
            assertEquals(4,buffer.numURIs);
            assertEquals(1,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(3,buffer.numStmts);
    
            assertEquals(3,s1.count);
            assertEquals(1,p1.count);
            assertEquals(1,o1.count);
            assertEquals(0,s2.count);
            assertEquals(2,p2.count);
            assertEquals(2,o2.count);
            assertEquals(0,s3.count);
            assertEquals(0,p3.count);
            assertEquals(0,o3.count);
    
            /*
             * add a duplicate statement using the _same_ term objects.
             */
            
            buffer.handleStatement(s3, p3, o3, StatementEnum.Explicit );
    
            assertEquals(4,buffer.numURIs);
            assertEquals(1,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(4,buffer.numStmts);
    
            assertEquals(4,s1.count);
            assertEquals(1,p1.count);
            assertEquals(1,o1.count);
            assertEquals(0,s2.count);
            assertEquals(3,p2.count);
            assertEquals(3,o2.count);
            assertEquals(0,s3.count);
            assertEquals(0,p3.count);
            assertEquals(0,o3.count);
    
            /*
             * generate the term sort keys.
             */
            buffer.generateTermSortKeys(store.getKeyBuilder());
            
            /*
             * sort terms by the assigned sort keys.
             */
            buffer.sortTermsBySortKeys();
    
            /* verify iterator before filtering.
             * 
             * Note: order URIs<Literals<BNodes.
             * 
             * Note: Arrays.sort() is stable.
             * 
             * Note: The sort order depends on the sort keys.  If you use an ASCII
             * sort keys then the sort order is different from treating the strings
             * as US-English Unicode data.  So this test can flunk depending on how
             * you are encoding unicode strings to keys. 
             */
            assertSameIterator(new Object[] { s1, /*s2,*/ p1, p2, o1, o2 },
                    new TermIterator(buffer));
            assertSameIterator(new Object[] { s1, /*s2,*/ p1, p2, o1, o2 },
                    new UnknownTermIterator(buffer));
    
    //        s1.duplicate = true;
    //        p1.known = true;
    //        
    //        assertSameIterator(new Object[] { s1, s2, p1, o1, p2, o2 },
    //                new TermIterator(buffer));
    //        assertSameIterator(new Object[] { s2, o1, p2, o2 },
    //                new UnknownTermIterator(buffer));
    //        
    //        s1.duplicate = false;
    //        p1.known = false;
    
    //        /*
    //         * filter out duplicate terms automatically.
    //         */
    //        
    //        buffer.filterDuplicateTerms();
    //        
    //        assertEquals(1,buffer.numDupURIs);
    //        assertEquals(0,buffer.numDupLiterals);
    //        assertEquals(0,buffer.numDupBNodes);
    //        assertFalse(s1.duplicate);
    //        assertFalse(p1.duplicate);
    //        assertFalse(o1.duplicate);
    //        assertTrue(s2.duplicate);
    //        assertFalse(p2.duplicate);
    //        assertFalse(o2.duplicate);
    //        
    //        assertSameIterator(new Object[] { s1, s2, p1, o1, p2, o2 },
    //                new TermIterator(buffer));
    //        assertSameIterator(new Object[] { s1, p1, o1, p2, o2 },
    //                new UnknownTermIterator(buffer));
            
            /*
             * 
             */
            assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1],
                    buffer.stmts[2], buffer.stmts[3] },
                    new UnknownStatementIterator(KeyOrder.SPO, buffer));

        } finally {

            store.closeAndDelete();
            
        }
        
    }
        
}
