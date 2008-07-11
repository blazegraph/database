/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Test suite for {@link StatementBuffer}.
 * 
 * FIXME This test suite should really be rewritten. The implementation has
 * evolved and the test suite has not kept up with that evolution. Right now,
 * most real testing of {@link StatementBuffer} is currently in terms of its
 * application - loading RDF and verifying that the loaded RDF agrees with the
 * source RDF.
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
            
            StatementBuffer buffer = new StatementBuffer(store,capacity);
            
            assertEquals(store,buffer.getDatabase());
            assertTrue(buffer.distinct);
            assertEquals(capacity,buffer.capacity);
            assertEquals(capacity*IRawTripleStore.N,buffer.values.length);
            assertEquals(capacity,buffer.stmts.length);
            assertEquals(0,buffer.numURIs);
            assertEquals(0,buffer.numLiterals);
            assertEquals(0,buffer.numBNodes);
            assertEquals(0,buffer.numStmts);

        } finally {

            store.closeAndDelete();
            
        }
        
    }

    public void test_handleStatement_distinct() {
        
        final int capacity = 5;

        AbstractTripleStore store = getStore();
        
        try {
        
            StatementBuffer buffer = new StatementBuffer(store,capacity);
            
            assertTrue(buffer.distinct);
            
            /*
             * add a statement.
             */
            
            _URI s1 = new _URI("http://www.foo.org");
            _URI p1 = new _URI( RDF.TYPE);
            _URI o1 = new _URI(RDFS.RESOURCE);
            _URI c1 = null; // no context.
    
            buffer.handleStatement(s1, p1, o1, c1, StatementEnum.Explicit );
            
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
//            assertSameIterator(new Object[] { /*uris*/s1, p1, o1 },
//                    new TermArrayIterator(buffer.values, buffer.numValues));
            
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
//            assertSameIterator(new Object[] { s1, p1, o1 },
//                    new TermIterator(buffer));
    
            /*
             * verify statement iterator.
             */
//            assertSameIterator(new Object[] { buffer.stmts[0] },
//                    new StatementIterator(KeyOrder.SPO, buffer));
            
            /*
             * add another statement.
             */
            
            _URI s2 = new _URI("http://www.foo.org"); // duplicate term!
            _URI p2 = new _URI( RDFS.LABEL);
            _Literal o2 =  new _Literal("test uri.");
//            _URI c2 = new _URI("http://www.foo.org/myGraph");
            _URI c2 = null;
            
            buffer.handleStatement(s2, p2, o2, c2, StatementEnum.Explicit );
    
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
//            assertEquals(1,c2.count);
    
            /*
             * verify term class (URI, Literal or BNode) iterators.
             */
            // Note: s2 was a duplicate.
//            assertSameIterator(new Object[] { /*uris*/s1, p1, o1, /*s2,*/ p2, /*literals*/o2 },
//                    new TermArrayIterator(buffer.values, buffer.numValues));

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
//            assertSameIterator(new Object[] { s1, p1, o1, /*s2,*/ p2, o2 },
//                    new TermIterator(buffer));
            
            /*
             * verify statement iterator.
             */
//            assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1] },
//                    new StatementIterator(KeyOrder.SPO, buffer));
    
            /*
             * add a duplicate statement.
             */
            
            _URI s3 = new _URI("http://www.foo.org"); // duplicate term!
            _URI p3 = new _URI( RDFS.LABEL);
            _Literal o3 =  new _Literal("test uri.");
//            _URI c3 = new _URI("http://www.foo.org/myGraph");
            _URI c3 = null;
            
            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit );
    
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
//            assertEquals(2,c3.count);
    
            /*
             * add a duplicate statement using the _same_ term objects.
             */
            
            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit );
    
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
    
//            /*
//             * generate the term sort keys.
//             */
//            buffer.generateTermSortKeys(store.getKeyBuilder());
//            
//            /*
//             * sort terms by the assigned sort keys.
//             */
//            buffer.sortTermsBySortKeys();
    
//            /* verify iterator before filtering.
//             * 
//             * Note: order URIs<Literals<BNodes.
//             * 
//             * Note: Arrays.sort() is stable.
//             * 
//             * Note: The sort order depends on the sort keys.  If you use an ASCII
//             * sort keys then the sort order is different from treating the strings
//             * as US-English Unicode data.  So this test can flunk depending on how
//             * you are encoding unicode strings to keys. 
//             */
//            assertSameIterator(new Object[] { s1, /*s2,*/ p1, p2, o1, o2 },
//                    new TermIterator(buffer));
//            assertSameIterator(new Object[] { s1, /*s2,*/ p1, p2, o1, o2 },
//                    new UnknownTermIterator(buffer));
    
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
            
//            /*
//             * 
//             */
//            assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1],
//                    buffer.stmts[2], buffer.stmts[3] },
//                    new UnknownStatementIterator(KeyOrder.SPO, buffer));

        } finally {

            store.closeAndDelete();
            
        }
        
    }
            
}
