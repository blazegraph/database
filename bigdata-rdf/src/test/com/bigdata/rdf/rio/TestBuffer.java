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

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.AbstractTripleStoreTestCase;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.rio.Buffer.StatementIterator;
import com.bigdata.rdf.rio.Buffer.TermClassIterator;
import com.bigdata.rdf.rio.Buffer.UnknownAndDistinctTermIterator;
import com.bigdata.rdf.rio.Buffer.UnknownAndDistinctStatementIterator;

/**
 * Test suite for {@link Buffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBuffer extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestBuffer() {
    }

    /**
     * @param name
     */
    public TestBuffer(String name) {
        super(name);
    }

    public void test_ctor() {
        
        final int capacity = 27;
        
        Buffer buffer = new Buffer(store,capacity,false);
        
        assertEquals(store,buffer.store);
        assertEquals(capacity,buffer.capacity);
        assertEquals(capacity,buffer.uris.length);
        assertEquals(capacity,buffer.literals.length);
        assertEquals(capacity,buffer.bnodes.length);
        assertEquals(capacity,buffer.stmts.length);
        assertEquals(0,buffer.numURIs);
        assertEquals(0,buffer.numLiterals);
        assertEquals(0,buffer.numBNodes);
        assertEquals(0,buffer.numStmts);

    }

    public void test_handleStatement() {
        
        final int capacity = 5;
        
        BulkLoaderBuffer buffer = new BulkLoaderBuffer(store,capacity,false);
        
        /*
         * add a statement.
         */
        
        _URI s1 = new _URI("http://www.foo.org");
        _URI p1 = new _URI( RDF.TYPE);
        _URI o1 =  new _URI(RDFS.RESOURCE);

        buffer.handleStatement(s1, p1, o1 );
        
        assertEquals(3,buffer.numURIs);
        assertEquals(0,buffer.numLiterals);
        assertEquals(0,buffer.numBNodes);
        assertEquals(1,buffer.numStmts);

        assertSameIterator(new Object[] { s1, p1, o1 },
                new TermClassIterator(buffer.uris, buffer.numURIs));

        assertSameIterator(new Object[] {},
                new TermClassIterator(buffer.literals, buffer.numLiterals));

        assertSameIterator(new Object[] {},
                new TermClassIterator(buffer.bnodes, buffer.numBNodes));

        assertSameIterator(new Object[] { buffer.stmts[0] },
                new StatementIterator(KeyOrder.SPO, buffer));
        
        /*
         * add another statement.
         */
        
        _URI s2 = new _URI("http://www.foo.org");
        _URI p2 = new _URI( RDFS.LABEL);
        _Literal o2 =  new _Literal("test uri.");
        
        buffer.handleStatement(s2, p2, o2 );
        
        assertEquals(5,buffer.numURIs);
        assertEquals(1,buffer.numLiterals);
        assertEquals(0,buffer.numBNodes);
        assertEquals(2,buffer.numStmts);

        assertSameIterator(new Object[] { s1, p1, o1, s2, p2 },
                new TermClassIterator(buffer.uris, buffer.numURIs));

        assertSameIterator(new Object[] { o2 },
                new TermClassIterator(buffer.literals, buffer.numLiterals));

        assertSameIterator(new Object[] {},
                new TermClassIterator(buffer.bnodes, buffer.numBNodes));

        assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1] },
                new StatementIterator(KeyOrder.SPO, buffer));
        
        /*
         * assign sort keys for terms.
         */
        buffer.generateTermSortKeys(store.keyBuilder);

        /*
         * sort terms by the assigned sort keys.
         */
        buffer.sortTermsBySortKeys();

        /* verify iterator before filtering.
         * 
         * Note: order URIs<Literals<BNodes.
         * 
         * Note: Arrays.sort() is stable.
         */
        assertSameIterator(new Object[] { s1, s2, p1, o1, p2, o2 },
                new UnknownAndDistinctTermIterator(buffer));

        s1.duplicate = true;
        p1.known = true;
        
        assertSameIterator(new Object[] { s2, o1, p2, o2 },
                new UnknownAndDistinctTermIterator(buffer));
        
        s1.duplicate = false;
        p1.known = false;

        /*
         * filter out duplicate terms automatically.
         */
        
        buffer.filterDuplicateTerms();
        
        assertEquals(1,buffer.numDupURIs);
        assertEquals(0,buffer.numDupLiterals);
        assertEquals(0,buffer.numDupBNodes);
        assertFalse(s1.duplicate);
        assertFalse(p1.duplicate);
        assertFalse(o1.duplicate);
        assertTrue(s2.duplicate);
        assertFalse(p2.duplicate);
        assertFalse(o2.duplicate);
        
        assertSameIterator(new Object[] { s1, p1, o1, p2, o2 },
                new UnknownAndDistinctTermIterator(buffer));
        
        /*
         * 
         */
        assertSameIterator(new Object[] { buffer.stmts[0], buffer.stmts[1] },
                new UnknownAndDistinctStatementIterator(KeyOrder.SPO,buffer));
        
    }
        
}
