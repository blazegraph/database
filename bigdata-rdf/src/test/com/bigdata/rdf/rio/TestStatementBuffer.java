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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.StatementEnum;
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

            StatementBuffer buffer = new StatementBuffer(store, capacity);

            assertEquals(store, buffer.getDatabase());
            assertTrue(buffer.distinct);
            assertEquals(capacity, buffer.capacity);
            assertEquals(capacity * IRawTripleStore.N, buffer.values.length);
            assertEquals(capacity, buffer.stmts.length);
            assertEquals(0, buffer.numURIs);
            assertEquals(0, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(0, buffer.numStmts);

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Test verifies detection of duplicate terms and their automatic
     * replacement with a canonicalizing term.
     */
    public void test_handleStatement_distinctTerm() {

        final int capacity = 5;

        AbstractTripleStore store = getStore();

        try {

            StatementBuffer buffer = new StatementBuffer(store, capacity);

            assertTrue(buffer.distinct);

            /*
             * add a statement.
             */

            final URI s1 = new URIImpl("http://www.foo.org");
            final URI p1 = RDF.TYPE;
            final URI o1 = RDFS.RESOURCE;
            final URI c1 = null; // no context.

            buffer.handleStatement(s1, p1, o1, c1, StatementEnum.Explicit);

            assertEquals(3, buffer.numURIs);
            assertEquals(0, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(1, buffer.numStmts);

            /*
             * add another statement.
             */

            final URI s2 = new URIImpl("http://www.foo.org"); // duplicate term!
            final URI p2 = RDFS.LABEL;
            final Literal o2 = new LiteralImpl("test lit.");
            final URI c2 = null;

            buffer.handleStatement(s2, p2, o2, c2, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs); // only 4 since one is duplicate.
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(2, buffer.numStmts);

            /*
             * add a duplicate statement.
             */

            final URI s3 = new URIImpl("http://www.foo.org"); // duplicate term
            final URI p3 = RDFS.LABEL;                        // duplicate term
            final Literal o3 = new LiteralImpl("test lit.");  // duplicate term
            final URI c3 = null;

            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs);
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(3, buffer.numStmts);

            /*
             * add a duplicate statement using the _same_ term objects.
             */

            buffer.handleStatement(s3, p3, o3, c3, StatementEnum.Explicit);

            assertEquals(4, buffer.numURIs);
            assertEquals(1, buffer.numLiterals);
            assertEquals(0, buffer.numBNodes);
            assertEquals(4, buffer.numStmts);
            
            buffer.flush();

        } finally {

            store.closeAndDelete();

        }

    }

}
