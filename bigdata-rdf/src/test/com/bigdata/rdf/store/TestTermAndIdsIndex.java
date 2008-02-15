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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.UUID;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Test suite for use of the terms and ids indices.
 * 
 * @deprecated This duplicates tests found in {@link TestTripleStore} since the test
 *       harness is not yet invariant so that it can run against each of the
 *       {@link ITripleStore} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermAndIdsIndex extends AbstractEmbeddedTripleStoreTestCase {

    /**
     * 
     */
    public TestTermAndIdsIndex() {
    }

    /**
     * @param arg0
     */
    public TestTermAndIdsIndex(String arg0) {
        super(arg0);
    }

    /**
     * Test helper verifies that the term is not in the lexicon, adds the term
     * to the lexicon, verifies that the term can be looked up by its assigned
     * term identifier, verifies that the term is now in the lexicon, and
     * verifies that adding the term again returns the same term identifier.
     * 
     * @param term The term.
     */
    protected void doAddTermTest(_Value term) {
        
        assertEquals(NULL,store.getTermId(term));
        
        final long id = store.addTerm(term);
        
        assertNotSame(NULL,id);
        
        assertEquals(id,store.getTermId(term));

        assertEquals(term,store.getTerm(id));

        assertEquals(id,store.addTerm(term));

    }
    
    /**
     * Simple test of inserting one term at a time into the lexicon.
     */
    public void test_addTerm() {

        doAddTermTest(new _URI("http://www.bigdata.com"));
        doAddTermTest(new _URI(RDF.TYPE));
        doAddTermTest(new _URI(RDFS.SUBCLASSOF));
        doAddTermTest(new _URI(XMLSchema.DECIMAL));

        doAddTermTest(new _Literal("abc"));
        doAddTermTest(new _Literal("abc", new _URI(XMLSchema.DECIMAL)));
        doAddTermTest(new _Literal("abc", "en"));

        doAddTermTest(new _BNode(UUID.randomUUID().toString()));
        doAddTermTest(new _BNode("a12"));

    }
    
    /**
     * Simple test of batch insertion of terms into the lexicon.
     */
    public void test_insertTerms() {

        _Value[] terms = new _Value[] {//
                
                new _URI("http://www.bigdata.com"),//
                
                new _URI(RDF.TYPE),//
                new _URI(RDFS.SUBCLASSOF),//
                new _URI(XMLSchema.DECIMAL),//

                new _Literal("abc"),//
                new _Literal("abc", new _URI(XMLSchema.DECIMAL)),//
                new _Literal("abc", "en"),//

                new _BNode(UUID.randomUUID().toString()),//
                new _BNode("a12") //
        };

        store.addTerms(store.getKeyBuilder(), terms, terms.length);
        
        for( int i=0; i<terms.length; i++) {

            // check the forward mapping (term -> id)
            assertEquals("forward mapping", terms[i], store.getTerm(terms[i].termId));

            // check the reverse mapping (id -> term)
            assertEquals("reverse mapping", terms[i].termId,
                    store.getTermId(terms[i]));

            /*
             * check flag set iff term was successfully asserted against both
             * the forward and reverse mappings.
             */
            assertTrue("known", terms[i].known);
            
        }

        /**
         * Dumps the forward mapping.
         */
        {
            
            System.err.println("terms index (forward mapping).");
            
            IEntryIterator itr = store.getTermIdIndex().rangeIterator(null,
                    null);
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();
                
                /*
                 * The sort key for the term. This is not readily decodable. See
                 * RdfKeyBuilder for specifics.
                 */
                byte[] key = tuple.getKey();

                /* 
                 * deserialize the term identifier (packed long integer).
                 */
                final long id;
                try {
                
                    id = tuple.getValueStream().unpackLong();
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
                System.err.println(BytesUtil.toString(key)+":"+id);
                
            }
            
        }
        
        /**
         * Dumps the reverse mapping.
         */
        {

            System.err.println("ids index (reverse mapping).");

            IEntryIterator itr = store.getIdTermIndex().rangeIterator(null,
                    null);
            
            while(itr.hasNext()) {

                ITuple tuple = itr.next();
                
                // decode the term identifier from the sort key.
                final long id = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(), 0);

                _Value term = _Value.deserialize(tuple.getValueStream());
                
                System.err.println( id + ":" + term );
                
            }
        
        }
        
    }
    
}
