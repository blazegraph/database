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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.scaleout;

import java.io.IOException;
import java.util.UUID;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rdf.ITripleStore;
import com.bigdata.rdf.TestTripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Test suite for use of the terms and ids indices.
 * <p>
 * In order to run connected to data services, you MUST specify at least the
 * following properties to the JVM and have access to the resources in
 * <code>src/resources/config</code>.
 * 
 * <pre>
 *       -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/ *
 * </pre>
 * 
 * FIXME write a stress test with concurrent threads inserting terms and having
 * occasional failures between the insertion into terms and the insertion into
 * ids and verify that the resulting mapping is always fully consistent because
 * the client verifies that the term is in the ids mapping on each call to
 * addTerm().
 * 
 * FIXME write a performance test with (a) a single client thread; and (b)
 * concurrent clients/threads loaded terms into the KB and use it to tune the
 * batch load operations (there is little point to tuning the single term load
 * operations).
 * 
 * @todo This duplicates tests found in {@link TestTripleStore} since the test
 *       harness is not yet invariant so that it can run against each of the
 *       {@link ITripleStore} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermAndIdsIndex extends AbstractDistributedTripleStoreTestCase {

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
        doAddTermTest(new _URI(XmlSchema.DECIMAL));

        doAddTermTest(new _Literal("abc"));
        doAddTermTest(new _Literal("abc", new _URI(XmlSchema.DECIMAL)));
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
                new _URI(XmlSchema.DECIMAL),//

                new _Literal("abc"),//
                new _Literal("abc", new _URI(XmlSchema.DECIMAL)),//
                new _Literal("abc", "en"),//

                new _BNode(UUID.randomUUID().toString()),//
                new _BNode("a12") //
        };

        store.insertTerms(terms, terms.length, false/* haveKeys */, false/* sorted */);
        
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
                
                // the term identifier.
                byte[] val = (byte[]) itr.next();
                
                /*
                 * The sort key for the term. This is not readily decodable. See
                 * RdfKeyBuilder for specifics.
                 */
                byte[] key = itr.getKey();

                /* 
                 * deserialize the term identifier (packed long integer).
                 */
                final long id;
                try {
                
                    id = new DataInputBuffer(val).unpackLong();
                    
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
                
                // the serialized term.
                byte[] val = (byte[]) itr.next();
                
                // the sort key for the term identifier.
                byte[] key = itr.getKey();
                
                // decode the term identifier from the sort key.
                final long id = KeyBuilder.decodeLong(key, 0);

                _Value term = _Value.deserialize(val);
                
                System.err.println( id + ":" + term );
                
            }
        
        }
        
    }
    
}
