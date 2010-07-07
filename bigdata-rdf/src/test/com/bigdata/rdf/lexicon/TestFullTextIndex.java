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
 * Created on Dec 19, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import junit.framework.AssertionFailedError;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.TestSPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.search.Hit;
import com.bigdata.search.Hiterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;

/**
 * Test of adding terms with the full text index enabled and of lookup of terms
 * by tokens which appear within those terms.
 * 
 * @todo test all term types (uris, bnodes, and literals). only literals are
 *       being indexed right now, but there could be a use case for tokenizing
 *       URIs. There is never going to be any reason to tokenize BNodes.
 * 
 * @todo test XML literal indexing (strip out CDATA and index the tokens found
 *       therein).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFullTextIndex extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestFullTextIndex() {
    }

    /**
     * @param name
     */
    public TestFullTextIndex(String name) {
        super(name);
    }

//    public Properties getProperties() {
//
//        Properties properties = new Properties(super.getProperties());
//        
//        // enable the full text index.
//        properties.setProperty(Options.TEXT_INDEX,"true");
//        
//        return properties;
//
//    }
    
    /**
     * Test helper verifies that the term is not in the lexicon, adds the term
     * to the lexicon, verifies that the term can be looked up by its assigned
     * term identifier, verifies that the term is now in the lexicon, and
     * verifies that adding the term again returns the same term identifier.
     * 
     * @param term
     *            The term.
     */
    protected void doAddTermTest(final AbstractTripleStore store,
            final BigdataValue term) {

        assertEquals(NULL, store.getIV(term));

        final IV id = store.addTerm(term);

        assertNotSame(NULL, id);

        assertEquals(id, store.getIV(term));

        assertEquals(term, store.getTerm(id));

        assertEquals(id, store.addTerm(term));

    }

    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final String languageCode, 
            final BigdataValue[] expected) {
        
        assertExpectedHits(store, query, languageCode, .4f/* minCosine */,
                expected);

    }

    @SuppressWarnings("unchecked")
    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final String languageCode,
            final float minCosine, final BigdataValue[] expected) {

        final Hiterator hitr = store.getLexiconRelation().getSearchEngine()
                .search(query, languageCode, false/* prefixMatch */, minCosine,
                        Integer.MAX_VALUE/* maxRank */, 2L/* timeout */,
                        TimeUnit.SECONDS);

        // assertEquals("#hits", (long) expected.length, itr.size());

        final ICloseableIterator<BigdataValue> itr2 = new BigdataValueIteratorImpl(
                store, new ChunkedWrappedIterator<IV>(new Striterator(hitr)
                        .addFilter(new Resolver() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            protected Object resolve(Object e) {
                                return new TermId(((Hit) e).getDocId());
                            }
                        })));

        try {

            TestSPOKeyOrder.assertSameIteratorAnyOrder(expected, itr2);

        } catch (AssertionFailedError ex) {

            fail("minCosine=" + minCosine + ", expected="
                    + Arrays.toString(expected) + ", actual=" + hitr, ex);

        } finally {

            itr2.close();

        }
        
    }

    public void test_fullTextIndex01() throws InterruptedException {

        AbstractTripleStore store = getStore();

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataValue[] terms = new BigdataValue[] {//
                    f.createLiteral("abc"),//
                    f.createLiteral("abc", "en"),//
                    f.createLiteral("good day", "en"),//
                    f.createLiteral("gutten tag", "de"),//
                    f.createLiteral("tag team", "en"),//
                    f.createLiteral("the first day", "en"),// // 'the' is a stopword.

                    f.createURI("http://www.bigdata.com"),//
                    f.asValue(RDF.TYPE),//
                    f.asValue(RDFS.SUBCLASSOF),//
                    f.asValue(XMLSchema.DECIMAL),//

                    f.createBNode(UUID.randomUUID().toString()),//
                    f.createBNode("a12"),//
            };

            store.addTerms(terms);

            if(log.isInfoEnabled()) {
                log.info(store.getLexiconRelation().dumpTerms());
            }

            /*
             * Note: the language code is only used when tokenizing literals. It
             * IS NOT applied as a filter to the recovered literals.
             */
            
            assertExpectedHits(store, "abc", null/* languageCode */,
                    new BigdataValue[] { //
                    f.createLiteral("abc"),//
                            f.createLiteral("abc", "en") //
                    });

            assertExpectedHits(store, "tag", "en", new BigdataValue[] {//
                    f.createLiteral("gutten tag", "de"), //
                    f.createLiteral("tag team", "en") //
                    });

            assertExpectedHits(store, "tag", "de", new BigdataValue[] {//
                    f.createLiteral("gutten tag", "de"), //
                    f.createLiteral("tag team", "en") //
                    });

            assertExpectedHits(store, "GOOD DAY", "en", //
                    .0f, // minCosine
                    new BigdataValue[] {//
                    f.createLiteral("good day", "en"), //
                    f.createLiteral("the first day", "en") //
                    });

            assertExpectedHits(store, "GOOD DAY", "en", //
                    .4f, // minCosine
                    new BigdataValue[] {//
                    f.createLiteral("good day", "en"), //
                    });

            assertExpectedHits(store, "day", "en", //
                    .0f, // minCosine
                    new BigdataValue[] {
                    f.createLiteral("good day", "en"),
                    f.createLiteral("the first day", "en") });

            // 'the' is a stopword, so there are no hits.
            assertExpectedHits(store, "the", "en", new BigdataValue[] {});

            /*
             * re-open the store before search to verify that the data were made
             * restart safe.
             */
            if (store.isStable()) {

                store.commit();

                store = reopenStore(store);

                assertNotNull(store.getLexiconRelation().getSearchEngine());
                
                assertExpectedHits(store, "abc", null/* languageCode */,
                        new BigdataValue[] { //
                        f.createLiteral("abc"),//
                                f.createLiteral("abc", "en") //
                        });

                assertExpectedHits(store, "tag", "en", new BigdataValue[] {//
                        f.createLiteral("gutten tag", "de"), //
                        f.createLiteral("tag team", "en") //
                        });

                assertExpectedHits(store, "tag", "de", new BigdataValue[] {//
                        f.createLiteral("gutten tag", "de"), //
                        f.createLiteral("tag team", "en") //
                        });

                assertExpectedHits(store, "GOOD DAY", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {//
                        f.createLiteral("good day", "en"), //
                        f.createLiteral("the first day", "en") //
                        });

                assertExpectedHits(store, "GOOD DAY", "en", //
                        .4f, // minCosine
                        new BigdataValue[] {//
                        f.createLiteral("good day", "en"), //
                        });

                assertExpectedHits(store, "day", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {
                        f.createLiteral("good day", "en"),
                        f.createLiteral("the first day", "en") });
                
            }
            
        } finally {

            store.__tearDownUnitTest();

        }

    }

    public void test_text_index_datatype_literals() {

        final Properties properties = getProperties();

        // explicitly enable full text indexing of data type literals.
        properties.setProperty(
                AbstractTripleStore.Options.TEXT_INDEX_DATATYPE_LITERALS,
                "true");
        
        AbstractTripleStore store = getStore();

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataValue[] terms = new BigdataValue[] {//
                    
                    f.createLiteral("quick brown fox"),//

                    f.createLiteral("slow brown dog", f
                            .asValue(XMLSchema.STRING)),//
                    
                    f.createLiteral("http://www.bigdata.com/mangy/yellow/cat",
                            f.asValue(XMLSchema.ANYURI)),//
            };

            store.addTerms(terms);

            assertExpectedHits(store, "brown", "en", //
                    0f, // minCosine,
                    new BigdataValue[] {//
                    f.createLiteral("quick brown fox"), //
                    f.createLiteral("slow brown dog", XMLSchema.STRING) //
                    });
            
            assertExpectedHits(store, "cat", "en", //
//                    0f, // minCosine,
                    new BigdataValue[] {//
                    f.createLiteral("http://www.bigdata.com/mangy/yellow/cat",
                            f.asValue(XMLSchema.ANYURI))//
                    });
            
            if(store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                assertNotNull(store.getLexiconRelation().getSearchEngine());
                
                // @todo retest.
                
            }
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }
    
}
