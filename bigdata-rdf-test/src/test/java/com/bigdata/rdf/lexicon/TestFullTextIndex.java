/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.AssertionFailedError;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.util.iterators.Iterators;

import com.bigdata.btree.ChunkedLocalRangeIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.TestSPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.rdf.util.DumpLexicon;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.search.Hit;
import com.bigdata.search.Hiterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Test of adding terms with the full text index enabled and of lookup of terms
 * by tokens which appear within those terms.
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
    
//    /**
//     * Test helper verifies that the term is not in the lexicon, adds the term
//     * to the lexicon, verifies that the term can be looked up by its assigned
//     * term identifier, verifies that the term is now in the lexicon, and
//     * verifies that adding the term again returns the same term identifier.
//     * 
//     * @param term
//     *            The term.
//     */
//    protected void doAddTermTest(final AbstractTripleStore store,
//            final BigdataValue term) {
//
//        assertEquals(NULL, store.getIV(term));
//
//        final IV<?,?> id = store.addTerm(term);
//
//        assertNotSame(NULL, id);
//
//        assertEquals(id, store.getIV(term));
//
//        assertEquals(term, store.getTerm(id));
//
//        assertEquals(id, store.addTerm(term));
//
//    }

    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final String languageCode, 
            final BigdataValue[] expected) {
        
        assertExpectedHits(store, query, languageCode, .4f/* minCosine */,
                expected);

    }

    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final boolean prefixMatch, 
            final BigdataValue[] expected) {
        
        assertExpectedHits(store, query, null, prefixMatch, .4f/* minCosine */,
                expected);

    }

    @SuppressWarnings("unchecked")
    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final String languageCode,
            final float minCosine, final BigdataValue[] expected) {
        
        assertExpectedHits(store, query, languageCode, false, minCosine,
                expected);
        
    }

    @SuppressWarnings("unchecked")
    private void assertExpectedHits(final AbstractTripleStore store,
            final String query, final String languageCode, final boolean prefixMatch,
            final float minCosine, final BigdataValue[] expected) {

        final Hiterator hitr = store.getLexiconRelation().getSearchEngine()
                .search(new FullTextQuery(
                		query, languageCode, 
                		prefixMatch,
                		null,// regex,
                        false,// matchAllTerms
                        false, // matchExact
                		minCosine, 1.0d/* maxCosine */,
                        1/* minRank */, Integer.MAX_VALUE/* maxRank */,
                        Long.MAX_VALUE,//2L/* timeout */,
                        TimeUnit.MILLISECONDS// TimeUnit.SECONDS
                        ));

        // assertEquals("#hits", (long) expected.length, itr.size());

        final ICloseableIterator<BigdataValue> itr2 = new BigdataValueIteratorImpl(
                store, new ChunkedWrappedIterator<IV>(new Striterator(hitr)
                        .addFilter(new Resolver() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            protected Object resolve(Object e) {
                                return ((Hit)e).getDocId();
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

    private LiteralImpl getLargeLiteral(final AbstractTripleStore store) {
        
        final int len = store.getLexiconRelation().getLexiconConfiguration().getBlobsThreshold();

        final StringBuilder sb = new StringBuilder(len);

        final String[] tokens = new String[] {
                "apple",
                "mary",
                "john",
                "barley",
                "mellow",
                "pudding",
                "fries",
                "peal",
                "gadzooks"
        };
        
        for (int i = 0; sb.length() < len; i++) {

            sb.append(tokens[(i % tokens.length)]);

            sb.append(" ");

        }

        final String s = sb.toString();

        if (log.isInfoEnabled())
            log.info("length(s)=" + s.length());

        return new LiteralImpl(s);
    
    }
    
    public void test_fullTextIndex01() throws InterruptedException {

        AbstractTripleStore store = getStore();

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());

            final BigdataValueFactory f = store.getValueFactory();
            
            final LiteralImpl largeLiteral = getLargeLiteral(store);
            
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
                    f.asValue(largeLiteral),//
            };

            store.addTerms(terms);

			if (log.isInfoEnabled()) {
				log.info(DumpLexicon
						.dump(store.getLexiconRelation()));
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
                    .5f, // minCosine
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

            // BLOB.
            assertExpectedHits(store, largeLiteral.getLabel(), null/*lang*/, //
                    .0f, // minCosine
                    new BigdataValue[] {
                    f.asValue(largeLiteral)
                    });

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
                        .5f, // minCosine
                        new BigdataValue[] {//
                        f.createLiteral("good day", "en"), //
                        });

                assertExpectedHits(store, "day", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {
                        f.createLiteral("good day", "en"),
                        f.createLiteral("the first day", "en") });

                // BLOB
                assertExpectedHits(store, largeLiteral.getLabel(), null/*lang*/, //
                        .0f, // minCosine
                        new BigdataValue[] {
                        f.asValue(largeLiteral)
                        });
                
            }
            
        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * Unit text for full text indexing of xsd datatype literals.
     */
    public void test_text_index_datatype_literals() {

        final Properties properties = getProperties();

		/*
		 * Explicitly enable full text indexing of data type literals.
		 */
        properties.setProperty(
                AbstractTripleStore.Options.TEXT_INDEX_DATATYPE_LITERALS,
                "true");

		/*
		 * Explicitly disable inlining of unicode data in the statement indices
		 * (this is not what we are trying to text here.)
		 */
        properties.setProperty(
                AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH,
                "0");

//        // We do not need any vocabulary to test this.
//        properties.setProperty(
//                AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        AbstractTripleStore store = getStore(properties);

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataValue[] terms = new BigdataValue[] {//
                    
                    f.createLiteral("quick brown fox"),//

                    f.createLiteral("slow brown dog", f
                            .asValue(XMLSchema.STRING)),//
                    
                    f.createLiteral("http://www.bigdata.com/mangy/yellow/cat",
                            f.asValue(XMLSchema.ANYURI)),//
                            
                    f.createLiteral("10.128.1.2",
                            f.asValue(XSD.IPV4)),//
            };

            store.addTerms(terms);
            
            if (log.isInfoEnabled())
                log.info(DumpLexicon.dump(store.getLexiconRelation()));
            
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
            
            assertExpectedHits(store, "10.128.", true,
//                  0f, // minCosine,
                    new BigdataValue[] {//
                    f.createLiteral("10.128.1.2",
                            f.asValue(XSD.IPV4)),//
                  });
          
            if(store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                assertNotNull(store.getLexiconRelation().getSearchEngine());
                
                assertExpectedHits(store, "brown", "en", //
                        0f, // minCosine,
                        new BigdataValue[] {//
                        f.createLiteral("quick brown fox"), //
                        f.createLiteral("slow brown dog", XMLSchema.STRING) //
                        });
                
                assertExpectedHits(store, "cat", "en", //
//                        0f, // minCosine,
                        new BigdataValue[] {//
                        f.createLiteral("http://www.bigdata.com/mangy/yellow/cat",
                                f.asValue(XMLSchema.ANYURI))//
                        });
                
                assertExpectedHits(store, "10.128.", true,
//                      0f, // minCosine,
                        new BigdataValue[] {//
                        f.createLiteral("10.128.1.2",
                                f.asValue(XSD.IPV4)),//
                      });
              
            }
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }

	/**
	 * Unit test for indexing fully inline plain, language code, and datatype
	 * literals which using a Unicode representation in the {@link IV}.
	 */
    public void test_text_index_inline_unicode_literals() {

    	if(true) {
    		/*
    		 * TODO The full text index does not currently have a code path for
    		 * inline Unicode Values.  We are considering a refactor which would
    		 * use a [token S P O : relevance] key for the full text index and
    		 * do maintenance on the index when statements are added or retracted.
    		 * It would make sense to support full text indexing for inline 
    		 * Unicode Values at that time since we will be seeing them when we
    		 * write on the statement indices.
    		 */
    		log.warn("Full text index is not supported for inline Unicode at this time.");
    		return;
    	}
    	
        final Properties properties = getProperties();

		/*
		 * Explicitly disable inlining of xsd primitive and numeric datatypes.
		 */
        properties.setProperty(
                AbstractTripleStore.Options.INLINE_XSD_DATATYPE_LITERALS,
                "false");

		/*
		 * Explicitly enable inlining of unicode data in the statement indices.
		 */
        properties.setProperty(
                AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH,
                "256");

        // We do not need any vocabulary to test this.
        properties.setProperty(
                AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        AbstractTripleStore store = getStore(properties);

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
                
                assertExpectedHits(store, "brown", "en", //
                        0f, // minCosine,
                        new BigdataValue[] {//
                        f.createLiteral("quick brown fox"), //
                        f.createLiteral("slow brown dog", XMLSchema.STRING) //
                        });
                
                assertExpectedHits(store, "cat", "en", //
//                        0f, // minCosine,
                        new BigdataValue[] {//
                        f.createLiteral("http://www.bigdata.com/mangy/yellow/cat",
                                f.asValue(XMLSchema.ANYURI))//
                        });
                
            }
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }

    /**
     * Unit test for {@link LexiconRelation#rebuildTextIndex()}.
     */
    public void test_rebuildIndex() {
        
        AbstractTripleStore store = getStore();

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());
            
            BigdataValueFactory f = store.getValueFactory();
            
            final LiteralImpl largeLiteral = getLargeLiteral(store);

            {
            
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
	                    
	                    f.asValue(largeLiteral),//
	
	            };
	            
				final Resource s = f.createURI("x:s");
				
				final URI p = f.createURI("x:p");
	            
        		final IStatementBuffer<Statement> buffer = new StatementBuffer<Statement>(null/* focusStore */, store,
        				terms.length/* capacity */, 0/* queueCapacity */);

        		for (int i = 0; i < terms.length; i++) {

	                buffer.add(s, p, terms[i]);

	            }

        		buffer.flush();
            
            }

            if (log.isInfoEnabled()) {
				log.info(DumpLexicon
						.dump(store.getLexiconRelation()));
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
                    .5f, // minCosine
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

            // BLOB
            assertExpectedHits(store, largeLiteral.getLabel(), null/*lang*/, //
                    .0f, // minCosine
                    new BigdataValue[] {
                    f.asValue(largeLiteral)
                    });

            /*
             * re-open the store before search to verify that the data were made
             * restart safe.
             */
            if (store.isStable()) {

                store.commit();

                store = reopenStore(store);

            } else {
                // we need to manually recreate SearchEngine, as reopening is not supported for temp store
                store.getLexiconRelation().getSearchEngine().destroy();
                try {
                    Field field = store.getLexiconRelation().getClass().getDeclaredField("viewRef");
                    field.setAccessible(true);
                    ((AtomicReference<?>)field.get(store.getLexiconRelation())).set(null);
                    store.getLexiconRelation().getSearchEngine().create();
                } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                    log.error("Error recreating SearchEngine for temp store", e);
                }
            }

            // Ruin full text index
            {
            
            	final String name = store.getLexiconRelation().getNamespace() + "." + BigdataValueCentricFullTextIndex.NAME_SEARCH;
            
            	final IIndex btree = store.getIndexManager().getIndex(name, store.getTimestamp());
	            
	            /*
	             * Range delete the keys matching the filter.
	             */
	            {
	                final ChunkedLocalRangeIterator<?> itr = new ChunkedLocalRangeIterator(
	                        btree,
	                        null/* fromKey */,
	                        null/* toKey */,
	                        100/* capacity */,
	                        IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.REMOVEALL,
	                        new TupleFilter() {
	                            protected boolean isValid(ITuple tuple) { return true; }
	                        });
	
	                while (itr.hasNext()) {
	
	                    final ITuple<?> x = itr.next();
	                    log.info("removed FTS value: " + x);
	
	                }
	            }
            }

            /*
             * re-open the store before search to verify that the data were made
             * restart safe.
             */
            if (store.isStable()) {

                store.commit();

                store = reopenStore(store);

            }

            // verify that full text index is  broken.
            {

                assertNotNull(store.getLexiconRelation().getSearchEngine());
                
                assertExpectedHits(store, "abc", null/* languageCode */,
                        new BigdataValue[] { //
                        });

                assertExpectedHits(store, "tag", "en", new BigdataValue[] {//
                        });

                assertExpectedHits(store, "tag", "de", new BigdataValue[] {//
                        });

                assertExpectedHits(store, "GOOD DAY", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {//
                        });

                assertExpectedHits(store, "GOOD DAY", "en", //
                        .5f, // minCosine
                        new BigdataValue[] {//
                        });

                assertExpectedHits(store, "day", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {
                		});
                
                // BLOB
                assertExpectedHits(store, largeLiteral.getLabel(), null/*lang*/, //
                        .0f, // minCosine
                        new BigdataValue[] {
                        });
                
            }

            // rebuild the full text index.
            {
            	store.getLexiconRelation().rebuildTextIndex(/* forceCreate */ false);

            	final String name = store.getLexiconRelation().getNamespace() + "." + BigdataValueCentricFullTextIndex.NAME_SEARCH;
	            
            	final IIndex btree = store.getIndexManager().getIndex(name, store.getTimestamp());
	            
            	final ITupleIterator itr = btree.rangeIterator();
	            
            	while (itr.hasNext()) {
	            
            		log.info("fixed FTS value: "+itr.next());
	            
            	}

            }


            /*
             * re-open the store before search to verify that the data were made
             * restart safe.
             */
            if (store.isStable()) {

                store.commit();

                store = reopenStore(store);

            }
            
            // re-verify the full text index.
            {
                f = store.getValueFactory();

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
                        .5f, // minCosine
                        new BigdataValue[] {//
                        f.createLiteral("good day", "en"), //
                        });

                assertExpectedHits(store, "day", "en", //
                        .0f, // minCosine
                        new BigdataValue[] {
                        f.createLiteral("good day", "en"),
                        f.createLiteral("the first day", "en") });
                
                // BLOB
                assertExpectedHits(store, largeLiteral.getLabel(), null/*lang*/, //
                        .0f, // minCosine
                        new BigdataValue[] {
                        f.asValue(largeLiteral)
                        });
                
            }
            
        } finally {

            store.__tearDownUnitTest();

        }

    }
    
}
