/*

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
 * Created on Apr 3, 2008
 */

package com.bigdata.search;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;

/**
 * Test suite using examples based on <a
 * href="http://www.ec-securehost.com/SIAM/SE17.html"><i>Understanding Search
 * Engines</i></a> by Barry and Browne. I recommend the book as a good
 * overview of search engine basis and an excellent reader for latent semantic
 * indexing (Barry was one of the original people involved with LSI).
 * <p>
 * There is a worksheet <code>src/architecture/search.xls</code> that gives
 * the background for this test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearch extends AbstractSearchTest {

    public TestSearch() {
        super();
    }

    public TestSearch(String name) {
        super(name);
    }

    /**
     * Note: the examples have been modified to only expose the terms that were
     * accepted by the indexer used in the book. Since the authors were using
     * stemming, the examples are pre-stemmed. This just reduces the complexity
     * of the system under test.
     */
    final String[] docs = new String[]{
            /* Infant & Toddler First Aid */
            "Infant Toddler",//
            /* Babies Children's Room (For Your Home) */
            "Bab Child Home",//
            /* Child Safety at Home */
            "Child Safety Home",// 
            /* Your Baby's Health and Safety: From Infant to Toddler */
            "Bab Health Safety Infant Toddler",//
            /* Baby Proofing Basics */
            "Bab Proofing",
            /* Your Guide To Easy Rust Proofing */
            "Guide Proofing",
            /* Beanie Babies Collector's Guide */
            "Bab Guide"
    };
    
    /**
     * Overrides some properties to setup the {@link FullTextIndex} configuration.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );

        /*
         * TODO Configure and test with various local, global, and document
         * normalization methods.
         */
        
        return properties;
        
    }

    public void test_ChildProofing() throws InterruptedException {

        /** all documents are in English. */
        final String languageCode = "EN";


        final boolean prefixMatch = false;
        final double minCosine = .0;
        final double maxCosine = 1.0d;
        final int minRank = 1;
        final int maxRank = Integer.MAX_VALUE;// (was 10000)
        final boolean matchAllTerms = false;
        final long timeout = Long.MAX_VALUE;
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final String regex = null;

        init();
        {

            /*
             * Index the documents.
             */
            long docId = 1;
            final int fieldId = 0;
            final TokenBuffer<Long> buffer = new TokenBuffer<Long>(docs.length, getNdx());
            for (String s : docs) {

                getNdx().index(buffer, Long.valueOf(docId++), fieldId,
                        languageCode, new StringReader(s));

            }

            // flush index writes to the database.
            buffer.flush();
        }

        // run query and verify results.
        {

            final String query = "child proofing";

            final Hiterator<Hit<Long>> itr = getNdx().search(new FullTextQuery(
            		query,
                    languageCode, prefixMatch, regex, 
                    matchAllTerms, false/* matchExact*/, 
                    minCosine, maxCosine,
                    minRank, maxRank, timeout, unit));
//                                query, languageCode, 0d/* minCosine */,
//                                Integer.MAX_VALUE/* maxRank */);
            
            assertSameHits(new IHit[] { //
                    new HT<Long>(5L, 0.44194173824159216d),//
                    new HT<Long>(6L, 0.44194173824159216d),//
                    new HT<Long>(2L, 0.35355339059327373d),//
                    new HT<Long>(3L, 0.35355339059327373d),//
            }, itr);
        }

    }
    
    /**
     * Compares the hit list to the expected hit list.
     * <p>
     * Note: Ties on cosine are broken by sorting the ties into increasing order
     * by docId.
     * 
     * @param hits
     *            The expected hits.
     * @param itr
     *            The iterator visiting the actual hits.
     */
    protected void assertSameHits(final IHit[] hits,
            final Iterator<? extends IHit> itr) {

        final int nhits = hits.length;

        for (int i = 0; i < nhits; i++) {

            assertTrue("Iterator exhausted after " + (i) + " hits out of "
                    + nhits, itr.hasNext());

            final IHit expected = hits[i];

            final IHit actual = itr.next();

            if(log.isInfoEnabled())
                log.info("rank=" + (i + 1) + ", expected=" + expected
                        + ", actual: " + actual);

            // first check the document.
            assertEquals("wrong document: rank=" + (i + 1),
                    expected.getDocId(), actual.getDocId());

            /*
             * Verify the cosine.
             * 
             * Note: This allows for some variation in the computed cosine. More
             * variation will be present when the local term weights in the
             * index are stored using single precision (versus double precision)
             * values.
             */
       
            final double expectedCosine = expected.getCosine();
       
            final double actualCosine = actual.getCosine();
            
            if (actualCosine < expectedCosine - .01d
                    || actualCosine > expectedCosine + .01d) {
            
                assertEquals("wrong cosine: rank=" + (i + 1), expected
                        .getCosine(), actual.getCosine());
                
            }

        }

        assertFalse("Iterator will visit too many hits - only " + nhits
                + " are expected", itr.hasNext());

    }

    private static class HT<V extends Comparable<V>> implements IHit<V> {

        final private V docId;

        final private double cosine;

        public HT(final V docId, final double cosine) {

            if (docId == null)
                throw new IllegalArgumentException();

            this.docId = docId;

            this.cosine = cosine;

        }

        public int getRank() {
        	
        	return 0;
        	
        }
        
        public double getCosine() {

            return cosine;

        }

        public V getDocId() {

            return docId;

        }

        public String toString() {

            return "{docId=" + docId + ",cosine=" + cosine + "}";

        }

    }

}
