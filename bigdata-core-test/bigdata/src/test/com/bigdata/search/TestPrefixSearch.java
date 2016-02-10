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
 * Created on Oct 1, 2008
 */

package com.bigdata.search;

import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;

/**
 * Unit test for prefix and exact match searches. Prefix search allows a query
 * "bro" to match "brown" rather than requiring an exact match on the search
 * term(s). Exact match searches should only visit tuples which match the full
 * length of the token (once encoded as a Unicode sort key).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPrefixSearch extends AbstractSearchTest {

    /**
     * 
     */
    public TestPrefixSearch() {
    }

    /**
     * @param name
     */
    public TestPrefixSearch(String name) {
        
        super(name);
        
    }

    public void test_prefixSearch() throws InterruptedException {

        final double minCosine = .4;
        final double maxCosine = 1.0d;
        final int minRank = 1;
        final int maxRank = Integer.MAX_VALUE;// (was 10000)
        final boolean matchAllTerms = false;
        final long timeout = Long.MAX_VALUE;
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final String regex = null;

        init();


            /*
             * Index document(s).
             */
            final long docId = 12L;
            final int fieldId = 3;
            final String languageCode = "EN";
            {


                final TokenBuffer<Long> buffer = new TokenBuffer<Long>(2, getNdx());

                // index a document. ("The" is a stopword).
                getNdx().index(buffer, docId, fieldId, languageCode,
                        new StringReader("The quick brown dog"));

                // index a document. ("The" is a stopword).
                getNdx().index(buffer, docId + 1, fieldId, languageCode,
                        new StringReader("The slow brown cow"));

                buffer.flush();

            }

            /* Search (exact match on one document, partial match on the other) */
            {

                final Hiterator<?> itr = getNdx().search(new FullTextQuery("The quick brown dog",
                        languageCode, false/* prefixMatch */
                        , regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));
                
                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(2, getNdx().count(new FullTextQuery("The quick brown dog",
                        languageCode, false/* prefixMatch */)));

                assertTrue(itr.hasNext());

                final IHit<?> hit1 = itr.next();

                assertEquals(12L,hit1.getDocId());

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

            /*
             * Search (prefix matches on one document, partial prefix matches on
             * the other)
             */
            {

                final Hiterator<?> itr = getNdx().search(new FullTextQuery("The qui bro do",
                        languageCode, true/*prefixMatch*/, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                        minRank, maxRank, timeout, unit));
                
                if(log.isInfoEnabled()) log.info("hits:" + itr);
                
                assertEquals(2, getNdx().count(new FullTextQuery("The qui bro do",
                        languageCode, true/*prefixMatch*/)));

                assertTrue(itr.hasNext());

                final IHit<?> hit1 = itr.next();

                assertEquals(12L,hit1.getDocId());

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

            /*
             * Search (one term, prefix match on that term in both documents
             * (the prefix match is an exact match in this case)).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("brown", languageCode, false/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(2, getNdx()
                        .count(new FullTextQuery("brown", languageCode, false/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit)));

            }

            /*
             * Search (one term, exact match on that term in both documents).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("brown", languageCode, true/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if(log.isInfoEnabled()) log.info("hits:" + itr);

                assertEquals(2, getNdx()
                        .count(new FullTextQuery("brown", languageCode, true/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit)));

            }

            /*
             * Search (one term, prefix match on that term in both documents).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("bro", languageCode, true/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if(log.isInfoEnabled()) log.info("hits:" + itr);

                assertEquals(2, getNdx()
                        .count(new FullTextQuery("bro", languageCode, true/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                        minRank, maxRank, timeout, unit)));

            }

            /*
             * Search (one term, no exact match on that term).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("bro", languageCode, false/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(0, itr.size());

            }
            
            /*
             * Search (one term, prefix match on that term in one document).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("qui", languageCode, true/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(1, itr.size());

            }

            /*
             * Search (one term, no exact match on that term).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("qui", languageCode, false/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(0, itr.size());

            }

            /*
             * Search (one term, exact match on that term in one document).
             */
            {

                final Hiterator<?> itr = getNdx()
                        .search(new FullTextQuery("quick", languageCode, false/* prefixMatch */, regex, matchAllTerms, false/* matchExact*/, minCosine, maxCosine,
                                minRank, maxRank, timeout, unit));

                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(1, itr.size());

            }


    }

}
