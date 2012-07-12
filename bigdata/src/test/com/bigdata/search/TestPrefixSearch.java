/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Oct 1, 2008
 */

package com.bigdata.search;

import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

/**
 * Unit test for prefix and exact match searches. Prefix search allows a query
 * "bro" to match "brown" rather than requiring an exact match on the search
 * term(s). Exact match searches should only visit tuples which match the full
 * length of the token (once encoded as a Unicode sort key).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPrefixSearch extends ProxyTestCase<IIndexManager> {

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

        final Properties properties = getProperties();
        
        final IIndexManager indexManager = getStore(properties);

        try {

            final String NAMESPACE = "test";

            final FullTextIndex<Long> ndx = new FullTextIndex<Long>(indexManager,
                    NAMESPACE, ITx.UNISOLATED, properties);

            /*
             * Index document(s).
             */
            final long docId = 12L;
            final int fieldId = 3;
            final String languageCode = "EN";
            {

                ndx.create();

                final TokenBuffer<Long> buffer = new TokenBuffer<Long>(2, ndx);

                // index a document. ("The" is a stopword).
                ndx.index(buffer, docId, fieldId, languageCode,
                        new StringReader("The quick brown dog"));

                // index a document. ("The" is a stopword).
                ndx.index(buffer, docId + 1, fieldId, languageCode,
                        new StringReader("The slow brown cow"));

                buffer.flush();

            }

            /* Search (exact match on one document, partial match on the other) */
            {

                final Hiterator<?> itr = ndx.search("The quick brown dog",
                        languageCode, false/* prefixMatch */
                        , minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);
                
                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(2, ndx.count("The quick brown dog",
                        languageCode, false/* prefixMatch */));

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

                final Hiterator<?> itr = ndx.search("The qui bro do",
                        languageCode, true/*prefixMatch*/, minCosine, maxCosine,
                        minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);
                
                if(log.isInfoEnabled()) log.info("hits:" + itr);
                
                assertEquals(2, ndx.count("The qui bro do",
                        languageCode, true/*prefixMatch*/));

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

                final Hiterator<?> itr = ndx
                        .search("brown", languageCode, false/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(2, ndx
                        .count("brown", languageCode, false/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex));

            }

            /*
             * Search (one term, exact match on that term in both documents).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("brown", languageCode, true/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if(log.isInfoEnabled()) log.info("hits:" + itr);

                assertEquals(2, ndx
                        .count("brown", languageCode, true/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex));

            }

            /*
             * Search (one term, prefix match on that term in both documents).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("bro", languageCode, true/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if(log.isInfoEnabled()) log.info("hits:" + itr);

                assertEquals(2, ndx
                        .count("bro", languageCode, true/* prefixMatch */, minCosine, maxCosine,
                        minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex));

            }

            /*
             * Search (one term, no exact match on that term).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("bro", languageCode, false/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(0, itr.size());

            }
            
            /*
             * Search (one term, prefix match on that term in one document).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("qui", languageCode, true/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if(log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(1, itr.size());

            }

            /*
             * Search (one term, no exact match on that term).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("qui", languageCode, false/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(0, itr.size());

            }

            /*
             * Search (one term, exact match on that term in one document).
             */
            {

                final Hiterator<?> itr = ndx
                        .search("quick", languageCode, false/* prefixMatch */, minCosine, maxCosine,
                                minRank, maxRank, matchAllTerms, false/* matchExact*/, timeout, unit, regex);

                if (log.isInfoEnabled())
                    log.info("hits:" + itr);

                assertEquals(1, itr.size());

            }

        } finally {

            indexManager.destroy();

        }

    }

}
