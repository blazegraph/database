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

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

/**
 * Unit test for prefix search. Prefix search allows a query "bro" to match
 * "brown" rather than requiring an exact match on the search term(s).
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

        final Properties properties = getProperties();
        
        final IIndexManager indexManager = getStore(properties);

        try {

            final String NAMESPACE = "test";

            final FullTextIndex ndx = new FullTextIndex(indexManager,
                    NAMESPACE, ITx.UNISOLATED, properties);

            /*
             * Index document(s).
             */
            final long docId = 12L;
            final int fieldId = 3;
            final String languageCode = "EN";
            {

                ndx.create();

                TokenBuffer buffer = new TokenBuffer(2, ndx);

                // index a document.
                ndx.index(buffer, docId, fieldId, languageCode,
                        new StringReader("The quick brown dog"));

                // index a document.
                ndx.index(buffer, docId + 1, fieldId, languageCode,
                        new StringReader("The slow brown cow"));

                buffer.flush();

            }

            /* Search (exact match on one document, partial match on the other) */
            {

                final Hiterator itr = ndx.search("The quick brown dog",
                        languageCode, false/* prefixMatch */);
                
                if(INFO) log.info("hits:" + itr);
                
                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                final IHit hit1 = itr.next();

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

                final Hiterator itr = ndx.search("The qui bro do",
                        languageCode, true/*prefixMatch*/);
                
                if(INFO) log.info("hits:" + itr);
                
                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                final IHit hit1 = itr.next();

                assertEquals(12L,hit1.getDocId());

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

            /*
             * Search (one term, exact match on that term in both documents).
             */
            {

                final Hiterator itr = ndx
                        .search("brown", languageCode, false/*prefixMatch*/);

                if(INFO) log.info("hits:" + itr);

                assertEquals(2, itr.size());

            }

            /*
             * Search (one term, prefix match on that term in both documents).
             */
            {

                final Hiterator itr = ndx
                        .search("bro", languageCode, true/* prefixMatch */);

                if(INFO) log.info("hits:" + itr);

                assertEquals(2, itr.size());

            }

            /*
             * Search (one term, prefix match on that term in one document).
             */
            {

                final Hiterator itr = ndx.search("qui", languageCode);

                if(INFO) log.info("hits:" + itr);

                assertEquals(1, itr.size());

            }

        } finally {

            indexManager.destroy();

        }

    }

}
