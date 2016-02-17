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
 * Created on Jan 23, 2008
 */

package com.bigdata.search;

import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;

/**
 * Simple test verifies that the {@link FullTextIndex} data are restart safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearchRestartSafe extends AbstractSearchTest {

    /**
     * 
     */
    public TestSearchRestartSafe() {
    }

    /**
     * @param arg0
     */
    public TestSearchRestartSafe(String arg0) {
        super(arg0);
    }

//    final File file;
//    {
//        
//        try {
//
//            file = File.createTempFile(getName(), ".tmp");
//   
//            System.err.println("file="+file);
//         
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//    }

//    public Properties getProperties() {
//
//        Properties properties = new Properties( super.getProperties() );
//            
//        // Note: overrides the buffer mode so that we can re-open it.
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                .toString());
//        
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return properties;
//        
//    }
    
    public void test_simple() throws InterruptedException {

        final boolean prefixMatch = false;
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
         * Index a document.
         */
        final long docId = 12L;
        final int fieldId = 3;
        final String text = "The quick brown dog";
        final String languageCode = "EN";
        {

            final TokenBuffer<Long> buffer = new TokenBuffer<Long>(2, getNdx());

            getNdx().index(buffer, docId, fieldId, languageCode,
                    new StringReader(text));

            getNdx().index(buffer, docId + 1, fieldId, languageCode,
                    new StringReader("The slow brown cow"));

            buffer.flush();

        }

        /* Search w/o restart. */
        {

        	final FullTextIndex<Long> ndx = new FullTextIndex<Long>(getIndexManager(),
                    getNamespace(), ITx.UNISOLATED, getSearchProperties());

            final Hiterator<?> itr = 
//                    ndx.search(
//                        text, languageCode
//                        );
            ndx.search(new FullTextQuery(text,
                    languageCode, prefixMatch, 
                    regex, matchAllTerms, false/* matchExact*/, 
                    minCosine, maxCosine,
                    minRank, maxRank, timeout, unit));
            
            assertEquals(1, itr.size()); // Note: 2nd result pruned by cosine.

            assertTrue(itr.hasNext());

            final IHit<?> hit1 = itr.next();

            if(log.isInfoEnabled())
                log.info("hit1:" + hit1);

//                /*
//                 * Note: with cosine computation only the first hit is visited.
//                 */

            assertFalse(itr.hasNext());

        }

        /*
         * Shutdown and restart.
         */
        setIndexManager(reopenStore(getIndexManager()));

        /* Search with restart. */
        {
        	final FullTextIndex<Long> ndx = new FullTextIndex<Long>(getIndexManager(), getNamespace(), 
        			ITx.UNISOLATED, getSearchProperties());


            final Hiterator<?> itr = // ndx.search(text, languageCode);
                ndx.search(new FullTextQuery(text,
                        languageCode, prefixMatch, 
                        regex, matchAllTerms, false/* matchExact*/, 
                        minCosine, maxCosine,
                        minRank, maxRank, timeout, unit));

            assertEquals(1, itr.size()); // Note: 2nd result pruned by cosine.

            assertTrue(itr.hasNext());

            final IHit<?> hit1 = itr.next();

            if(log.isInfoEnabled())
                log.info("hit1:" + hit1);

//                /*
//                 * Note: with cosine computation only the first hit is visited.
//                 */

            assertFalse(itr.hasNext());

        }

    }

}
