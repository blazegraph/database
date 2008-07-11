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
 * Created on Jun 16, 2008
 */

package com.bigdata.search;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Simple test verifies that the {@link FullTextIndex} data are restart safe
 * when written on a {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafeWithJournal extends TestCase2 {

    /**
     * 
     */
    public TestRestartSafeWithJournal() {
    }

    /**
     * @param name
     */
    public TestRestartSafeWithJournal(String name) {
        super(name);
    }

    public void test_restartSafe() throws IOException, InterruptedException {
        
        final Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        final File file = File.createTempFile(getName(), ".tmp");

        System.err.println("file="+file);
        
        properties.setProperty(Options.FILE,file.toString());
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
        
        final String NAMESPACE = "test";

        Journal journal = new Journal(properties);

        try {

            /*
             * Index some stuff.
             */
            final String text = "The quick brown dog";
            final String languageCode = "EN";
            final long docId = 12L;
            final int fieldId = 3;
            {

                FullTextIndex ndx = new FullTextIndex(journal,NAMESPACE,
                        ITx.UNISOLATED, properties);

                TokenBuffer buffer = new TokenBuffer(2, ndx);

                ndx.index(buffer, docId, fieldId, languageCode,
                        new StringReader(text));

                ndx.index(buffer, docId + 1, fieldId, languageCode,
                        new StringReader("The slow brown cow"));

                buffer.flush();
                
            }

            // do the search.
            {

                FullTextIndex ndx = new FullTextIndex(journal,NAMESPACE,
                        ITx.UNISOLATED, properties);

                Hiterator itr = ndx.search(text, languageCode);

                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                IHit hit1 = itr.next();

                System.err.println("hit1:" + hit1);

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

            // commit changes, including the registration of the index!
            journal.commit();
            
            // close the backing store.
            journal.close();

            // re-open the backing store.
            journal = new Journal(properties);

            /*
             * Do the search.
             */
            {

                FullTextIndex ndx = new FullTextIndex(journal,NAMESPACE,
                        ITx.UNISOLATED, properties);

                Hiterator itr = ndx.search(text, languageCode);

                assertEquals(2, itr.size());

                assertTrue(itr.hasNext());

                IHit hit1 = itr.next();

                System.err.println("hit1:" + hit1);

                /*
                 * Note: with cosine computation only the first hit is visited.
                 */

                assertFalse(itr.hasNext());

            }

        } finally {

            journal.closeAndDelete();

        }

    }

}
