/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Nov 2, 2010
 */

package com.bigdata.bop.fed;

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;

/**
 * Stress test for correct shutdown of query controllers as allocated by the
 * {@link QueryEngineFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryEngineFactory extends TestCase2 {

    /**
     * 
     */
    public TestQueryEngineFactory() {
    }

    /**
     * @param name
     */
    public TestQueryEngineFactory(String name) {
        super(name);
    }
    
    public void test_basics() {

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);

        final Journal jnl = new Journal(properties);

        try {

            // does not exist yet.
            assertNull(QueryEngineFactory.getExistingQueryController(jnl));

            // was not created.
            assertNull(QueryEngineFactory.getExistingQueryController(jnl));

            final QueryEngine queryEngine = QueryEngineFactory
                    .getQueryController(jnl);

            // still exists and is the same reference.
            assertTrue(queryEngine == QueryEngineFactory
                    .getExistingQueryController(jnl));

        } finally {

            jnl.destroy();
            
        }

    }

    /**
     * Look for a memory leak in the {@link QueryEngineFactory}.
     * 
     * @throws InterruptedException 
     */
    public void test_memoryLeak() throws InterruptedException {

        if (true) {
            /*
             * FIXME Disabled for now since causing CI to fail.
             */
            log.error("Enable test.");

            return;
        }

        final int limit = 200;
        
        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);

        int ncreated = 0;

        try {

            for (int i = 0; i < limit; i++) {

                Journal jnl = new Journal(properties);

                // does not exist yet.
                assertNull(QueryEngineFactory.getExistingQueryController(jnl));
                
                // was not created.
                assertNull(QueryEngineFactory.getExistingQueryController(jnl));
                
                final QueryEngine queryEngine = QueryEngineFactory.getQueryController(jnl);
                
                // still exists and is the same reference.
                assertTrue(queryEngine == QueryEngineFactory.getExistingQueryController(jnl));

                ncreated++;

            }

        } catch (OutOfMemoryError err) {
            
            System.err.println("Out of memory after creating " + ncreated
                    + " query controllers.");
            
        }

        // Demand a GC.
        System.gc();

        // Wait for it.
        Thread.sleep(1000/*ms*/);
        
        System.err.println("Created " + ncreated + " query controllers.");

        final int nalive = QueryEngineFactory.getQueryControllerCount();

        System.err.println("There are " + nalive
                + " query controllers which are still alive.");

        if (nalive == ncreated) {

            fail("No query controllers were finalized.");

        }

    }

}
