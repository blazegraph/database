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
 * Created on Nov 2, 2010
 */

package com.bigdata.bop.fed;

import java.lang.ref.WeakReference;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.util.Bytes;

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
            assertNull(QueryEngineFactory.getInstance().getExistingQueryController(jnl));

            // was not created.
            assertNull(QueryEngineFactory.getInstance().getExistingQueryController(jnl));

			final QueryEngine queryEngine = QueryEngineFactory.getInstance().getQueryController(jnl);

			// still exists and is the same reference.
			assertTrue(queryEngine == QueryEngineFactory.getInstance().getExistingQueryController(jnl));

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

        // This test currently fails.... [this has been fixed.]
        // fail("See https://sourceforge.net/apps/trac/bigdata/ticket/196.");

        final int limit = 200;

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);

        int ncreated = 0;

        /*
         * An array of weak references to the journals. These references will
         * not cause the journals to be retained. However, since we can not
         * force a major GC, the non-cleared references are used to ensure that
         * all journals are destroyed by the end of this test.
         */
        final WeakReference<Journal>[] refs = new WeakReference[limit];

        try {

            try {

                for (int i = 0; i < limit; i++) {

                    final Journal jnl = new Journal(properties);

                    refs[i] = new WeakReference<Journal>(jnl);

                    // does not exist yet.
                    assertNull(QueryEngineFactory.getInstance()
                            .getExistingQueryController(jnl));

                    // was not created.
                    assertNull(QueryEngineFactory.getInstance()
                            .getExistingQueryController(jnl));

                    final QueryEngine queryEngine = QueryEngineFactory.getInstance()
                            .getQueryController(jnl);

                    // still exists and is the same reference.
                    assertTrue(queryEngine == QueryEngineFactory.getInstance()
                            .getExistingQueryController(jnl));

                    ncreated++;

                }

            } catch (OutOfMemoryError err) {

                log.error("Out of memory after creating " + ncreated
                        + " query controllers.");

            }

            // Demand a GC.
            System.gc();

            // Wait for it.
            Thread.sleep(1000/* ms */);

            if (log.isInfoEnabled())
                log.info("Created " + ncreated + " query controllers.");

            final int nalive = QueryEngineFactory.getInstance().getQueryControllerCount();

            if (log.isInfoEnabled())
                log.info("There are " + nalive
                        + " query controllers which are still alive.");

            if (nalive == ncreated) {

                fail("No query controllers were finalized.");

            }

        } finally {

            /*
             * Ensure that all journals are destroyed by the end of the test.
             */
            for (int i = 0; i < refs.length; i++) {
                final Journal jnl = refs[i] == null ? null : refs[i].get();
                if (jnl != null) {
                    jnl.destroy();
                }
            }

        }

    }

}
