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

package com.bigdata.rdf.sparql.ast.cache;

import java.lang.ref.WeakReference;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;

/**
 * Stress test for correct shutdown of the {@link SparqlCache} as allocated by
 * the {@link SparqlCacheFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestQueryEngineFactory.java 4585 2011-06-01 13:42:56Z
 *          thompsonbry $
 */
public class TestSparqlCacheFactory extends TestCase2 {

    /**
     * 
     */
    public TestSparqlCacheFactory() {
    }

    /**
     * @param name
     */
    public TestSparqlCacheFactory(String name) {
        super(name);
    }
    
    public void test_basics() {

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);

        QueryEngine queryEngine = null;

        final Journal jnl = new Journal(properties);

        try {

            // Get the query controller for that Journal.
            queryEngine = QueryEngineFactory.getQueryController(jnl);

            assertNotNull(queryEngine);
            
            // does not exist yet.
            assertNull(SparqlCacheFactory.getExistingSparqlCache(queryEngine));

            // was not created.
            assertNull(SparqlCacheFactory.getExistingSparqlCache(queryEngine));

            final ISparqlCache sparqlCache = SparqlCacheFactory
                    .getSparqlCache(queryEngine);

            // still exists and is the same reference.
            assertTrue(sparqlCache == SparqlCacheFactory
                    .getExistingSparqlCache(queryEngine));

        } finally {

            if (queryEngine != null) {

                queryEngine.shutdownNow();

            }

            jnl.destroy();

        }

    }

    /**
     * Look for a memory leak in the {@link SparqlCacheFactory}.
     * 
     * @throws InterruptedException
     */
    public void test_memoryLeak() throws InterruptedException {

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
        @SuppressWarnings("unchecked")
        final WeakReference<Journal>[] refs = new WeakReference[limit];
        @SuppressWarnings("unchecked")
        final WeakReference<QueryEngine>[] refs2 = new WeakReference[limit];

        try {

            try {

                for (int i = 0; i < limit; i++) {

                    final Journal jnl = new Journal(properties);

                    refs[i] = new WeakReference<Journal>(jnl);

                    final QueryEngine queryEngine;

                    refs2[i] = new WeakReference<QueryEngine>(
                            queryEngine = QueryEngineFactory
                                    .getQueryController(jnl));

                    // does not exist yet.
                    assertNull(SparqlCacheFactory
                            .getExistingSparqlCache(queryEngine));

                    // was not created.
                    assertNull(SparqlCacheFactory
                            .getExistingSparqlCache(queryEngine));

                    final ISparqlCache sparqlCache = SparqlCacheFactory
                            .getSparqlCache(queryEngine);

                    // still exists and is the same reference.
                    assertTrue(sparqlCache == SparqlCacheFactory
                            .getExistingSparqlCache(queryEngine));

                    ncreated++;

                }

            } catch (OutOfMemoryError err) {

                log.error("Out of memory after creating " + ncreated
                        + " instances.");

            }

            // Demand a GC.
            System.gc();

            // Wait for it.
            Thread.sleep(1000/* ms */);

            if (log.isInfoEnabled())
                log.info("Created " + ncreated + " instances.");

            final int nalive = SparqlCacheFactory.getSparqlCacheCount();

            if (log.isInfoEnabled())
                log.info("There are " + nalive
                        + " instances which are still alive.");

            if (nalive == ncreated) {

                fail("No instances were finalized.");

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
