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

package com.bigdata.journal;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

import com.bigdata.rawstore.Bytes;

/**
 * Stress test for correct shutdown of journals based on weak reference
 * semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJournalShutdown extends TestCase2 {

    /**
     * 
     */
    public TestJournalShutdown() {
    }

    /**
     * @param name
     */
    public TestJournalShutdown(String name) {
        super(name);
    }

    /**
     * Look for a memory leak when the test calls {@link Journal#close()}
     * explicitly.
     * 
     * @throws InterruptedException
     */
    public void test_memoryLeakWithExplicitClose() throws InterruptedException {

        doMemoryLeakTest(true);
    }

    /**
     * Look for a memory leak when the test DOES NOT call
     * {@link Journal#close()} explicitly and instead relies on the JVM to
     * invoke finalized() on the {@link Journal}.
     * <p>
     * Note: You SHOULD NOT need to close the Journal. Once it is no longer
     * strongly referenced it SHOULD get finalized(). This MAY be set to [true]
     * to verify that the journal is properly shutting down all of its thread
     * pools, but it MUST be [false] for CI since the whole purpose of this test
     * is to verify that Journals are eventually finalized() if the application
     * no longer holds a strong reference to the journal.
     * 
     * @throws InterruptedException
     */
    public void test_memoryLeakWithoutExplicitClose()
            throws InterruptedException {

        // This test currently fails....
        fail("See https://sourceforge.net/apps/trac/bigdata/ticket/196.");
        
        doMemoryLeakTest(false);
        
    }

    /**
     * Test helper looks for a memory leak in the {@link Journal}.
     * 
     * @param closeJournal
     *            when <code>true</code> the test will close each
     *            {@link Journal} that it creates. Otherwise, it relies on the
     *            finalized() method to close() the {@link Journal}.
     * 
     * @throws InterruptedException
     */
    private void doMemoryLeakTest(final boolean closeJournal)
            throws InterruptedException {

        final int limit = 200;

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        properties.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);

        final AtomicInteger ncreated = new AtomicInteger();
        final AtomicInteger nunfinalized = new AtomicInteger();

        try {

            for (int i = 0; i < limit; i++) {

                final Journal jnl = new Journal(properties) {
                    protected void finalize() throws Throwable {
                        super.finalize();
                        nunfinalized.decrementAndGet();
                        if (log.isDebugEnabled())
                            log.debug("Journal was finalized: ncreated="
                                    + ncreated + ", nalive=" + nunfinalized);
                    }
                };

                nunfinalized.incrementAndGet();
                ncreated.incrementAndGet();

                if (closeJournal) {
                    /*
                     * Exercise each of the ways in which we can close the
                     * journal.
                     * 
                     * Note: The Journal will not be finalized() unless it is
                     * closed. It runs a variety of services which have
                     * references back to the Journal and which will keep it
                     * from being finalized until those services are shutdown.
                     */
                    switch (i % 4) {
                    case 0:
                        jnl.shutdown();
                        break;
                    case 1:
                        jnl.shutdownNow();
                        break;
                    case 2:
                        jnl.close();
                        break;
                    case 3:
                        jnl.destroy();
                        break;
                    default:
                        throw new AssertionError();
                    }

                }

            }

        } catch (OutOfMemoryError err) {

            log.error("Out of memory after creating " + ncreated
                            + " journals.");

        }

        // Demand a GC.
        System.gc();

        // Wait for it.
        Thread.sleep(1000/* ms */);

        if (log.isInfoEnabled()) {

            log.info("Created " + ncreated + " journals.");

            log.info("There are " + nunfinalized
                    + " journals which are still alive.");

        }

        if (nunfinalized.get() == ncreated.get()) {

            fail("Created " + ncreated
                    + " journals.  No journals were finalized.");

        }

    }

}
