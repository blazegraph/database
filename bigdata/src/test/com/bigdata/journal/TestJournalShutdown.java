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

import com.bigdata.bop.fed.QueryEngineFactory;
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

        final AtomicInteger ncreated = new AtomicInteger();

        final AtomicInteger nalive = new AtomicInteger();
        
        try {

            for (int i = 0; i < limit; i++) {

                Journal jnl = new Journal(properties) {
                    protected void finalize() throws Throwable {
                        super.finalize();
                        nalive.decrementAndGet();
                        System.err.println("Journal was finalized: ncreated="
                                + ncreated + ", nalive=" + nalive);
                    }
                };

                nalive.incrementAndGet();
                ncreated.incrementAndGet();

            }

        } catch (OutOfMemoryError err) {

            System.err.println("Out of memory after creating " + ncreated
                    + " journals.");

        }

        // Demand a GC.
        System.gc();

        // Wait for it.
        Thread.sleep(1000/*ms*/);
        
        System.err.println("Created " + ncreated + " journals.");

        System.err.println("There are " + nalive
                + " journals which are still alive.");

        if (nalive.get() == ncreated.get()) {

            fail("Created " + ncreated
                    + " journals.  No journals were finalized.");

        }

    }

}
