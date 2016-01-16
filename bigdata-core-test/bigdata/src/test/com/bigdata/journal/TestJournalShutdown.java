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

package com.bigdata.journal;

import java.lang.ref.WeakReference;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.util.Bytes;

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

    private int startupActiveThreads = 0;
    
    public void setUp() throws Exception {

        super.setUp();
        
        startupActiveThreads = Thread.currentThread().getThreadGroup().activeCount();
        
    }

    private static boolean s_checkThreads = true;
    
    public void tearDown() throws Exception {

        TestHelper.checkJournalsClosed(this);
        
        if (s_checkThreads) {

            final ThreadGroup grp = Thread.currentThread().getThreadGroup();
            final int tearDownActiveThreads = grp.activeCount();
            if (startupActiveThreads != tearDownActiveThreads) {
                final Thread[] threads = new Thread[tearDownActiveThreads];
                grp.enumerate(threads);
                final StringBuilder info = new StringBuilder();
                int nactive = 0;
                for (Thread t : threads) {
                    if (t == null)
                        continue;
                    if(nactive>0)
                        info.append(',');
                    info.append("[" + t.getName() + "]");
                    nactive++;
                }
                
                final String failMessage = "Threads left active after task"
                        +": test=" + getName()//
//                        + ", delegate="+getOurDelegate().getClass().getName()
                        + ", startupCount=" + startupActiveThreads
                        + ", teardownCount("+nactive+")=" + tearDownActiveThreads
                        + ", thisThread="+Thread.currentThread().getName()
                        + ", threads: " + info;
                
                if (grp.activeCount() != startupActiveThreads)
                    log.error(failMessage);  

                /*
                 * Wait up to 2 seconds for threads to die off so the next test
                 * will run more cleanly.
                 */
                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);
                    if (grp.activeCount() != startupActiveThreads)
                        break;
                }

            }
            
        }
        
        super.tearDown();
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

//        // This test currently fails.... [this has been fixed].
//        fail("See https://sourceforge.net/apps/trac/bigdata/ticket/196.");
        
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

                    final Journal jnl = new Journal(properties) {
                        protected void finalize() throws Throwable {
                            super.finalize();
                            nunfinalized.decrementAndGet();
                            if (log.isDebugEnabled())
                                log
                                        .debug("Journal was finalized: ncreated="
                                                + ncreated
                                                + ", nalive="
                                                + nunfinalized);
                            // ensure that journals are destroyed when the are finalized.
                            destroy();
                        }
                    };

                    refs[i] = new WeakReference<Journal>(jnl);

                    nunfinalized.incrementAndGet();
                    ncreated.incrementAndGet();

                    /**
                     * FIXME If we submit a task which registers an index on the
                     * new journal then we once again have a memory leak in the
                     * condition where we do not issue an explicit close against
                     * the Journal. [Actually, submitting a read-only task which
                     * does not cause an index to be retained does not cause a
                     * memory leak.]
                     * 
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/196
                     */
                    // force the use of the LockManager.
                    try {
                        
                        /*
                         * Task does not create an index, but does use the
                         * LockManager and the WriteExecutorService.
                         */
                        final AbstractTask task1 = new NOpTask(
                                jnl.getConcurrencyManager(), ITx.UNISOLATED,
                                "name");

                        /*
                         * Task does not create an index. Since it accesses a
                         * historical view, it does not use the LockManager or
                         * the WriteExecutorService.
                         * 
                         * Note: This task may be run w/o causing Journal
                         * references to be retained. However, [task1] and
                         * [task2] will both cause journal references to be
                         * retained.
                         */
                        final AbstractTask task1b = new NOpTask(
                                jnl.getConcurrencyManager(), ITx.READ_COMMITTED,
                                "name");

                        /*
                         * Task uses the LockManager and the
                         * WriteExecutorService and creates an index. A hard
                         * reference to that index will make it into the
                         * journal's index cache.
                         */
                        final AbstractTask task2 = new RegisterIndexTask(
                                jnl.getConcurrencyManager(), "name",
                                new IndexMetadata("name", UUID.randomUUID()));

                        /*
                         * Submit one of the tasks and *wait* for its Future.
                         */
                      jnl.getConcurrencyManager().submit(task1).get();
                      jnl.getConcurrencyManager().submit(task1b).get();
                      jnl.getConcurrencyManager().submit(task2).get();
                        
                    } catch (/*Execution*/Exception e) {
                        log.error("Problem registering index: " + e, e);
                    }
                    
                    if (closeJournal) {
                        /*
                         * Exercise each of the ways in which we can close the
                         * journal.
                         * 
                         * Note: The Journal will not be finalized() unless it
                         * is closed. It runs a variety of services which have
                         * references back to the Journal and which will keep it
                         * from being finalized until those services are
                         * shutdown.
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

            /*
             * Loop, doing GCs waiting for the journal(s) to be finalized.
             */
            int lastCount = nunfinalized.get();
            
//            // Wait for the index references to be expired from the journal caches.
//            Thread.sleep(60000/* ms */);

            for (int i = 0; i < 20 && nunfinalized.get() > 0; i++) {

                // Demand a GC.
                System.gc();

                // Wait for it.
                Thread.sleep(500/* ms */);
                
                final int currentCount = nunfinalized.get();
                
                if (currentCount != lastCount) {
                
                    if (log.isInfoEnabled())
                        log.info("npasses=" + (i + 1) + ", nfinalized="
                                + (lastCount - currentCount) + ", unfinalized="
                                + currentCount);
                    
                    lastCount = currentCount;
                    
                }
                
            }

            if (log.isInfoEnabled()) {

                log.info("Created " + ncreated + " journals.");

                log.info("There are " + nunfinalized
                        + " journals which are still alive.");

            }

            if (nunfinalized.get() > 0) {

                fail("Created " + ncreated
                        + " journals, and " + nunfinalized + " journals were not finalized.");

            }

        } finally {

            /*
             * Ensure that all journals are destroyed by the end of the test.
             */
            int destroyed = 0;
            for (int i = 0; i < refs.length; i++) {
                final Journal jnl = refs[i] == null ? null : refs[i].get();
                if (jnl != null) {
                    destroyed++;
                    jnl.destroy();
                }
            }
            if (destroyed > 0) {
                log.error("Destroyed " + destroyed + " non finalized journals");
            }

        }
        
    }

    /**
     * A task which does nothing, but which will wait for its locks anyway.
     */
    private static class NOpTask extends AbstractTask<Void> {

        /**
         * @param concurrencyManager
         * @param timestamp
         * @param resource
         */
        protected NOpTask(IConcurrencyManager concurrencyManager,
                long timestamp, String resource) {
            super(concurrencyManager, timestamp, resource);
        }

        @Override
        protected Void doTask() throws Exception {
            return null;
        }

    }

}
