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
 * Created on Nov 3, 2008
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.htree.HTree;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.stream.Stream.StreamIndexMetadata;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Test suite for {@link DumpJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Test historical journal artifacts for compatibility.
 * 
 *          TODO Test command line utility.
 * 
 *          FIXME GIST : Test other types of indices (HTree (one test exists
 *          now), Stream (no tests yet)).
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 */
public class TestDumpJournal extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestDumpJournal() {
    }

    /**
     * @param name
     */
    public TestDumpJournal(final String name) {

        super(name);
        
    }

    /**
     * Dump an empty journal.
     */
    public void test_emptyJournal() throws IOException, InterruptedException,
            ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, true/* showTuples */);

        } finally {

            src.destroy();

        }
        
    }
    
    /**
     * Dump a journal with a single named index.
     */
    public void test_journal_oneIndexNoData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, true/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with a journal on which a single index has been registered with
     * random data on the index.
     */
    public void test_journal_oneIndexRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            
            {
              
                final BTree ndx = src.getIndex(NAME);
                
                final KV[] a = AbstractBTreeTestCase
                        .getRandomKeyValues(1000/* ntuples */);
                
                for (KV kv : a) {
                
                    ndx.insert(kv.key, kv.val);
                    
                }
                
            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
    * Test with an HTree.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1229" > DumpJournal fails on
    *      non-BTree classes </a>
    */
    public void test_journal_oneIndex_HTree_RandomData_withoutDumpPages() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";

            src.registerIndex(new HTreeIndexMetadata(NAME, UUID.randomUUID()));

            {

                final HTree ndx = (HTree) src.getUnisolatedIndex(NAME);

                final KV[] a = AbstractBTreeTestCase
                        .getRandomKeyValues(1000/* ntuples */);

                for (KV kv : a) {

                    ndx.insert(kv.key, kv.val);

                }

            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, false/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with an HTree.
     */
    public void test_journal_oneIndex_HTree_RandomData_dumpPages() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";

            src.registerIndex(new HTreeIndexMetadata(NAME, UUID.randomUUID()));

            {

                final HTree ndx = (HTree) src.getUnisolatedIndex(NAME);

                final KV[] a = AbstractBTreeTestCase
                        .getRandomKeyValues(1000/* ntuples */);

                for (KV kv : a) {

                    ndx.insert(kv.key, kv.val);

                }

            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
    * Test with an {@link SolutionSetStream}.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1229" > DumpJournal fails on
    *      non-BTree classes </a>
    */
    public void test_journal_oneIndex_SolutionSetStream_NoData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
   
            {
               final StreamIndexMetadata md = new StreamIndexMetadata(NAME,
                     UUID.randomUUID());
               /**
                * TODO GIST : We should not have to do this here. See
                * Checkpoint.create() and SolutionSetStream.create() for why this
                * is necessary.
                * 
                * @see https://sourceforge.net/apps/trac/bigdata/ticket/585 (GIST)
                */
               md.setStreamClassName(SolutionSetStream.class.getName());
               src.registerIndex(md);
            }

            {

                final SolutionSetStream ndx = (SolutionSetStream) src.getUnisolatedIndex(NAME);

//                ndx.put(new Striterator(solutions));
//                
//                final KV[] a = AbstractBTreeTestCase
//                        .getRandomKeyValues(1000/* ntuples */);
//
//                for (KV kv : a) {
//
//                    ndx.insert(kv.key, kv.val);
//
//                }

            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with a journal on which many indices have been registered and
     * populated with random data.
     */
    public void test_journal_manyIndicesRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final String PREFIX = "testIndex#";
        final int NUM_INDICES = 4;

        Journal src = getStore(getProperties());

        try {

            for (int i = 0; i < NUM_INDICES; i++) {

                // register an index
                final String name = PREFIX + i;

                src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
                {

                    // lookup the index.
                    final BTree ndx = src.getIndex(name);

                    // #of tuples to write.
                    final int ntuples = r.nextInt(1000);

                    // generate random data.
                    final KV[] a = AbstractBTreeTestCase
                            .getRandomKeyValues(ntuples);

                    // write tuples (in random order)
                    for (KV kv : a) {

                        ndx.insert(kv.key, kv.val);

                        if (r.nextInt(100) < 10) {

                            // randomly increment the counter (10% of the time).
                            ndx.getCounter().incrementAndGet();

                        }

                    }

                }

            }

            // commit the journal (!)
            src.commit();

            new DumpJournal(src)
                    .dumpJournal(false/* dumpHistory */, true/* dumpPages */,
                            false/* dumpIndices */, false/* showTuples */);
            
            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

            // test again w/o dumpPages
            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, false/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

            /*
             * Now write some more data, going through a series of commit
             * points. This let's us check access to historical commit points.
             */
            for (int j = 0; j < 10; j++) {
                
                for (int i = 0; i < NUM_INDICES; i++) {

                    // register an index
                    final String name = PREFIX + i;

                    // lookup the index.
                    final BTree ndx = src.getIndex(name);

                    // #of tuples to write.
                    final int ntuples = r.nextInt(1000);

                    // generate random data.
                    final KV[] a = AbstractBTreeTestCase
                            .getRandomKeyValues(ntuples);

                    // write tuples (in random order)
                    for (KV kv : a) {

                        ndx.insert(kv.key, kv.val);

                        if (r.nextInt(100) < 10) {

                            // randomly increment the counter (10% of the time).
                            ndx.getCounter().incrementAndGet();

                        }

                    }

                }

                src.commit();

                new DumpJournal(src)
                        .dumpJournal(false/* dumpHistory */,
                                true/* dumpPages */, false/* dumpIndices */,
                                false/* showTuples */);

                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                // test again w/o dumpPages
                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                false/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);
            }

            if (src.isStable()) {

                src = reopenStore(src);

//                src.warmUp(null/*namespaces*/);
                
                new DumpJournal(src)
                        .dumpJournal(false/* dumpHistory */,
                                true/* dumpPages */, false/* dumpIndices */,
                                false/* showTuples */);

                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                // test again w/o dumpPages
                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                false/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

            }
            
            
        } finally {

            src.destroy();

        }

    }

    /**
     * Unit test for {@link DumpJournal} with concurrent updates against the
     * backing store. This is intended primarily to detect failures to protect
     * against the recycling model associated with the {@link IRWStrategy}.
     * 
     * @throws Exception 
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/762">
     *      DumpJournal does not protect against concurrent updates (NSS) </a>
     */
    public void test_dumpJournal_concurrent_updates() throws Exception {
        if(!BigdataStatics.runKnownBadTests) {
           /* FIXME Disabled per #762 pending resolution. Note that DumpJournal
            * concurrent with updates appears to work at the NSS layer as of the
            * 1.5.2 development branch.
            */
           return;
        }
        final String PREFIX = "testIndex#";
        final int NUM_INDICES = 4;

        Journal src = getStore(getProperties());

        try {

            for (int i = 0; i < NUM_INDICES; i++) {

                // register an index
                final String name = PREFIX + i;

                src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
                {

                    // lookup the index.
                    final BTree ndx = src.getIndex(name);

                    // #of tuples to write.
                    final int ntuples = r.nextInt(1000);

                    // generate random data.
                    final KV[] a = AbstractBTreeTestCase
                            .getRandomKeyValues(ntuples);

                    // write tuples (in random order)
                    for (KV kv : a) {

                        ndx.insert(kv.key, kv.val);

                        if (r.nextInt(100) < 10) {

                            // randomly increment the counter (10% of the time).
                            ndx.getCounter().incrementAndGet();

                        }

                    }

                }

            }

            // commit the journal (!)
            src.commit();

            /**
             * Task to run various DumpJournal requests.
             */
            final class DumpTask implements Callable<Void> {

                private final Journal src;
                
                public DumpTask(final Journal src) {
                
                    this.src = src;
                    
                }
                @Override
                public Void call() throws Exception {
                    
                    new DumpJournal(src)
                            .dumpJournal(false/* dumpHistory */,
                                    true/* dumpPages */,
                                    false/* dumpIndices */, false/* showTuples */);

                    new DumpJournal(src)
                            .dumpJournal(true/* dumpHistory */,
                                    true/* dumpPages */, true/* dumpIndices */,
                                    false/* showTuples */);

                    // test again w/o dumpPages
                    new DumpJournal(src)
                            .dumpJournal(true/* dumpHistory */,
                                    false/* dumpPages */,
                                    true/* dumpIndices */, false/* showTuples */);
                    
                    return (Void) null;
                    
                }
            
            }
            
            final class UpdateTask implements Callable<Void> {
                
                private final Journal src;
                
                public UpdateTask(final Journal src) {
                
                    this.src = src;
                    
                }
                @Override
                public Void call() throws Exception {
                    
                    /*
                     * Now write some more data, going through a series of commit
                     * points. This let's us check access to historical commit points.
                     */
                    for (int j = 0; j < 10; j++) {
                        
                        for (int i = 0; i < NUM_INDICES; i++) {

                            // register an index
                            final String name = PREFIX + i;

                            // lookup the index.
                            final BTree ndx = src.getIndex(name);

                            // #of tuples to write.
                            final int ntuples = r.nextInt(1000);

                            // generate random data.
                            final KV[] a = AbstractBTreeTestCase
                                    .getRandomKeyValues(ntuples);

                            // write tuples (in random order)
                            for (KV kv : a) {

                                ndx.insert(kv.key, kv.val);

                                if (r.nextInt(100) < 10) {

                                    // randomly increment the counter (10% of the time).
                                    ndx.getCounter().incrementAndGet();

                                }

                            }

                        }

                        log.info("Will commit");
                        src.commit();
                        log.info("Did commit");

                    }
                
                    return (Void) null;
                }
            }

            final List<FutureTask<Void>> tasks1 = new LinkedList<FutureTask<Void>>();
            final List<FutureTask<Void>> tasks2 = new LinkedList<FutureTask<Void>>();
            final List<FutureTask<Void>> tasksAll = new LinkedList<FutureTask<Void>>();

            // Setup executor that limits parallelism.
            final LatchedExecutor executor1 = new LatchedExecutor(
                    src.getExecutorService(), 1/* nparallel */);

            // Setup executor that limits parallelism.
            final LatchedExecutor executor2 = new LatchedExecutor(
                    src.getExecutorService(), 1/* nparallel */);

            try {

                // Tasks to run.
                tasks1.add(new FutureTaskMon<Void>(new DumpTask(src)));
                tasks1.add(new FutureTaskMon<Void>(new DumpTask(src)));
                tasks1.add(new FutureTaskMon<Void>(new DumpTask(src)));

                tasks2.add(new FutureTaskMon<Void>(new UpdateTask(src)));
                tasks2.add(new FutureTaskMon<Void>(new UpdateTask(src)));
                tasks2.add(new FutureTaskMon<Void>(new UpdateTask(src)));

                // Schedule the tasks.
                for (FutureTask<Void> ft : tasks1) 
                    executor1.execute(ft);
                for (FutureTask<Void> ft : tasks2) 
                    executor2.execute(ft);

                log.info("Blocking for futures");

                // Wait for tasks.
                tasksAll.addAll(tasks1);
                tasksAll.addAll(tasks2);
                int ndone = 0;
                for (FutureTask<Void> ft : tasksAll) {

                    /*
                     * Check Future.
                     * 
                     * Note: sanity check for test termination w/ timeout.
                     */

                    try {
                        ft.get(2, TimeUnit.MINUTES);
                    } catch (ExecutionException ex) {
                        log.error("ndone=" + ndone, ex);
                        throw ex;
                    }

                    log.info("ndone=" + ndone);
                    ndone++;
                }

            } finally {

                // Ensure tasks are terminated.
                for (FutureTask<Void> ft : tasksAll) {
                 
                    ft.cancel(true/*mayInterruptIfRunning*/);
                    
                }
                
            }
            
            if (src.isStable()) {

                src = reopenStore(src);

                // Try running the DumpTask again.
                new DumpTask(src).call();

            }
            
        } finally {

            src.destroy();

        }

    }
    /** Stress test to look for different failure modes. */
    public void _testStress_dumpJournal_concurrent_updates() throws Exception {
        final int LIMIT = 20;
        for (int i = 0; i < LIMIT; i++) {
            if (i > 1)
                setUp();
            try {
                test_dumpJournal_concurrent_updates();
            } catch (Exception ex) {
                log.fatal("FAILURE: i=" + i + ", cause=" + ex);
            }
            if (i + 1 < LIMIT)
                tearDown();
        }
    }
}
