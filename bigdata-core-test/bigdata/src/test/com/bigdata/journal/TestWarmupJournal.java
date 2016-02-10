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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;

/**
 * Test suite for {@link WarmUpTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal on
 *      startup </a>
 */
public class TestWarmupJournal extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestWarmupJournal() {
    }

    /**
     * @param name
     */
    public TestWarmupJournal(String name) {
        super(name);
    }

    /**
     * Verify operation Ok when nothing has been written on the journal.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
   public void test_emptyJournal() throws IOException, InterruptedException,
         ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            // create and submit task.
            final Future<Map<String, BaseIndexStats>> f = src
                  .warmUp(null/* namespaces */);

            // await future.
            final Map<String, BaseIndexStats> map = f.get();

            assertEquals(0, map.size());

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

   }
    
   /**
     * Test of a journal on which a single index has been register (and the
     * journal committed) but no data was written onto the index.
     */
    public void test_journal_oneIndexNoData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            src.commit();

            // create and submit task.
            final Future<Map<String, BaseIndexStats>> f = src
                  .warmUp(null/* namespaces */);

            // await future.
            final Map<String, BaseIndexStats> map = f.get();

            // verify state
            assertEquals(1,map.size());
            
            final BaseIndexStats stats = map.get(NAME);
            
            assertNotNull(stats);
            
            assertEquals(0, stats.height);
            assertEquals(0, stats.nnodes);
            assertEquals(1, stats.nleaves);

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

    }

    /**
     * Test with a journal on which a single index has been registered with
     * random data on the index.
     */
    public void test_journal_oneIndexRandomData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            int ntuples = 0;
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            {
               final BTree ndx = src.getIndex(NAME);
               final KV[] a = AbstractBTreeTestCase
                     .getRandomKeyValues(1000/* ntuples */);
               for (KV kv : a) {
                  if (ndx.insert(kv.key, kv.val) == null) {
                     // only count first time inserted (vs replaced).
                     ntuples++;
                  }
               }
            }
            src.commit();

            // create and submit task.
            final Future<Map<String, BaseIndexStats>> f = src
                  .warmUp(null/* namespaces */);

            // await future.
            final Map<String, BaseIndexStats> map = f.get();

            // verify state
            assertEquals(1,map.size());
            
            final BaseIndexStats stats = map.get(NAME);
            
            assertNotNull(stats);

            assertEquals(ntuples, stats.ntuples);

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

    }

    /**
     * Test with a journal on which many indices have been registered and
     * populated with random data.
     */
    public void test_journal_manyIndicesRandomData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            final String PREFIX = "testIndex#";
            final int NUM_INDICES = 20;

            for (int i = 0; i < NUM_INDICES; i++) {

               // register an index
               final String name = PREFIX + i;

               src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
               {

                  // lookup the index.
                  final BTree ndx = src.getIndex(name);

                  // #of tuples to write.
                  final int ntuples = r.nextInt(10000);

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

            {

               // create and submit task.
               final Future<Map<String, BaseIndexStats>> f = src
                     .warmUp(null/* namespaces */);

               // await future.
               final Map<String, BaseIndexStats> map = f.get();
               
               assertEquals(NUM_INDICES, map.size());

               // Exercise the logic to write out the statistics table.
               {
                  final PrintWriter w = new PrintWriter(System.out);
                  BaseIndexStats.writeOn(w, map);
                  w.flush();
               }

            }

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

   }

    /**
     * Test with a journal on which many indices have been registered and
     * populated with random data.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_manyIndicesRandomData_concurrentWriter() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            final String PREFIX = "testIndex#";
            final int NUM_INDICES = 20;

            for (int i = 0; i < NUM_INDICES; i++) {

               // register an index
               final String name = PREFIX + i;

               src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
               {

                  // lookup the index.
                  final BTree ndx = src.getIndex(name);

                  // #of tuples to write.
                  final int ntuples = r.nextInt(10000);

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

            /*
             * Submit task for concurrent writes. Note that this task does not
             * do a commit, but it does write modifications onto the live
             * indices. Those changes should not be visible in the snapshot.
             * Since the task does not do a commit, the snapshot should have
             * exactly the same data that was in the journal as of the previous
             * commit point. That commit point is restored (below) when we
             * discard the write set.
             */
            final Future<Void> f2 = src.getExecutorService().submit(new Callable<Void>() {

               @Override
               public Void call() throws Exception {

                  for (int i = 0; i < NUM_INDICES; i++) {

                     final String name = PREFIX + i;

                     // lookup the index.
                     final BTree ndx = src.getIndex(name);

                     // #of tuples to write.
                     final int ntuples = r.nextInt(10000);

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
                  // Done.
                  return null;
               }});

            try {

               // Run warmup while writer is running.
               {

                  // create and submit task.
                  final Future<Map<String, BaseIndexStats>> f = src
                        .warmUp(null/* namespaces */);

                  // await future.
                  final Map<String, BaseIndexStats> map = f.get();

                  assertEquals(NUM_INDICES, map.size());

                  // Exercise the logic to write out the statistics table.
                  {
                     final PrintWriter w = new PrintWriter(System.out);
                     BaseIndexStats.writeOn(w, map);
                     w.flush();
                  }

               }

               // Await the success of the writer.
               f2.get();

               // Discard the write set.
               src.abort();

            } finally {
               f2.cancel(true/* mayInterruptIfRunning */);
            }

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

   }

}
