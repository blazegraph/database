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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;
import com.bigdata.util.InnerCause;

/**
 * Test suite for
 * {@link Journal#snapshot(com.bigdata.journal.Journal.ISnapshotFactory)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1172"> Online backup for Journal
 *      </a>
 */
public class TestSnapshotJournal extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestSnapshotJournal() {
    }

    /**
     * @param name
     */
    public TestSnapshotJournal(String name) {
        super(name);
    }

    private static class MySnapshotFactory implements ISnapshotFactory {

      private final String testName;
      private final boolean compressed;

      public MySnapshotFactory(final String testName, final boolean compressed) {
         this.testName = testName;
         this.compressed = compressed;
      }
       
      @Override
      public File getSnapshotFile(final IRootBlockView rbv) throws IOException {
         return File.createTempFile(
               testName + "-snapshot-" + rbv.getCommitCounter(),
               getCompress() ? "jnl.gz" : ".jnl");
      }

      @Override
      public boolean getCompress() {
         return compressed;
      }
       
    };

    /**
     * Open a journal snapshot.
     * <p>
     * Note: If the snapshot was compressed, then it was decompressed in order to
     * open it. In this case the caller is responsible for deleting the backing
     * file. Typically just call {@link Journal#destroy()}.
     * 
     * @param snapshotResult
     * 
     * @return The open snapshot.
     * @throws IOException
     */
    private Journal openSnapshot(final ISnapshotResult snapshotResult)
          throws IOException {

       final File snapshotFile = snapshotResult.getFile();

       final boolean compressed = snapshotResult.getCompressed();

       final File journalFile;

       if (compressed) {

          /*
           * Decompress the snapshot.
           */

          // source is the snapshot.
          final File in = snapshotFile;

          // decompress onto a temporary file.
          final File out = File.createTempFile(snapshotFile.getName()
                + "-decompressed", Journal.Options.JNL);

          if (log.isInfoEnabled())
             log.info("Decompressing " + in + " to " + out);

          // Decompress the snapshot.
          SnapshotTask.decompress(in, out);

          journalFile = out;

       } else {

          journalFile = snapshotFile;

       }

       // Open the journal.
       final Properties properties = getProperties();

       properties.setProperty(Journal.Options.FILE, journalFile.toString());

       properties.setProperty(Journal.Options.CREATE_TEMP_FILE, "false");

       final Journal tmp = new Journal(properties);

       return tmp;
        
    }

    /**
     * Verifies exception if there are no commits on the journal (the
     * lastCommitTime will be zero which does not identify a valid commit
     * point).
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

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

            final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                  getName(), false/* compressed */);

            // create and submit task.
            final Future<ISnapshotResult> f = src.snapshot(snapshotFactory);

            try {

               f.get();

               fail("Expecting nested " + IllegalStateException.class.getName());

            } catch (Exception ex) {

               // snapshot of empty journal is not supported.
               if (!InnerCause.isInnerCause(ex, IllegalStateException.class)) {

                  fail("Expecting nested "
                        + IllegalStateException.class.getName(), ex);

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
     * Verifies exception if there are no commits on the journal (the
     * lastCommitTime will be zero which does not identify a valid commit
     * point).
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
   public void test_nonEmptyJournal() throws IOException, InterruptedException,
         ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {
            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

            src.write(getRandomData(128));
            src.commit();
            try {
               final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                     getName(), false/* compressed */);
               // create and submit snapshot task.
               final Future<ISnapshotResult> f = src
                     .snapshot(snapshotFactory);
               // obtain snapshot result.
               final ISnapshotResult snapshotResult = f.get();
               final File snapshotFile = snapshotResult.getFile();
               try {
                  final Journal tmp = openSnapshot(snapshotResult);
                  // Verify same root block as source journal.
                  assertEquals(src.getRootBlockView(), tmp.getRootBlockView());
                  // destroy new journal if succeeded (clean up).
                  tmp.destroy();
               } finally {
                  if (snapshotFile.exists()) {
                     snapshotFile.delete();
                  }
               }
            } catch (IllegalArgumentException ex) {
               // log expected exception.
               log.info("Ignoring expected exception: " + ex);
            }

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
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_oneIndexNoData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            src.commit();

            final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                  getName(), false/* compressed */);

            final ISnapshotResult snapshotResult = src.snapshot(snapshotFactory).get();

            final Journal newJournal = openSnapshot(snapshotResult);

            // verify state
            try {

               // verify index exists.
               assertNotNull(newJournal.getIndex(NAME));

               // verify data is the same.
               AbstractBTreeTestCase.assertSameBTree(src.getIndex(NAME),
                     newJournal.getIndex(NAME));

            } finally {

               newJournal.destroy();

            }

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
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_oneIndexRandomData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            {
               BTree ndx = src.getIndex(NAME);
               KV[] a = AbstractBTreeTestCase
                     .getRandomKeyValues(1000/* ntuples */);
               for (KV kv : a) {
                  ndx.insert(kv.key, kv.val);
               }
            }
            src.commit();

            final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                  getName(), false/* compressed */);

            final ISnapshotResult snapshotResult = src.snapshot(snapshotFactory).get();

            final Journal newJournal = openSnapshot(snapshotResult);

            // verify state
            try {

               // verify index exists.
               assertNotNull(newJournal.getIndex(NAME));

               // verify data is the same.
               AbstractBTreeTestCase.assertSameBTree(src.getIndex(NAME),
                     newJournal.getIndex(NAME));

            } finally {
               newJournal.destroy();
            }

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
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_oneIndexRandomData_compressedSnapshot() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            {
               BTree ndx = src.getIndex(NAME);
               KV[] a = AbstractBTreeTestCase
                     .getRandomKeyValues(1000/* ntuples */);
               for (KV kv : a) {
                  ndx.insert(kv.key, kv.val);
               }
            }
            src.commit();

            final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                  getName(), true/* compressed */);

            final ISnapshotResult snapshotResult = src.snapshot(snapshotFactory).get();

            final Journal newJournal = openSnapshot(snapshotResult);

            // verify state
            try {

               // verify index exists.
               assertNotNull(newJournal.getIndex(NAME));

               // verify data is the same.
               AbstractBTreeTestCase.assertSameBTree(src.getIndex(NAME),
                     newJournal.getIndex(NAME));

            } finally {
               newJournal.destroy();
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
    public void test_journal_manyIndicesRandomData() throws IOException,
            InterruptedException, ExecutionException {

      final File out = File.createTempFile(getName(), Options.JNL);

      try {

         final Journal src = getStore(getProperties());

         try {

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

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

               final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                     getName(), false/* compressed */);

               final ISnapshotResult snapshotResult = src.snapshot(
                     snapshotFactory).get();

               final Journal newJournal = openSnapshot(snapshotResult);

               // verify state
               try {

                  for (int i = 0; i < NUM_INDICES; i++) {

                     final String name = PREFIX + i;

                     // verify index exists.
                     assertNotNull(newJournal.getIndex(name));

                     // verify data is the same.
                     AbstractBTreeTestCase.assertSameBTree(src.getIndex(name),
                           newJournal.getIndex(name));

                     // and verify the counter was correctly propagated.
                     assertEquals(src.getIndex(name).getCounter().get(),
                           newJournal.getIndex(name).getCounter().get());

                  }

               } finally {

                  newJournal.destroy();

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

            if (!(src.getBufferStrategy() instanceof IHABufferStrategy)) {
               // Feature is not supported.
               return;
            }

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
            final Future<Void> f = src.getExecutorService().submit(new Callable<Void>() {

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

            // Take a snapshot while the writer is running.
            final ISnapshotResult snapshotResult;
            try {

               final ISnapshotFactory snapshotFactory = new MySnapshotFactory(
                     getName(), false/* compressed */);

               snapshotResult = src.snapshot(snapshotFactory).get();

               // Await the success of the writer.
               f.get();

               // Discard the write set.
               src.abort();

            } finally {
               
               f.cancel(true/* mayInterruptIfRunning */);
               
            }
            
            // Verify the snapshot.
            {
             
               final Journal newJournal = openSnapshot(snapshotResult);

               // verify state
               try {

                  for (int i = 0; i < NUM_INDICES; i++) {

                     final String name = PREFIX + i;

                     // verify index exists.
                     assertNotNull(newJournal.getIndex(name));

                     // verify data is the same.
                     AbstractBTreeTestCase.assertSameBTree(src.getIndex(name),
                           newJournal.getIndex(name));

                     // and verify the counter was correctly propagated.
                     assertEquals(src.getIndex(name).getCounter().get(),
                           newJournal.getIndex(name).getCounter().get());

                  }

               } finally {

                  newJournal.destroy();

               }

            }

         } finally {

            src.destroy();

         }

      } finally {

         out.delete();

      }

   }

}
