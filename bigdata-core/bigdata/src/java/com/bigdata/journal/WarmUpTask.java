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
 * Created on Apr 14, 2015
 */
package com.bigdata.journal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.util.Bytes;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Helper class to warm up the indices associated with various namespaces on the
 * journal.
 * 
 * @author bryan
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal on
 *      startup </a>
 * 
 *      TODO The current implementation assigns one thread per index. However,
 *      it is also possible to use a reader pool to accelerate the index scans.
 *      This would require a change to the
 *      {@link com.bigdata.btree.AbstractBTree#dumpPages(boolean, boolean)}
 *      implementation to parallelize the IOs over the children of a node. We
 *      have experimented with that in the past but not observed a big win for
 *      query. It might be more significant for warmup since we are using a full
 *      scan of each index.
 */
public class WarmUpTask implements Callable<Map<String,BaseIndexStats>> {

   private static final Logger log = Logger.getLogger(WarmUpTask.class);
   
   /**
    * The journal.
    */
   private final Journal journal;

   /**
    * A list of zero or more namespaces to be warmed up (optional). When
    * <code>null</code> or empty, all namespaces will be warmed up.
    */
   private final List<String> namespaces;
   
   /**
    * The commit time to be warmed up and -or- {@link ITx#READ_COMMITTED} to
    * warm up the last commit point on the journal.
    */
   private final long timestamp;

   /**
    * The #of threads that will be used to scan the pages in the indices
    * associated with those namespaces (GTE ONE). The #of threads that will be
    * used to scan the pages in the indices associated with those namespaces
    * (GTE ONE). The indices will be scanned with one thread per index. This
    * parameter determines the #of such scans that will execute in parallel.
    * Since the thread will block on any IO, you need a modestly large number of
    * threads here to enqueue enough disk reads to drive enough IOPs for an
    * efficient disk scan. Even a slow disk should use at least 10 threads.
    */
   private final int nparallel;
   
   /**
    * when <code>true</code> the leaves of the indices will also be read.
    */
   private final boolean visitLeaves;

   /**
    * Used to collect statistics obtained as a side-effect of the warmup
    * procedure.
    * 
    * Note: This collection is ordered and is NOT thread-safe.
    */
   private final Map<String, BaseIndexStats> statsMap = new TreeMap<String, BaseIndexStats>();

   /**
    * 
    * @param journal
    *           The journal.
    * @param namespaces
    *           A list of zero or more namespaces to be warmed up (optional).
    *           When <code>null</code> or empty, all namespaces will be warmed
    *           up.
    * @param timestamp
    *           The commit time to be warmed up and -or-
    *           {@link ITx#READ_COMMITTED} to warm up the last commit point on
    *           the journal.
    * @param nparallel
    *           The #of threads that will be used to scan the pages in the
    *           indices associated with those namespaces (GTE ONE). The indices
    *           will be scanned with one thread per index. This parameter
    *           determines the #of such scans that will execute in parallel.
    *           Since the thread will block on any IO, you need a modestly large
    *           number of threads here to enqueue enough disk reads to drive
    *           enough IOPs for an efficient disk scan. Even a slow disk should
    *           use at least 10 threads.
    * @param visitLeaves
    *           when <code>true</code> the leaves of the indices will also be
    *           read (the recommended warm up procedure does not read the
    *           leaves).
    */
   public WarmUpTask(//
         final Journal journal,//
         final List<String> namespaces, //
         final long timestamp,//
         final int nparallel,//
         final boolean visitLeaves//
   ) {
      if (journal == null)
         throw new IllegalArgumentException();
      if (nparallel < 1)
         throw new IllegalArgumentException();
      this.journal = journal;
      this.namespaces = namespaces;
      this.timestamp = timestamp;
      this.nparallel = nparallel;
      this.visitLeaves = visitLeaves;
   }

   @Override
   public Map<String, BaseIndexStats> call() throws Exception {

      final long begin = System.nanoTime();
      
      // Attempts to pin a view of the journal as of that timestamp.
      final long tx = journal.newTx(timestamp);

      try {

         final long readOnCommitTime = journal.getLocalTransactionManager()
               .getTx(tx).getReadsOnCommitTime();

         final ICommitRecord commitRecord = journal
               .getCommitRecord(readOnCommitTime);

         // Populate the tasks.
         final List<FutureTask<BaseIndexStats>> tasks = new LinkedList<FutureTask<BaseIndexStats>>();
         {
            // Scan the named indices.
            final Iterator<String> nitr = journal.indexNameScan(
                  null/* prefix */, timestamp);

            while (nitr.hasNext()) {

               // a registered index.
               final String name = nitr.next();

               if (namespaces != null && !namespaces.isEmpty()) {

                  boolean found = false;

                  for (String namespace : namespaces) {

                     if (name.startsWith(namespace)) {

                        found = true;

                        break;

                     }

                  }

                  if (!found) {

                     // Skip this index. Not a desired namespace.
                     continue;

                  }

               }

               if (log.isInfoEnabled())
                  log.info("Will warm up index: name=" + name);
               
               tasks.add(new FutureTask<BaseIndexStats>(
                     new Callable<BaseIndexStats>() {
                        @Override
                        public BaseIndexStats call() throws Exception {
                           return warmUpIndex(name, commitRecord);
                        }
                     }));

            } // while(itr) (next index)

         }

         // Execute the tasks with limited parallelism.
         try {

            final LatchedExecutor executor = new LatchedExecutor(
                  journal.getExecutorService(), nparallel);

            // Enqueue tasks to run with limited parallelism.
            for (FutureTask<BaseIndexStats> ft : tasks) {
               executor.execute(ft);
            }

            // Get the futures, throwing out any errors.
            for (FutureTask<BaseIndexStats> ft : tasks) {
               // The statistics from scanning a single index.
               final BaseIndexStats stats = ft.get();
               /*
                * Add those statistics to our collection.
                * 
                * Note: collection is not thread safe, but this logic is single
                * threaded.
                */
               statsMap.put(stats.name, stats);
            }

         } finally {
            
            // Ensure tasks are cancelled.
            for (FutureTask<BaseIndexStats> ft : tasks) {
               ft.cancel(true/* mayInterruptIfRunning */);
            }

         }

         final long elapsed = System.nanoTime() - begin;
         
         if (log.isInfoEnabled()) {
            /*
             * Write out warmup statistics (summary and detail).
             */
            final StringWriter strw = new StringWriter(statsMap.size()
                  * Bytes.kilobyte32);
            strw.append("Warmed up: " + statsMap.size() + " indices in "
                  + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms using up to "
                  + nparallel + " threads.\n");
            final PrintWriter w = new PrintWriter(strw);
            BaseIndexStats.writeOn(w, statsMap);
            w.flush();
            log.info(strw.toString());
         }

         return Collections.unmodifiableMap(statsMap);

      } finally {

         journal.abort(tx);

      }

   }// call()

   /**
    * Warm up a single index.
    * 
    * @param name
    *           The name of the index.
    * @param commitRecord
    *           The commit record from which the index will be loaded.
    *           
    * @return The statistics obtained as a side-effect of warming up the index.
    */
   private BaseIndexStats warmUpIndex(final String name,
         final ICommitRecord commitRecord) {

      // load index from its checkpoint record.
      final ICheckpointProtocol ndx;
      try {

         ndx = journal.getIndexWithCommitRecord(name, commitRecord);

      } catch (Throwable t) {

         if (InnerCause.isInnerCause(t, ClassNotFoundException.class)) {

            /*
             * This is typically a tuple serializer that has a dependency on an
             * application class that is not present in the CLASSPATH. Add the
             * necessary dependency(s) and you should no longer see this
             * message.
             */

            log.warn("Could not load index: "
                  + InnerCause.getInnerCause(t, ClassNotFoundException.class));

            return null;

         } else
            throw new RuntimeException(t);

      }

      // show checkpoint record.
      if (log.isDebugEnabled())
         log.debug(ndx.getCheckpoint());

      /*
       * Collect statistics on the page usage for the index.
       */
      {

         final BaseIndexStats stats = ndx.dumpPages(true/* recursive */,
               visitLeaves);

         if (log.isInfoEnabled())
            log.info("name=" + name + ", stats=" + stats);

         return stats;

      }

   }

} // class WarmUpTask
