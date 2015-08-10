/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.bd;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.frontier.StaticFrontier2;
import com.bigdata.rdf.graph.impl.scheduler.STScheduler;
import com.bigdata.rdf.graph.impl.util.GASImplUtil;
import com.bigdata.rdf.graph.impl.util.IArraySlice;
import com.bigdata.rdf.graph.impl.util.ManagedArray;
import com.bigdata.rdf.graph.util.GASUtil;

/**
 * This scheduler uses thread-local buffers ({@link LinkedHashSet}) to track the
 * distinct vertices scheduled by each execution thread. After the computation
 * round, those per-thread segments of the frontier are combined into a single
 * global, compact, and ordered frontier. To maximize the parallel activity, the
 * per-thread frontiers are sorted using N threads (one per segment). Finally,
 * the frontier segments are combined using a {@link MergeSortIterator} - this
 * is a sequential step with a linear cost in the size of the frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TLScheduler2 implements IGASSchedulerImpl {

    private static final Logger log = Logger.getLogger(TLScheduler2.class);
    
    /**
     * Class bundles a reusable, extensible array for sorting the thread-local
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class MySTScheduler extends STScheduler {

        /**
         * This is used to sort the thread-local frontier (that is, the frontier
         * for a single thread). The backing array will grow as necessary and is
         * reused in each round.
         * <P>
         * Note: The schedule (for each thread) is using a set - see the
         * {@link STScheduler} base class. This means that the schedule (for
         * each thread) is compact, but not ordered. We need to use (and re-use)
         * an array to order that compact per-thread schedule. The compact
         * per-thread schedules are then combined into a single compact frontier
         * for the new round.
         */
        private final ManagedArray<Value> tmp;

        public MySTScheduler(final GASEngine gasEngine) {

            super(gasEngine);

            tmp = new ManagedArray<Value>(Value.class, 64);

        }
        
    } // class MySTScheduler
    
    private final GASEngine gasEngine;
    private final int nthreads;
    private final ConcurrentHashMap<Long/* threadId */, MySTScheduler> map;

    public TLScheduler2(final GASEngine gasEngine) {

        this.gasEngine = gasEngine;

        this.nthreads = gasEngine.getNThreads();

        this.map = new ConcurrentHashMap<Long, MySTScheduler>(
                nthreads/* initialCapacity */, .75f/* loadFactor */, nthreads);

    }

    private IGASScheduler threadLocalScheduler() {

        final Long id = Thread.currentThread().getId();

        MySTScheduler s = map.get(id);

        if (s == null) {

            final IGASScheduler old = map.putIfAbsent(id, s = new MySTScheduler(
                    gasEngine));

            if (old != null) {

                /*
                 * We should not have a key collision since this is based on the
                 * threadId.
                 */

                throw new AssertionError();

            }

        }

        return s;

    }

    @Override
    public void schedule(final Value v) {

        threadLocalScheduler().schedule(v);

    }

    @Override
    public void clear() {

        /*
         * Clear the per-thread maps, but do not discard. They will be reused in
         * the next round.
         * 
         * Note: This is a big cost. Simply clearing [map] results in much less
         * time and less GC.
         */
//        for (STScheduler s : map.values()) {
//
//            s.clear();
//
//        }
        map.clear();
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        /*
         * Figure out the #of sources and the #of vertices across those sources.
         * 
         * This also computes the cumulative offsets into the new frontier for
         * the different per-thread segments.
         */
        final int[] off = new int[nthreads]; // zero for 1st thread.
        final int nsources;
        final int nvertices;
        {
            int ns = 0, nv = 0;
            for (MySTScheduler s : map.values()) {
                final MySTScheduler t = s;
                final int sz = t.size();
                off[ns] = nv; // starting index.
                ns++;
                nv += sz;
            }
            nsources = ns;
            nvertices = nv;
        }

        if (nsources > nthreads) {

            /*
             * nsources could be LT nthreads if we have a very small frontier,
             * but it should never be GTE nthreads.
             */

            throw new AssertionError("nsources=" + nsources + ", nthreads="
                    + nthreads);

        }
       
        if (nvertices == 0) {

            /*
             * The new frontier is empty.
             */

            frontier.resetFrontier(0/* minCapacity */, false/* sortFrontier */,
                    GASUtil.EMPTY_VERTICES_ITERATOR);

            return;

        }

        /*
         * Parallel copy of the per-thread frontiers onto the new frontier.
         * 
         * Note: This DOES NOT produce a compact frontier! The code that maps
         * the gather/reduce operations over the frontier will eliminate
         * duplicate work.
         */
        
        // TODO Requires a specific class to work! API!
        final StaticFrontier2 f2 = (StaticFrontier2) frontier;
        {
            
            // ensure sufficient capacity! 
            f2.resetAndEnsureCapacity(nvertices);
            f2.setCompact(false); // NOT COMPACT!
            
            final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                    nsources);

            int i = 0;
            for (MySTScheduler s : map.values()) { // TODO Paranoia suggests to put these into an [] so we know that we have the same traversal order as above. That might not be guaranteed.
                final MySTScheduler t = s;
                final int index = i++;
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        final IArraySlice<Value> orderedSegment = GASImplUtil
                                .compactAndSort(t.getVertices(), t.tmp);
                        f2.copyIntoResetFrontier(off[index], orderedSegment);
                        return (Void) null;
                    }
                });
            }
            
            // invokeAll() - futures will be done() before it returns.
            final List<Future<Void>> futures;
            try {
                futures = gasEngine.getGASThreadPool().invokeAll(tasks);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            for (Future<Void> f : futures) {

                try {
                    f.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        if (log.isInfoEnabled())
            log.info("Done: " + this.getClass().getCanonicalName()
                    + ",frontier=" + frontier);
//        /*
//         * Now merge sort those arrays and populate the new frontier.
//         */
//        mergeSortSourcesAndSetFrontier(nsources, nvertices, frontiers, frontier);

    }

//    /**
//     * Now merge sort the ordered frontier segments and populate the new
//     * frontier.
//     * 
//     * @param nsources
//     *            The #of frontier segments.
//     * @param nvertices
//     *            The total #of vertice across those segments (may double-count
//     *            across segments).
//     * @param frontiers
//     *            The ordered, compact frontier segments
//     * @param frontier
//     *            The new frontier to be populated.
//     */
//    private void mergeSortSourcesAndSetFrontier(final int nsources,
//            final int nvertices, final IArraySlice<Value>[] frontiers,
//            final IStaticFrontier frontier) {
//
//        // wrap Values[] as Iterators.
//        @SuppressWarnings("unchecked")
//        final Iterator<Value>[] itrs = new Iterator[nsources];
//
//        for (int i = 0; i < nsources; i++) {
//
//            itrs[i] = frontiers[i].iterator();
//
//        }
//
//        // merge sort of those iterators.
//        final Iterator<Value> itr = new MergeSortIterator(itrs);
//
//        frontier.resetFrontier(nvertices/* minCapacity */, true/* ordered */,
//                itr);
//
//    }

}