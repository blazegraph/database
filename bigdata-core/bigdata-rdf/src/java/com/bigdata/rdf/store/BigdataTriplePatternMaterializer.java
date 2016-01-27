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
package com.bigdata.rdf.store;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.system.SystemUtil;

import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.concurrent.LatchedExecutor;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Efficient batched, streaming resolution of triple patterns to statements
 * spanned by those triple patterns that are present in the data.
 * <p>
 * Note: If the input contains triple patterns that have a high cardinality
 * in the data, then a large number of statements may be returned.
 * 
 * @param triplePatterns
 *            A collection of triple patterns or fully bound statements. If
 *            this collection contains triple patterns that have a high
 *            cardinality in the data, then a large number of statements may
 *            be returned.
 *            
 * @return An iterator from which the materialized statements spanned by
 *         those triple patterns may be read.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/866" > Efficient batch
 *      remove of a collection of triple patterns </a>
 */
public class BigdataTriplePatternMaterializer
        extends
        AbstractChunkedResolverator<BigdataTriplePattern, ISPO, AbstractTripleStore>
        implements ICloseableIterator<ISPO> {
//        implements IChunkedOrderedIterator<ISPO> {

    private final int nthreads;
    
    public BigdataTriplePatternMaterializer(final AbstractTripleStore db,
            final IChunkedOrderedIterator<BigdataTriplePattern> src) {
        this(db, src, 4/* nthreads */);
    }

    public BigdataTriplePatternMaterializer(final AbstractTripleStore db,
            final IChunkedOrderedIterator<BigdataTriplePattern> src,
            final int nthreads) {

        super(db, src, new BlockingBuffer<ISPO[]>(
                db.getChunkOfChunksCapacity(), 
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));
        
        if (nthreads < 0)
            throw new IllegalArgumentException();

        // At least 1 thread. At most ncpus*2.
        this.nthreads = Math.max(
                Math.min(nthreads, SystemUtil.numProcessors() * 2), 1);

    }

    @Override
    public BigdataTriplePatternMaterializer start(final ExecutorService service) {

        helperService.set(new LatchedExecutor(service, nthreads));

        super.start(service);

        return this;

    }
    private final AtomicReference<LatchedExecutor> helperService = new AtomicReference<LatchedExecutor>();

    @Override
    protected ISPO[] resolveChunk(final BigdataTriplePattern[] chunk) {

        final LatchedExecutor helperService = this.helperService.get();

        if (helperService == null)
            throw new IllegalStateException();

        /**
         * The output will be at most sizeof(chunk) arrays. Each array will have
         * one or more statements. Any triple patterns that have no intersection
         * in the data will be dropped and will not put anything into this
         * output queue.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/985" > Deadlock in
         *      BigdataTriplePatternMaterializer </a>
         */
        final Queue<ISPO[]> out = new ConcurrentLinkedQueue<ISPO[]>(
                /*chunk.length*/);

        final List<FutureTask<Long>> tasks = new LinkedList<FutureTask<Long>>();

        try {

            final CountDownLatch latch = new CountDownLatch(chunk.length);

            /*
             * Create FutureTasks for each subquery. The futures are not
             * submitted to the Executor yet. That happens in call(). By
             * deferring the evaluation until call() we gain the ability to
             * cancel all subqueries if any subquery fails.
             */
            for (BigdataTriplePattern stmt : chunk) {

                /*
                 * Task runs subquery and cancels all subqueries in [tasks] if
                 * it fails.
                 */
                final FutureTask<Long> ft = new FutureTask<Long>(
                        new ResolveTriplePatternTask(stmt, out)) {
                    /*
                     * Hook future to count down the latch when the task is
                     * done.
                     */
                    @Override
                    public void run() {
                        try {
                            super.run();
                        } finally {
                            latch.countDown();
                        }
                    }
                };

                tasks.add(ft);

            }

            /*
             * Run triple pattern resolution with limited parallelism.
             */
            for (FutureTask<Long> ft : tasks) {
                helperService.execute(ft);
            }

            /*
             * Wait for all tasks to complete.
             */
            latch.await();

            /*
             * Check futures, counting the #of solutions.
             */
            long nfound = 0L;
            for (FutureTask<Long> ft : tasks) {
                nfound += ft.get();
                if (nfound > Integer.MAX_VALUE)
                    throw new UnsupportedOperationException();
            }

            /*
             * Convert into a single ISPO[] chunk.
             */
            final ISPO[] dest = new ISPO[(int) nfound];
            int destPos = 0;
            ISPO[] src = null;
            while ((src = out.poll()) != null) {
                System.arraycopy(src/* src */, 0/* srcPos */, dest, destPos,
                        src.length);
                destPos += src.length;
            }
            
            return dest;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {

            // Cancel any tasks which are still running.
            for (FutureTask<Long> ft : tasks)
                ft.cancel(true/* mayInterruptIfRunning */);

        }

    }

    /**
     * Resolve a triple pattern to the statements that it spans in the data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class ResolveTriplePatternTask implements Callable<Long> {

        private final BigdataTriplePattern stmt;
        private final Queue<ISPO[]> out;

        public ResolveTriplePatternTask(final BigdataTriplePattern stmt,
                final Queue<ISPO[]> out) {
            this.stmt = stmt;
            this.out = out;
        }

        @Override
        public Long call() throws Exception {
            /*
             * TODO What about closure over the SIDs?
             * 
             * final IChunkedOrderedIterator<ISPO> itr =
             * database.computeClosureForStatementIdentifiers(
             * database.getAccessPath(s, p, o, c).iterator());
             */
            final IAccessPath<ISPO> ap = (IAccessPath<ISPO>) state.getAccessPath(
            		stmt.getSubject(), stmt.getPredicate(), 
            		stmt.getObject(), stmt.getContext());
            
//            if(ap.isFullyBoundForKey()) {
//                /*
//                 * Optimize when triple pattern is a fully bound statement.
//                 * In this case, the output is either that statement (with IVs
//                 * resolved) or the triple pattern is dropped.
//                 */
//                final IChunkedOrderedIterator<ISPO> itr = ap.iterator();
//                try {
//                    if (!itr.hasNext())
//                        return 0L;
//                    final ISPO spo = itr.next();
//                    out.add(new ISPO[]{spo});
//                    return 1L;
//                } finally {
//                    itr.close();
//                }
//            } else {
            long n = 0L;
            final IChunkedOrderedIterator<ISPO> itr = ap.iterator();
            try {
                while (itr.hasNext()) {
                    final ISPO[] a = itr.nextChunk();
//                    if (true) {
//                        // verify no null array elements.
//                        for (int i = 0; i < a.length; i++) {
//                            if (a[i] == null)
//                                throw new AssertionError(Arrays.toString(a));
//                        }
//                    }
                    /**
                     * This will never fail for a ConcurrentLinkedQueue.
                     * 
                     * @see <a href="http://trac.blazegraph.com/ticket/985" >
                     *      Deadlock in BigdataTriplePatternMaterializer </a>
                     */
                    final boolean result = out.offer(a);
                    assert result : "insertion failed - expects an unbounded queue";
                    n += a.length;
                }
                return n;
            } finally {
                itr.close();
            }
//           }
            
        }

    }

}
