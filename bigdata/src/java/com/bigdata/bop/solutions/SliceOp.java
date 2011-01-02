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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.ChunkedRunningQuery;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.service.IBigdataFederation;

/**
 * An operator which imposes an offset/limit on a binding set pipeline.
 * <p>
 * Note: join processing typically involves concurrent processes, hence the
 * order of the results will not be stable unless the results are sorted before
 * applying the slice. When a slice is applied without a sort, the same query
 * may return different results each time it is evaluated.
 * <p>
 * Note: When running on an {@link IBigdataFederation}, this operator must be
 * imposed on the query controller so it can count the solutions as they flow
 * through - see {@link #getEvaluationContext()}.
 * <p>
 * Note: {@link SliceOp} is safe for concurrent invocations for the same query.
 * Multiple chunks may flow through multiple invocations of the operator so long
 * as they use the same {@link BOpStats} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Slice is not safe for subqueries - the entire query is cancelled when
 *       the slice is satisfied.
 *       <p>
 *       If we allow complex operator trees in which "subqueries" can also use a
 *       slice then either they need to run as their own query with their own
 *       {@link ChunkedRunningQuery} state or the API for cancelling a running query as
 *       used here needs to only cancel evaluation of the child operators.
 *       Otherwise we could cancel all operator evaluation for the query,
 *       including operators which are ancestors of the {@link SliceOp}.
 */
public class SliceOp extends PipelineOp {

    private final static transient Logger log = Logger.getLogger(SliceOp.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The first solution to be returned to the caller (origin ZERO).
         */
        String OFFSET = SliceOp.class.getName() + ".offset";

        long DEFAULT_OFFSET = 0L;

        /**
         * The maximum #of solutions to be returned to the caller (default is
         * all).
         */
        String LIMIT = SliceOp.class.getName() + ".limit";

        /**
         * A value of {@link Long#MAX_VALUE} is used to indicate that there is
         * no limit.
         */
        long DEFAULT_LIMIT = Long.MAX_VALUE;

    }
    
    /**
     * Deep Copy constructor.
     * @param op
     */
    public SliceOp(final SliceOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SliceOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

//        if (args.length != 1)
//            throw new IllegalArgumentException();
//
//        if (!(args[0] instanceof BindingSetPipelineOp))
//            throw new IllegalArgumentException();

        switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

    }

    /**
     * @see Annotations#OFFSET
     */
    public long getOffset() {

        return getProperty(Annotations.OFFSET, Annotations.DEFAULT_OFFSET);

    }

    /**
     * @see Annotations#LIMIT
     */
    public long getLimit() {

        return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);
        
    }

    /**
     * Overridden to return <code>true</code> since the correct decision
     * semantics for the slice depend on concurrent invocations for the same
     * query having the same {@link SliceStats} object.
     * <p>
     * {@inheritDoc}
     */
    @Override
    final public boolean isSharedState() {
        
        return true;
        
    }
    
    /**
     * Extends {@link BOpStats} to capture the state of the {@link SliceOp}.
     */
    public static class SliceStats extends BOpStats {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /** #of solutions visited. */
        public final AtomicLong nseen = new AtomicLong();

        /** #of solutions accepted. */
        public final AtomicLong naccepted = new AtomicLong();

        @Override
        public void add(final BOpStats o) {

            if (this == o) {
                // Do not add to self!
                return;
            }
            
            super.add(o);
            
            if (o instanceof SliceStats) {
            
                final SliceStats t = (SliceStats) o;
                
                nseen.addAndGet(t.nseen.get());
                
                naccepted.addAndGet(t.naccepted.get());
                
            }
            
        }

        @Override
        protected void toString(final StringBuilder sb) {

            sb.append(",nseen=" + nseen);

            sb.append(",naccepted=" + naccepted);

        }
        
    }
    
    public SliceStats newStats() {
        
        return new SliceStats();
        
    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new SliceTask(this, context));
        
    }

    /**
     * Copy the source to the sink or the alternative sink depending on the
     * condition.
     */
    static private class SliceTask implements Callable<Void> {

        private final SliceOp op;
        
        private final BOpContext<IBindingSet> context;
        
        /** #of solutions to skip before accepting the first solution. */
        private final long offset;

        /** #of solutions to accept. */
        private final long limit;

//        private final long last;

        private final SliceStats stats;
        
        SliceTask(final SliceOp op, final BOpContext<IBindingSet> context) {

            this.op = op;
            
            this.context = context;
            
            this.offset = op.getOffset();

            this.limit = op.getLimit();

            if (offset < 0)
                throw new IllegalArgumentException(Annotations.OFFSET);

            if (limit <= 0)
                throw new IllegalArgumentException(Annotations.LIMIT);

            this.stats = (SliceStats) context.getStats();
            
//            this.last = offset + limit;
//            this.last = BigInteger.valueOf(offset).add(
//                    BigInteger.valueOf(limit)).min(
//                    BigInteger.valueOf(Long.MAX_VALUE)).longValue();

        }

        public Void call() throws Exception {

            final IAsynchronousIterator<IBindingSet[]> source = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            /*
             * buffer forms chunks which get flushed onto the sink.
             * 
             * @todo if we have visibility into the #of source chunks, then do
             * not buffer more than min(#source,#needed).
             */
            final UnsynchronizedArrayBuffer<IBindingSet> out = new UnsynchronizedArrayBuffer<IBindingSet>(
                    sink, op.getChunkCapacity());

            while (source.hasNext()) {

                final IBindingSet[] chunk = source.next();

                /*
                 * Batch each chunk through a lock for better concurrency
                 * (avoids CAS contention).
                 * 
                 * Note: This is safe because the source chunk is already
                 * materialized and the sink will not block (that is part of the
                 * bop evaluation contract).
                 * 
                 * Note: We need to be careful here with concurrent close of the
                 * sink (which is the shared queryBuffer) by concurrent
                 * SliceOps. The problem is that the slice can count off the
                 * solutions without having them flushed all the way through to
                 * the queryBuffer, but we can not close the query buffer until
                 * we actually see the last solution added to the query buffer.
                 * This is why the slice flushes the buffer while it is
                 * synchronized.
                 */
                synchronized (stats) {

                    if (log.isTraceEnabled())
                        log.trace(toString() + ": stats=" + stats + ", sink="
                                + sink);

                    final boolean halt = handleChunk(out, chunk);

                    if (!out.isEmpty())
                        out.flush();

                    sink.flush();

                    if (halt) {

                        if (log.isInfoEnabled())
                            log.info("Slice will interrupt query.");

                        context.getRunningQuery().halt();

                    }

                }

            }

            return null;

        }

        /**
         * <p>
         * Apply the slice semantics to a chunk of binding sets.
         * </p>
         * <h2>example</h2>
         * <p>
         * offset=2, limit=3, last=3+2=5. The number line represents the
         * observed binding sets. The first binding set is at index ZERO (0).
         * The initial conditions are: nseen(S)=0 and naccepted(A)=0. S is
         * placed beneath each observation and paired with the value of A for
         * that observation. The offset is satisfied when S=2 and observation
         * ONE (1) is the first observation accepted. The limit is satisfied
         * when A=3, which occurs at observation FOUR (4) which is also
         * S=last=5. The observation on which the limit is satisfied is accepted
         * and the slice halts as no more observations should be made. {2,3,4}
         * are accepted.
         * </p>
         * 
         * <pre>
         *  0 1 2 3 4 5 6 7 8 9
         *  S=1, A=0
         * </pre>
         * 
         * <pre>
         *  0 1 2 3 4 5 6 7 8 9
         *    S=2, A=0
         * </pre>
         * 
         * <pre>
         *  0 1 2 3 4 5 6 7 8 9
         *      S=3, A=1 {2}
         * </pre>
         * 
         * <pre>
         *  0 1 2 3 4 5 6 7 8 9
         *        S=4, A=2 {2,3}
         * </pre>
         * 
         * <pre>
         *  0 1 2 3 4 5 6 7 8 9
         *          S=5, A=3 {2,3,4}
         * </pre>
         * <p>
         * Note: The caller MUST be synchronized on the <em>shared</em>
         * {@link SliceStats} in order for the decision process to be thread
         * safe.
         * 
         * @param chunk
         *            The chunk of binding sets.
         * 
         * @return <code>true</code> if the slice is satisfied and the query
         *         should halt.
         */
        private boolean handleChunk(
                final UnsynchronizedArrayBuffer<IBindingSet> out,
                final IBindingSet[] chunk) {

            stats.chunksIn.increment();

//            int nadded = 0;
            
            for (int i = 0; i < chunk.length; i++) {

                if (stats.naccepted.get() >= limit)
                    return true; // nothing more will be accepted.

                stats.unitsIn.increment();

                final long S = stats.nseen.incrementAndGet();
                
                if (S <= offset)
                    continue; // skip solution.

                final long A = stats.naccepted.get();

                if (A < limit) {

                    final IBindingSet bset = chunk[i];

                    out.add(bset);

//                    nadded++;
                    
                    stats.naccepted.incrementAndGet();

                    if (log.isTraceEnabled())
                        log.trace(toString() + ":" + bset);

                }

            } // next bindingSet
            
            return false;

        }

        public String toString() {

            return super.toString() + "{offset=" + offset + ",limit="
                    + limit + ",nseen=" + stats.nseen + ",naccepted="
                    + stats.naccepted + "}";

        }

    }

//    /**
//     * This operator must be evaluated on the query controller.
//     */
//    @Override
//    public BOpEvaluationContext getEvaluationContext() {
//
//        return BOpEvaluationContext.CONTROLLER;
//
//    }

}
