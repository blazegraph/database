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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.RunningQuery;
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
 * through.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo If this operator is invoked for each chunk output by a query onto the
 *       pipeline then it will over produce unless (A) it is given the same
 *       {@link BOpStats} each time; and (B) it is not invoked for two chunks
 *       concurrently.
 *       <p>
 *       A safer way to impose the slice constraint is by wrapping the query
 *       buffer on the query controller. Once the slice is satisfied, it can
 *       just cancel the query. The only drawback of this approach is that the
 *       wrapping a buffer is not really the same as applying a {@link BOp} to
 *       the pipeline so it falls outside of the standard operator evaluation
 *       logic.
 * 
 * @todo If we allow complex operator trees in which "subqueries" can also use a
 *       slice then either then need to run as their own query with their own
 *       {@link RunningQuery} state or the API for cancelling a running query as
 *       used here needs to only cancel evaluation of the child operators.
 *       Otherwise we could cancel all operator evaluation for the query,
 *       including operators which are ancestors of the {@link SliceOp}.
 */
public class SliceOp extends BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BindingSetPipelineOp.Annotations {

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
     * @param op
     */
    public SliceOp(PipelineOp<IBindingSet> op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public SliceOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * @see Annotations#OFFSET 
     */
    public long getOffset() {
        
        return getRequiredProperty(Annotations.OFFSET);
        
    }

    /**
     * @see Annotations#LIMIT 
     */
    public long getLimit() {
        
        return getRequiredProperty(Annotations.LIMIT);
        
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

        /** #of solutions visited. */
        private long nseen;

        /** #of solutions accepted. */
        private long naccepted;

        SliceTask(final SliceOp op, final BOpContext<IBindingSet> context) {

            this.op = op;
            
            this.context = context;
            
            this.offset = op.getOffset();

            this.limit = op.getLimit();

            if (offset < 0)
                throw new IllegalArgumentException();

            if (limit <= 0)
                throw new IllegalArgumentException();

        }

        public Void call() throws Exception {

            final IAsynchronousIterator<IBindingSet[]> source = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final BOpStats stats = context.getStats();

            try {

                // buffer forms chunks which get flushed onto the sink.
                final UnsynchronizedArrayBuffer<IBindingSet> out = new UnsynchronizedArrayBuffer<IBindingSet>(
                        sink, op.getChunkCapacity());

                boolean halt = false;
                
                while (source.hasNext()) {

                    final IBindingSet[] chunk = source.next();

                    stats.chunksIn.increment();

                    for (int i = 0; i < chunk.length; i++) {

                        stats.unitsIn.increment();

                        if (nseen < offset) {
                            // skip solution.
                            nseen++;
                            continue;
                        }

                        if (out.add2(chunk[i])) {
                            // chunk was output.
                            stats.chunksOut.increment();
                        }

//                        System.err.println("accept: " + chunk[i]);

                        stats.unitsOut.increment();

                        naccepted++;
                        nseen++;
                        if (naccepted >= limit) {
                            if (!out.isEmpty()) {
                                out.flush();
                                stats.chunksOut.increment();
                            }
                            halt = true;
                            break;
                        }

                    }
                    
                }

                sink.flush();

                if (halt)
                    cancelQuery();
                
                return null;
                
            } finally {
                
                sink.close();
                
            }

        }

        /**
         * Cancel the query evaluation. This is invoked when the slice has been
         * satisfied. At that point we want to halt not only the {@link SliceOp}
         * but also the entire query since it does not need to produce any more
         * results.
         */
        private void cancelQuery() {

            context.halt();
            
        }

    }

}
