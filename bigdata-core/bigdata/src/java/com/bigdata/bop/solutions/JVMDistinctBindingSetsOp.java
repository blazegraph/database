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
package com.bigdata.bop.solutions;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.ConcurrentHashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.IDistinctFilter;
import com.bigdata.bop.join.JVMDistinctFilter;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A pipelined DISTINCT operator based on a hash table.
 * <p>
 * Note: This implementation is a pipelined operator which inspects each chunk
 * of solutions as they arrive and those solutions which are distinct for each
 * chunk are passed on. It uses a {@link ConcurrentMap} and is thread-safe. It
 * is significantly faster than the single-threaded hash index routines in the
 * {@link JVMHashJoinUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class JVMDistinctBindingSetsOp extends PipelineOp {

//	private final static transient Logger log = Logger
//			.getLogger(JVMDistinctBindingSetsOp.class);
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            ConcurrentHashMapAnnotations, DistinctAnnotations {

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public JVMDistinctBindingSetsOp(final JVMDistinctBindingSetsOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public JVMDistinctBindingSetsOp(final BOp[] args,
            final Map<String, Object> annotations) {

		super(args, annotations);

		switch (getEvaluationContext()) {
		case CONTROLLER:
        case HASHED:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		// shared state is used to share the hash table.
		if (!isSharedState()) {
			throw new UnsupportedOperationException(Annotations.SHARED_STATE
					+ "=" + isSharedState());
		}

		final IVariable<?>[] vars = (IVariable[]) getProperty(Annotations.VARIABLES);

		if (vars == null)
			throw new IllegalArgumentException();

    }

    public JVMDistinctBindingSetsOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * @see Annotations#INITIAL_CAPACITY
     */
    public int getInitialCapacity() {

        return getProperty(Annotations.INITIAL_CAPACITY,
                Annotations.DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * @see Annotations#LOAD_FACTOR
     */
    public float getLoadFactor() {

        return getProperty(Annotations.LOAD_FACTOR,
                Annotations.DEFAULT_LOAD_FACTOR);

    }

    /**
     * @see Annotations#CONCURRENCY_LEVEL
     */
    public int getConcurrencyLevel() {

        return getProperty(Annotations.CONCURRENCY_LEVEL,
                Annotations.DEFAULT_CONCURRENCY_LEVEL);

    }
    
    /**
     * @see Annotations#VARIABLES
     */
    public IVariable<?>[] getVariables() {

        return (IVariable<?>[]) getRequiredProperty(Annotations.VARIABLES);
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DistinctTask(this, context));
        
    }

    /**
     * Task executing on the node.
     */
    static private class DistinctTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final IDistinctFilter filter;
        
        private final int chunkCapacity;
        
        DistinctTask(final JVMDistinctBindingSetsOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.chunkCapacity = op.getChunkCapacity();
            
            final IVariable<?>[] vars = op.getVariables();

            /*
             * The map is shared state across invocations of this operator task.
             */
            {
                final Integer key = op.getId();

                final IQueryAttributes attribs = context.getRunningQuery()
                        .getAttributes();

                IDistinctFilter filter = (IDistinctFilter) attribs.get(key);

                if (filter == null) {

                    filter = new JVMDistinctFilter(vars,
                            op.getInitialCapacity(), op.getLoadFactor(),
                            op.getConcurrencyLevel());

                    final IDistinctFilter tmp = (IDistinctFilter) attribs
                            .putIfAbsent(key, filter);

                    if (tmp != null)
                        filter = tmp;

                }

                this.filter = filter;

            }

        }
        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final ICloseableIterator<IBindingSet[]> itr = context.getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        chunkCapacity, sink);

                filter.filterSolutions(itr, stats, unsyncBuffer);

                unsyncBuffer.flush();

                sink.flush();

                // done.
                return null;

            } finally {

                if (context.isLastInvocation()) {

                    filter.release();

                }

                sink.close();

            }

        }

    }

}
