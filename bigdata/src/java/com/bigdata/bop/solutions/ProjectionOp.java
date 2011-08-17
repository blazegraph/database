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
package com.bigdata.bop.solutions;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * A pipelined which may be used to project bare variables and computed value
 * expressions, but not value expressions involving aggregates. This operator is
 * thread-safe and may be used in any {@link BOpEvaluationContext}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
@Deprecated // Probably does not make sense with the materialization pipeline.
public class ProjectionOp extends PipelineOp {

    static private final transient Logger log = Logger
            .getLogger(ProjectionOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * An {@link IValueExpression}[] identifying the ordered list of value
         * expressions to be computed.
         * <p>
         * Note: In order to pass on all variables bound in the solutions you
         * should simply NOT use the {@link ProjectionOp}. However, the query
         * plan will often define anonymous variables which must be dropped. In
         * that case, the query plan is responsible for either explicitly
         * projecting the desired variables or filtering out the anonymous
         * variables. (Anonymous variables should be filtered out as soon as
         * possible to reduce heap pressure and minimize NIO on a cluster.)
         */
        String PROJECT = JoinAnnotations.class.getName() + ".project";

    }

    /**
     * Required deep copy constructor.
     */
    public ProjectionOp(final ProjectionOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public ProjectionOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

        final IValueExpression<?>[] exprs = (IValueExpression[]) getRequiredProperty(Annotations.PROJECT);

        if (exprs.length == 0)
            throw new IllegalArgumentException(Annotations.PROJECT);

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Task executing on the node.
     */
    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final ProjectionOp op;

        private final IValueExpression<?>[] exprs;

        ChunkTask(final ProjectionOp op, final BOpContext<IBindingSet> context) {

            this.op = op;

            this.context = context;

            this.exprs = (IValueExpression[]) op
                    .getRequiredProperty(Annotations.PROJECT);

        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        op.getChunkCapacity(), sink);

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    for (IBindingSet bset : a) {

                        final IBindingSet out = new ListBindingSet();

                        try {

                            for (IValueExpression<?> e : exprs) {

                                final Object val = e.get(bset);

                                final IVariable<?> var = (e instanceof IVariable<?> ? (IVariable<?>) e
                                        : ((IBind<?>) e).getVar());

                                @SuppressWarnings({ "unchecked", "rawtypes" })
                                final Constant<?> c = new Constant(val);
                                
                                out.set(var, c);

                            }

                        } catch (SparqlTypeErrorException ex) {

                            if (log.isInfoEnabled())
                                log.info("Dropping solution with type error: "
                                        + ex);

                            continue;

                        }
                       
                        unsyncBuffer.add(out);

                    }

                }

                unsyncBuffer.flush();
                
                sink.flush();

                // done.
                return null;

            } finally {

                sink.close();

            }

        }

    } // ChunkTask

}
