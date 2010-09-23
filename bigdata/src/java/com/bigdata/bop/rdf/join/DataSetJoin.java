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
 * Created on Sep 20, 2010
 */

package com.bigdata.bop.rdf.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * DataSetJoin(left,var)[graphs={graphIds}; maxParallel=50]
 * <p>
 * SPARQL specific join binds <i>var</i> to each of the given graphIds values
 * for each source binding set. This join operator is useful when the
 * multiplicity of the graphs is small to moderate. If there are a very large
 * number of graphs, then the operator tree is to cumbersome and you would do
 * better off joining against an index (whether temporary or permanent)
 * containing the graphs.
 * <p>
 * The evaluation context is {@link BOpEvaluationContext#ANY}.
 * 
 * @todo An alternative would be to develop an inline access path and then
 *       specify a standard predicate which references the data in its
 *       annotation. That could then generalize to a predicate which references
 *       persistent data, query or tx local data, or inline data. However, the
 *       DataSetJoin is still far simpler since it just binds the var and send
 *       out the asBound binding set and does not need to worry about internal
 *       parallelism, alternative sinks, or chunking.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataSetJoin extends BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BindingSetPipelineOp.Annotations {

        /**
         * The variable to be bound.
         */
        String VAR = DataSetJoin.class.getName() + ".var";

        /**
         * The {@link IV}s to be bound. This is logically a set and SHOULD NOT
         * include duplicates. The elements in this array SHOULD be ordered for
         * improved efficiency.
         */
        String GRAPHS = DataSetJoin.class.getName() + ".graphs";

    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public DataSetJoin(DataSetJoin op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * @param args
     * @param annotations
     */
    public DataSetJoin(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
        getVar();
        getGraphs();
    }

    public IVariable<?> getVar() {
        return (IVariable<?>)getRequiredProperty(Annotations.VAR);
    }

    public IV[] getGraphs() {
        return (IV[]) getRequiredProperty(Annotations.GRAPHS);
    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DataSetJoinTask(this,context));
        
    }

    /**
     * Copy the source to the sink.
     * 
     * @todo Optimize this. When using an {@link IChunkAccessor} we should be
     *       able to directly output the same chunk.
     */
    static private class DataSetJoinTask implements Callable<Void> {

        private final DataSetJoin op;
        
        private final BOpContext<IBindingSet> context;
        
        private final IVariable<?> var;
        private final IV[] graphs;

        DataSetJoinTask(final DataSetJoin op,
                final BOpContext<IBindingSet> context) {

            this.op = op;

            this.context = context;

            var = op.getVar();

            graphs = op.getGraphs();

        }

        /**
         *       FIXME unit tests.
         */
        public Void call() throws Exception {
            final IAsynchronousIterator<IBindingSet[]> source = context
                    .getSource();
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
            try {
                final BOpStats stats = context.getStats();
                while (source.hasNext()) {
                    final IBindingSet[] chunk = source.next();
                    stats.chunksIn.increment();
                    stats.unitsIn.add(chunk.length);
                    handleChunk_(chunk, sink);
                }
                sink.flush();
                return null;
            } finally {
                sink.close();
                source.close();
            }
        }

        /**
         * Cross product join. For each source binding set and each graph,
         * output one binding set in which the variable is bound to that graph.
         * 
         * @param chunk
         *            A chunk of {@link IBindingSet}s from the source.
         * @param sink
         *            Where to write the data.
         * 
         * @todo Should we choose the nesting order of the loops based on the
         *       multiplicity of the source chunk size and the #of graphs to be
         *       bound? That way the inner loop decides the chunk size of the
         *       output.
         *       <p>
         *       Should we always emit an asBound source chunk for a given
         *       graphId? That will cluster better when the target predicate is
         *       mapped over CSPO.
         */
        private void handleChunk_(final IBindingSet[] chunk,
                final IBlockingBuffer<IBindingSet[]> sink) {
            
            final IBindingSet[] chunkOut = new IBindingSet[chunk.length
                    * graphs.length];
            
            int n = 0;
            
            for (IBindingSet bset : chunk) {
            
                for (IV c : graphs) {
                
                    bset = bset.clone();
                    
                    bset.set(var, new Constant<IV>(c));
                    
                    chunkOut[n++] = bset;
                    
                }
                
            }
            
            sink.add(chunkOut);
        
        }

    }

}
