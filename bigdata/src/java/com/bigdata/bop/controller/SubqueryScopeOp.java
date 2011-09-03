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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * This operator is used to coordinate the change of lexical scope for SPARQL
 * variables when entering and leaving a <em>pipelined</em> subquery. A variable
 * within a subquery is distinct from the same name variable outside of the
 * subquery unless the variable is projected from the subquery. This is achieved
 * using {@link IBindingSet#push(IVariable[])} and
 * {@link IBindingSet#pop(IVariable[])} to scope the bindings within the
 * subquery to its projection.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232#comment:11
 */
public class SubqueryScopeOp extends PipelineOp {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The {@link IVariable}[] consisting of the variables projected by the
         * subquery (required).
         */
        final String VARS = SubqueryScopeOp.class.getName() + ".vars";

        /**
         * A boolean annotation whose truth state indicates whether a PUSH or
         * POP operation will be executed for the {@link IBindingSet}.
         */
        final String PUSH = SubqueryScopeOp.class.getName() + ".push";
        
    }

    /**
     * Deep copy constructor.
     */
    public SubqueryScopeOp(final SubqueryScopeOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SubqueryScopeOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        final IVariable<?>[] vars = (IVariable[]) getRequiredProperty(Annotations.VARS);

        for (IVariable<?> var : vars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.VARS);

        }

        getRequiredProperty(Annotations.PUSH);

    }

    public SubqueryScopeOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * @see Annotations#PUSH
     */
    public boolean getPush() {

        return (Boolean) getRequiredProperty(Annotations.PUSH);

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));
        
    }
    
	/**
	 * Evaluates the subquery for each source binding set. If the controller
	 * operator is interrupted, then the subqueries are cancelled. If a subquery
	 * fails, then all subqueries are cancelled.
	 */
    private static class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The variables projected by the subquery.
         */
        private final IVariable<?>[] vars;

        /**
         * When <code>true</code>, PUSH, otherwise POP.
         */
        private final boolean push;

        public ChunkTask(final SubqueryScopeOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.vars = (IVariable[]) op.getRequiredProperty(Annotations.VARS);

            this.push = op.getPush();

        }

        /**
         * Evaluate.
         */
        public Void call() throws Exception {
            
            try {
                
                // source.
                final IAsynchronousIterator<IBindingSet[]> source = context
                        .getSource();

                // default sink
                final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

                final BOpStats stats = context.getStats();
                
                while (source.hasNext()) {

                    final IBindingSet[] chunk = source.next();

                    for (int i = 0; i < chunk.length; i++) {

                        final IBindingSet bs = chunk[i];//.clone();

                        if (push)
                            bs.push(vars);
                        else
                            bs.pop(vars);

//                        chunk[i] = bs;
                        
                    }
                    
                    if (stats != null) {

                        stats.chunksIn.increment();

                        stats.unitsIn.add(chunk.length);

                    }

                    sink.add(chunk);
                    
                }
                
                sink.flush();

                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();
                
                if (context.getSink2() != null)
                    context.getSink2().close();

            }
            
        }

    }

}
