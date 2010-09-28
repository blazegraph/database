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
 * Created on Aug 25, 2010
 */

package com.bigdata.bop.bset;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * This operator copies its source to its sink. Specializations exist which are
 * used to feed the the initial set of intermediate results into a pipeline (
 * {@link StartOp}) and which are used to replicate intermediate results to more
 * than one sink ({@link Tee}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CopyBindingSetOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * An optional {@link IConstraint}[] which places restrictions on the
         * legal patterns in the variable bindings.
         */
        String CONSTRAINTS = CopyBindingSetOp.class.getName() + ".constraints";

    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public CopyBindingSetOp(CopyBindingSetOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public CopyBindingSetOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * @see Annotations#CONSTRAINTS
     */
    public IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new CopyTask(this, context));

    }

    /**
     * Copy the source to the sink.
     * 
     * @todo Optimize this. When using an {@link IChunkAccessor} we should be
     *       able to directly output the same chunk.
     */
    static private class CopyTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The constraint (if any) specified for the join operator.
         */
        final private IConstraint[] constraints;

        CopyTask(final CopyBindingSetOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.constraints = op.constraints();

        }

        public Void call() throws Exception {
            final IAsynchronousIterator<IBindingSet[]> source = context
                    .getSource();
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
            final IBlockingBuffer<IBindingSet[]> sink2 = context.getSink2();
            try {
                final BOpStats stats = context.getStats();
                while (source.hasNext()) {
                    final IBindingSet[] chunk = source.next();
                    stats.chunksIn.increment();
                    stats.unitsIn.add(chunk.length);
                    final IBindingSet[] tmp = applyConstraints(chunk);
                    sink.add(tmp);
                    if (sink2 != null)
                        sink2.add(tmp);
                }
                sink.flush();
                if (sink2 != null)
                    sink2.flush();
                return null;
            } finally {
                sink.close();
                if (sink2 != null)
                    sink2.close();
                source.close();
            }
        }

        private IBindingSet[] applyConstraints(final IBindingSet[] chunk) {
            
            if (constraints == null) {

                /*
                 * No constraints, copy all binding sets.
                 */
                
                return chunk;
                
            }
            
            /*
             * Copy binding sets which satisfy the constraint(s).
             */
            
            IBindingSet[] t = new IBindingSet[chunk.length];
            
            int j = 0;
            
            for (int i = 0; i < chunk.length; i++) {
            
                final IBindingSet bindingSet = chunk[i];
                
                if (context.isConsistent(constraints, bindingSet)) {
                
                    t[j++] = bindingSet;
                    
                }
                
            }

            if (j != chunk.length) {

                // allocate exact size array.
                final IBindingSet[] tmp = (IBindingSet[]) java.lang.reflect.Array
                        .newInstance(chunk[0].getClass(), j);

                // make a dense copy.
                System.arraycopy(t/* src */, 0/* srcPos */, tmp/* dst */,
                        0/* dstPos */, j/* len */);

                t = tmp;

            }

            return t;
            
        }

    } // class CopyTask

}
