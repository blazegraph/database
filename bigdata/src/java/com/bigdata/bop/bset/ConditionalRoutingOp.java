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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An operator for conditional routing of binding sets in a pipeline. The
 * operator will copy binding sets either to the default sink (if a condition is
 * satisfied) and to the alternate sink otherwise.
 * <p>
 * Conditional routing can be useful where a different data flow is required
 * based on the type of an object (for example a term identifier versus an
 * inline term in the RDF database) or where there is a need to jump around a
 * join group based on some condition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConditionalRoutingOp extends BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AbstractPipelineOp.Annotations {

        /**
         * An {@link IConstraint} which specifies the condition. When the
         * condition is satisfied the binding set is routed to the default sink.
         * When the condition is not satisfied, the binding set is routed to the
         * alternative sink.
         */
        String CONDITION = ConditionalRoutingOp.class.getName() + ".condition";

    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public ConditionalRoutingOp(ConditionalRoutingOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public ConditionalRoutingOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * @see Annotations#CONDITION
     */
    public IConstraint getCondition() {
        
        return (IConstraint) getProperty(Annotations.CONDITION);
        
    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ConditionalRouteTask(this, context));
        
    }

    /**
     * Copy the source to the sink or the alternative sink depending on the
     * condition.
     */
    static private class ConditionalRouteTask implements Callable<Void> {

        private final BOpStats stats;

        private final IConstraint condition;
        
        private final IAsynchronousIterator<IBindingSet[]> source;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        ConditionalRouteTask(final ConditionalRoutingOp op,
                final BOpContext<IBindingSet> context) {

            this.stats = context.getStats();
            
            this.condition = op.getCondition();

            if (condition == null)
                throw new IllegalArgumentException();
            
            this.source = context.getSource();
            
            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            if (sink2 == null)
                throw new IllegalArgumentException();
            
            if (sink == sink2)
                throw new IllegalArgumentException();

        }

        public Void call() throws Exception {
            try {
                while (source.hasNext()) {
                    
                    final IBindingSet[] chunk = source.next();
                    
                    stats.chunksIn.increment();
                    stats.unitsIn.add(chunk.length);

                    final IBindingSet[] def = new IBindingSet[chunk.length];
                    final IBindingSet[] alt = new IBindingSet[chunk.length];
                    
                    int ndef = 0, nalt = 0;

                    for(int i=0; i<chunk.length; i++) {

                        final IBindingSet bset = chunk[i];

                        if (condition.accept(bset)) {

                            def[ndef++] = bset;
                            
                        } else {
                            
                            alt[nalt++] = bset;
                            
                        }
                        
                    }

                    if (ndef > 0) {
                        if (ndef == def.length)
                            sink.add(def);
                        else
                            sink.add(Arrays.copyOf(def, ndef));
                        stats.chunksOut.increment();
                        stats.unitsOut.add(ndef);
                    }

                    if (nalt > 0) {
                        if (nalt == alt.length)
                            sink2.add(alt);
                        else
                            sink2.add(Arrays.copyOf(alt, nalt));
                        stats.chunksOut.increment();
                        stats.unitsOut.add(nalt);
                    }
                    
                }

                sink.flush();
                sink2.flush();
                
                return null;
                
            } finally {
                
                sink.close();
                sink2.close();
                
            }

        }

    }

}
