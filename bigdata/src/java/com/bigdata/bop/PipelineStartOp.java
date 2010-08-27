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

package com.bigdata.bop;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * This operator copies its source to its sink. It is used to feed the first
 * join in the pipeline. The operator should have no children but may be
 * decorated with annotations as necessary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineStartOp extends AbstractPipelineOp<IBindingSet> implements
        BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public PipelineStartOp(PipelineStartOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public PipelineStartOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new CopyTask(context));
        
    }

    /**
     * Copy the source to the sink. 
     */
    static private class CopyTask implements Callable<Void> {

//        private final BOpContext<IBindingSet> context;

        private final BOpStats stats;
        
        private final IAsynchronousIterator<IBindingSet[]> source;

        private final IBlockingBuffer<IBindingSet[]> sink;

        CopyTask(final BOpContext<IBindingSet> context) {

//            this.context = context;
        
            stats = context.getStats();
            
            this.source = context.getSource();
            
            this.sink = context.getSink();

        }

        public Void call() throws Exception {
            try {
                while (source.hasNext()) {
                    final IBindingSet[] chunk = source.next();
                    stats.chunksIn.increment();
                    stats.unitsIn.add(chunk.length);
                    sink.add(chunk);
                    stats.chunksOut.increment();
                    stats.unitsOut.add(chunk.length);
                }
                return null;
            } finally {
                sink.close();
            }
        }

    }

}
