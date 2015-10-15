/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct. 15, 2015
 */
package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * {@inheritDoc}
 * <p>
 * JVM specific version of the solution set hash join op, 
 * 
 * @see JVMPipelinedHashJoinUtility
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class JVMPipelinedSolutionSetHashJoinOp extends SolutionSetHashJoinOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * Deep copy constructor.
     */
    public JVMPipelinedSolutionSetHashJoinOp(JVMPipelinedSolutionSetHashJoinOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public JVMPipelinedSolutionSetHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);
        
    }

    public JVMPipelinedSolutionSetHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<IBindingSet>(context, this));
        
    }

   /**
    * Task executing on the node.
    */
   protected static class ChunkTask<E> extends SolutionSetHashJoinOp.ChunkTask<E> {

      public ChunkTask(final BOpContext<IBindingSet> context,
            final SolutionSetHashJoinOp op) {
         super(context, op);
      }

      @Override
      protected void doHashJoin() {

         // TODO -> we need another condition here, this is not valid for queries
         // with OPTIONAL etc (which may not buffer anything anymore)
         // if (state.isEmpty())
         // return;

         stats.accessPathCount.increment();

         // TODO: is this correct?
         stats.accessPathRangeCount.add(state.getRightSolutionCount());

         final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = 
            new UnsyncLocalOutputBuffer<IBindingSet>(op.getChunkCapacity(), sink);

         state.hashJoin2(context.getSource(), stats, unsyncBuffer, constraints);

         if (context.isLastInvocation()) {

            doLastPass(unsyncBuffer);

         }

         unsyncBuffer.flush();
         sink.flush();

      }

      @Override
      protected void doLastPass(
            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer) {
         
         /**
          * There's nothing to be done here: for the pipelined version, the
          * handling of Optionals and the like must not be delayed to the end,
          * but is covered directly in hashJoin2().
          */
      }
   }
}
