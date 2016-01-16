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
/*
 * Created on Nov 3, 2011
 */

package com.bigdata.bop.rdf.join;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Operator computing a join over two variables already bound variables in the
 * mapping set (for which the IV has been computed already). More specifically, 
 * given two variables ?targetVar and ?tmpVar as input, the operator
 * iterates over the source binding set and tries to unify ?targetVar and
 * ?tmpVar according to the following rules:
 * 
 * (C1) If a binding binds both ?targetVar and ?tmpVar, and ?targetVar *equals*
 *       ?tmpVar, then ?tmpVar is dropped from the binding (which essentially
 *       means that the join/unification operation succeeded)
 * (C2) If a binding binds both ?targetVar and ?tmpVar, and ?targetVar does
 *       *not* equal ?tmpVar, then the whole binding is filtered out
 *       (which essentially means that the join/unification failed)
 * (C3) If a binding binds only ?targetVar, then the binding is left unmodified
 *       (this can be considered a succeeding join over an unbound variable)
 * (C4) If a binding binds only ?tmpVar, then we rename ?tmpVar to ?targetVar
 *       (this can be considered a succeeding join over an unbound variable)
 *  
 * Note that in all mappings that pass this unification procedure, ?tmpVar will
 * not be bound anymore. Also note that this operator modifies the mapping sets,
 * which is why we cannot cover this through a {@link ConditionalRoutingOp} with
 * a special condition (as we want to avoid conditions that have side effects
 * on the underlying mapping sets).
 * 
 * This operator is used in the context of BIND clauses, whenever they
 * construct complex values (such as fresh URIs) that are later reused in
 * comparisons (such as joins, filters, etc.): for these cases, we first
 * bind the BIND variable to a dummy variable (?X'), compute the IV for these
 * dummy variables, and then use the VariableUnificationOp to join it with 
 * potentially earlier bound variables.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1007">Ticket 1007: Using bound
 *      variables to refer to a graph</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 **/
public class VariableUnificationOp extends PipelineOp {

//   private final static Logger log = 
//      Logger.getLogger(VariableUnificationOp.class);

   private static final long serialVersionUID = 1L;

   public interface Annotations extends PipelineOp.Annotations {

      String VARS = VariableUnificationOp.class.getName() + ".vars";

   }

   /**
    * @param args
    * @param annotations
    */
   public VariableUnificationOp(final BOp[] args,
         final Map<String, Object> annotations) {

      super(args, annotations);
      
      // make sure there are exactly two variables, targetVar and tmpVar
      if (getVars().length!=2)
         throw new IllegalArgumentException();
      
   }

   public VariableUnificationOp(final BOp[] args, final NV... annotations) {

      this(args, NV.asMap(annotations));

   }
   
   public VariableUnificationOp(final VariableUnificationOp op) {

      super(op);
      
  }

   /**
    * @return the target variable for unification, which will remain bound
    * in mappings passing the unification process
    */
   public IVariable<?> getTargetVar() {

      return getVars()[0];

   }

   /**
    * @return the variable for unification that will be removed/renamed in
    * mappings passing the unification process
    */
   public IVariable<?> getTmpVar() {
      
      return getVars()[1];
      
   }
   

   /**
    * @return the array [targetVar, tmpVar] consisting of exactly two variables.
    */
   protected IVariable<?>[] getVars() {

      IVariable<?>[] vars = (IVariable<?>[]) getProperty(Annotations.VARS);

      assert vars!=null && vars.length==2;
      
      return vars;

   }

   @Override
   public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

      return new FutureTask<Void>(new ChunkTask(this, context));

   }

   /**
    * Task executing on the node.
    */
   static private class ChunkTask implements Callable<Void> {

      private final BOpContext<IBindingSet> context;

      private final IVariable<?> targetVar;
      
      private final IVariable<?> tmpVar;


      ChunkTask(final VariableUnificationOp op,
            final BOpContext<IBindingSet> context) {

         this.context = context;

         this.targetVar = op.getTargetVar();
         
         this.tmpVar = op.getTmpVar();
      }

      @Override
      public Void call() throws Exception {

         final BOpStats stats = context.getStats();

         final ICloseableIterator<IBindingSet[]> itr = context.getSource();

         final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

         try {

            while (itr.hasNext()) {

               final IBindingSet[] chunk = itr.next();

               // array to collect unified mappings, including a counter
               final IBindingSet[] unified = new IBindingSet[chunk.length];
               int nunified = 0;
               
               stats.chunksIn.increment();
               stats.unitsIn.add(chunk.length);

               for (IBindingSet bs : chunk) {
                  
                  final IConstant<?> tmpVal = bs.get(tmpVar);
                  final IConstant<?> targetVal = bs.get(targetVar);

                  /*
                   * Variable to be initialized iff unified binding set exists 
                   */
                  IBindingSet bsUnified = null; // unified bs, if succeeds
                  
                  /*
                   * Case 1: tmpVal is bound, so we need to further investigate
                   */
                  if (tmpVal!=null) {
                     
                     /*
                      * Case 1.1: if targetVar is not bound in the mapping, then
                      * simply rename tmpVar to targetVar
                      */
                     if (targetVal==null) {
                        
                        bsUnified= bs.clone();
                        bsUnified.set(targetVar, tmpVal);
                        bsUnified.clear(tmpVar);
                        
                     } else {
                        
                        /*
                         * Case 2: if targetVar is also bound in the mapping,
                         * then we need to compare the values; in case this
                         * check succeeds, the variables are identical and we
                         * can remove the temporary variable; if they are not,
                         * the join failed and we drop the whole mapping
                         */
                        if (targetVal.equals(tmpVal)) {
                           
                           bsUnified = bs.clone();
                           bsUnified.clear(tmpVar);

                        } // else: drop mapping, i.e. do not set bsUnified
                     }
                     
                  /*
                   * Case 2: if tmpVal is not bound, unification succeeds by
                   * definition and we can keep the mapping as is
                   */
                  } else {
                      
                     bsUnified = bs.clone();
                        
                  }
                  
                  /*
                   * If a unified mapping exists, collect it in the result array
                   */
                  if (bsUnified!=null)
                     unified[nunified++] = bsUnified;
                  
               }

               /**
                * Copy the unified mappings to the sink in an efficient way
                */
               if (nunified > 0) {
                  if (nunified == unified.length)
                     sink.add(unified);
                  else
                     sink.add(Arrays.copyOf(unified, nunified));
               }

            }

            sink.flush();

            // done.
            return null;

         } finally {

            sink.close();

         }

      }

   } // ChunkTask

}
