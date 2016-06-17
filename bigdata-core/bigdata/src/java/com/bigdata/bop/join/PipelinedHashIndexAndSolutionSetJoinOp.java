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
 * Created on Oct 20, 2015
 */

package com.bigdata.bop.join;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.SubqueryAnnotations;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Operator for pipelined hash index construction and subsequent join. Note that
 * this operator needs not to be combined with a solution set hash join, but
 * instead gets the subquery/subgroup passed as a parameter and thus can be
 * considered as an "all-in-one" build-hash-index-and-join operation.
 * 
 * The operator is designed for single-threaded use (as it is the case for the
 * non-pipelined hash index & join operators). It's processing scheme is
 * illustrated by an example in the following. Assume we have a query such as
 * 
 * <code>
   SELECT * WHERE {
     ?s <http://p1> ?o1
     OPTIONAL {
       ?s <http://p2> ?o2 .
       ?s <http://p3> ?o3 .
     }
   } LIMIT 10
   </code>
   
   to be evaluated over the data set
   
   <code>
     <http://s1> <http://p1> <http://o11> .
     <http://s1> <http://p2> <http://o12> .
     <http://s1> <http://p3> <http://o13> .

     <http://s2> <http://p1> <http://o21> .
     <http://s2> <http://p2> <http://o22> .

     <http://s1> <http://p1> <http://o11b> .

   </code>
 * 
 * , where the OPTIONAL is considered as a complex group that is translated
 * using a hash join pattern. The approach taken by this operator is that the
 * OPTIONAL pattern is considered as subquery that is passed in to this operator
 * via Annotations.SUBQUERY. The operator logically proceeds as follows:
 * 
 * 1. Incoming are the bindings from outside, i.e. in our example the bindings
 *    for triple pattern "?s <http://p1> ?o1". Given the input data at hand,
 *    this means we have the following binding set
 *    
 *    { 
 *      { ?s -> <http://s1> , ?o1 -> <http://o11> },
 *      { ?s -> <http://s2> , ?o1 -> <http://o21> } 
 *      { ?s -> <http://s1> , ?o1 -> <http://o11b> }      
 *    }
 *    
 *    coming in. For the sake of this example, assume the solutions are dropping
 *    in one after the after (chunk size=1) s.t. we have three iterations,
 *    but note that the implementation implements a generalized, vectored
 *    approach processing the input chunk by chunk..
 *    
 *  2. The join variable set is { ?s }. We compute the distinct projection over
 *     ?s on the incoming bindings:
 *     
 *     - In the 1st iteration, the distinct projection is { ?s -> <http://s1> },
 *     - In the 2nd iteration, the distinct projection is { ?s -> <http://s2> },
 *     - In the 3rd iteration, the distinct projection is { ?s -> <http://s1> } again.
 *     
 *  3. For each of these distinct projections, we decide whether the subquery
 *     has been evaluated with the distinct projection as input before. 
 *     If the subquery has already been evaluated, we take the fast path and 
 *     proceed with step 5. If it has not yet been evaluated before, proceed 
 *     to step 4 (namely its evaluation and buffering of the solution on the
 *     hash index).
 *     
 *     For our example this means: in the first iteration we proceed to step
 *     (4) with { ?s -> <http://s1> } as input; in the second iteration, we
 *     proceed to step (4) with { ?s -> <http://s2> } as input; in the third
 *     iteration, we can directly proceed to step (5): the distinct projection 
 *     { ?s -> <http://s1> } has been encountered in iteration 1 already.
 *     
 *  4. Evaluate the subquery for the incoming binding. The result is stored in
 *     a hash index (this hash index is used, in future, to decide whether
 *     the result has been already computed before for the distinct projection
 *     in step 3; note that, in addition to the hash index, we also record those
 *     distinct projections for which the subquery has been evaluated without 
 *     producing a result, to avoid unnecessary re-computation).
 *     
 *     In the first iteration, we compute the subquery result 
 *     { ?s -> <http://s1> , ?o2 -> <http://o12> , ?o3 -> <http://o13> } for
 *     the input binding { ?s -> <http://s1> }; in the second iteration, we
 *     compute { ?s -> <http://s2> } for the input binding { ?s -> <http://s2> }
 *     (i.e., OPTIONAL subquery that does not match, leaving the input
 *     unmodified); there is no third iteration step.
 *     
 *  5. Join the original bindings (from which the distinct projection was 
 *     obtained) against the hash index and output the results. The operator
 *     supports all kinds of joins (Normal, Exists, NotExists, etc.).
 *     
 *     We thus obtain the following matches with the hash index:
 *     - Iteration 1: { ?s -> <http://s1>,  ?o1 -> <http://o11>} JOIN 
 *                    { ?s -> <http://s1> , ?o2 -> <http://o12> , ?o3 -> <http://o13> }
 *     - Iteration 2: { ?s -> <http://s2>, ?o1 -> <http://o12> } JOIN { ?s -> <http://s2> }
 *     - Iteration 3: { ?s -> <http://s1> , ?o1 -> <http://o11b> } JOIN 
 *                    { ?s -> <http://s1> , ?o2 -> <http://o12> , ?o3 -> <http://o13> }
 *                    
 *     This gives us the expected final result:
 *     
 *     {
 *       { ?s -> <http://s1>, ?o1 -> <http://o11>,  ?o2 -> <http://o12> , ?o3 -> <http://o13> },
 *       { ?s -> <http://s2>, ?o1 -> <http://o12> },
 *       { ?s -> <http://s2>, ?o1 -> <http://o11b>, ?o2 -> <http://o12> , ?o3 -> <http://o13> }
 *     }
 *     
 *  Note that this strategy is pipelined in the sense that all results from the
 *  left are emitted as soon as the subquery result for its distinct projection
 *  result has been calculated.
 *  
 *  # Further notes: 
 *  1.) Vectored processing: the implementation uses a vectored processing
 *  approach: instead of processing the mappings one by one, we collect
 *  unseen distinct projections and their associated incoming mappings in two
 *  buffers, namely the distinctProjectionBuffer and the incomingBindingssBuffer. 
 *  
 *  Buffer size is controlled via annotations: 
 *  incomingBindingssBufferThreshold and the Annotations.DISTINCT_PROJECTION_BUFFER_THRESHOLD. 
 *  If, after processing a chunk, one of these thresholds is exceeded for the
 *  respective buffer (or, alternatively, if we're in the lastPass), the
 *  mappings in the distinctProjectionBuffer are provided as input to
 *  the subquery "in batch" (cf. step 4.)) and subsequently the bindings from
 *  the incomingBindingssBuffer are joined (and released). At that point, the
 *  buffers become empty again.
 *  
 *  Effectively, the thresholds allow to reserve some internal buffer space. For
 *  now, we go with default values, but we may want to expose these buffers via
 *  query hints or the like at some point. 
 *  
 *  2.) Alternative source: the strategy sketched above illustrates how the op
 *  works for subqueries. An alternative way of using the operator is by
 *  setting Annotations.BINDING_SETS_SOURCE *instead of* the subquery. This
 *  is used when computing a hash join with a VALUES clause; in that case, the
 *  binding sets provided by the VALUES clause are considered as "inner query";
 *  as a notable difference, this set of values is static and does not need to
 *  be re-evaluated each time, it is submitted once to the hash index in the
 *  beginning and joined with every incoming binding.
 *  
 *  3.) There exist two "implementations" of the operator, namely the
 *  {@link JVMPipelinedHashJoinUtility} and the {@link HTreePipelinedHashJoinUtility}.
 *  
 *  # Other remarks:
 *  There are some more technicalities like support for ASK_VAR (which is used
 *  by the FILTER (NOT) EXISTS translation scheme, which work in principle in
 *  the same way as they do for the standard hash join.
 *  
 *  Usage: the pipelined hash join operator is preferably used for queries
 *  containing LIMIT but *no* ORDER BY. It can also be globally enabled by
 *  system parameter {@link QueryHints#PIPELINED_HASH_JOIN} and via query
 *  hints. See {@link AST2BOpUtility#usePipelinedHashJoin} for the method
 *  implementing its selection strategy.
 * 
 * @see JVMPipelinedHashJoinUtility for implementation
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class PipelinedHashIndexAndSolutionSetJoinOp extends HashIndexOp {

   private static final long serialVersionUID = 3473675701742394157L;

   
   public interface Annotations extends HashIndexOp.Annotations, SubqueryAnnotations {
   
       /**
        * The variables that is projected into the inner subgroup. Typically,
        * this is identical to the join variables. There are, however, 
        * exceptions where we need to project in a superset. For instance, for
        * the query 
        * 
        * select * where {
        * ?a :knows ?b .
        *   OPTIONAL {
        *     ?b :knows ?c .
        *     ?c :knows ?d .
        *     filter(?a != :paul) # Note: filter applies to *outer* variable
        *    }
        * }
        * 
        * we have joinVars={?b} and projectInVars={?a, ?b}, because variables
        * from outside are visible in the inner filter.
        */
       String PROJECT_IN_VARS = 
           PipelinedHashIndexAndSolutionSetJoinOp.class.getName() + ".projectInVars";

       /**
        * The threshold defining when to release the distinctProjectionBuffer.
        * Note that releasing this buffer means releasing the
        * distinctProjectionBuffer at the same time.
        */
       String DISTINCT_PROJECTION_BUFFER_THRESHOLD = 
           PipelinedHashIndexAndSolutionSetJoinOp.class.getName() 
           + ".distinctProjectionBufferThreshold";
       
       // set default to have a default's chunk size default
       int DEFAULT_DISTINCT_PROJECTION_BUFFER_THRESHOLD = 50;
       

       /**
        * The threshold defining when to release the incomingBindingsBuffer.
        * Note that releasing this buffer means releasing the
        * distinctProjectionBuffer at the same time.
        */
       String INCOMING_BINDINGS_BUFFER_THRESHOLD = 
           PipelinedHashIndexAndSolutionSetJoinOp.class.getName() 
           + ".incomingBindingsBuffer";
       
       // having buffered 1000 incoming bindings, we release both buffers;
       // this might happen if we observe 1000 incoming mappings with less
       // than DISTINCT_PROJECTION_BUFFER_THRESHOLD distinct projections
       int DEFAULT_INCOMING_BINDINGS_BUFFER_THRESHOLD = 1000;

    }

    /**
      * Deep copy constructor.
      */
    public PipelinedHashIndexAndSolutionSetJoinOp(final PipelinedHashIndexAndSolutionSetJoinOp op) {
       
        super(op);

        // max parallel must be one
        if (getMaxParallel() != 1)
            throw new IllegalArgumentException(Annotations.MAX_PARALLEL + "=" + getMaxParallel());

    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public PipelinedHashIndexAndSolutionSetJoinOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);
        
        // max parallel must be one
        if (getMaxParallel() != 1)
            throw new IllegalArgumentException(Annotations.MAX_PARALLEL + "=" + getMaxParallel());

    }
    
    public PipelinedHashIndexAndSolutionSetJoinOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    protected ChunkTaskBase createChunkTask(final BOpContext<IBindingSet> context) {

        /**
         * The operator offers two ways to generate the hash index of the input
         * stream, either via subquery or via binding set that is passed in.
         * Exactly one of both *must* be provided.
         */
        final PipelineOp subquery = 
            (PipelineOp)getProperty(Annotations.SUBQUERY);
        
        final IBindingSet[] bsFromBindingsSetSource =
            (IBindingSet[]) getProperty(Annotations.BINDING_SETS_SOURCE);
        
        if (subquery==null && bsFromBindingsSetSource==null) {
            throw new IllegalArgumentException(
                "Neither subquery nor binding set source provided.");
        } else if (subquery!=null && bsFromBindingsSetSource!=null) {
            throw new IllegalArgumentException(
                "Both subquery and binding set source provided.");           
        }
        
        final IVariable<?> askVar = 
           (IVariable<?>) getProperty(HashJoinAnnotations.ASK_VAR);
        
        final IVariable<?>[] projectInVars = 
           (IVariable<?>[]) getProperty(Annotations.PROJECT_IN_VARS);
        
        final int distinctProjectionBufferThreshold = 
            getProperty(
               Annotations.DISTINCT_PROJECTION_BUFFER_THRESHOLD, 
               Annotations.DEFAULT_DISTINCT_PROJECTION_BUFFER_THRESHOLD);

        final int incomingBindingsBufferThreshold = 
              getProperty(
                 Annotations.INCOMING_BINDINGS_BUFFER_THRESHOLD, 
                 Annotations.DEFAULT_INCOMING_BINDINGS_BUFFER_THRESHOLD);
        
        return new ChunkTask(this, context, subquery, 
           bsFromBindingsSetSource, projectInVars, askVar,
           distinctProjectionBufferThreshold, incomingBindingsBufferThreshold);
        
    }
    
    /**
     * A chunk task. See outer class for explanation of parameters.
     */
    private static class ChunkTask extends com.bigdata.bop.join.HashIndexOp.ChunkTask {

        final PipelineOp subquery;

        final IBindingSet[] bsFromBindingsSetSource;
        
        final IConstraint[] joinConstraints;
        
        final IVariable<?> askVar;
        
        final IVariable<?>[] projectInVars;
        
        final int distinctProjectionBufferThreshold;
        
        final int incomingBindingsBufferThreshold;
        
        public ChunkTask(final PipelinedHashIndexAndSolutionSetJoinOp op,
                final BOpContext<IBindingSet> context, 
                final PipelineOp subquery, 
                final IBindingSet[] bsFromBindingsSetSource,
                final IVariable<?>[] projectInVars,
                final IVariable<?> askVar,
                final int distinctProjectionBufferThreshold, 
                final int incomingBindingsBufferThreshold) {

            super(op, context);
            
            joinConstraints = BOpUtility.concat(
                  (IConstraint[]) op.getProperty(Annotations.CONSTRAINTS),
                  state.getConstraints());
            
            // exactly one of the two will be non-null
            this.subquery = subquery;
            this.bsFromBindingsSetSource = bsFromBindingsSetSource;
            this.projectInVars = projectInVars;
            this.askVar = askVar;
            this.distinctProjectionBufferThreshold = distinctProjectionBufferThreshold;
            this.incomingBindingsBufferThreshold = incomingBindingsBufferThreshold;

        }
        
        /**
         * Evaluate.
         */
        @Override
        public Void call() throws Exception {

            try {

                // Buffer all source solutions.
                acceptAndOutputSolutions();

                if (context.isLastInvocation()) {

                    // Done. Release the allocation context.
                    state.release();

                }

                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            }
            
        }
        
        /**
         * Output the buffered solutions.
         */
        private void acceptAndOutputSolutions() {

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = 
               new UnsyncLocalOutputBuffer<IBindingSet>(
                 op.getChunkCapacity(), sink);

            final ICloseableIterator<IBindingSet[]> src;

            if (sourceIsPipeline) {
            
                src = context.getSource();
                
            } else if (op.getProperty(Annotations.NAMED_SET_SOURCE_REF) != null) {
                
                /*
                 * Metadata to identify the optional *source* solution set. When
                 * <code>null</code>, the hash index is built from the solutions flowing
                 * through the pipeline. When non-<code>null</code>, the hash index is
                 * built from the solutions in the identifier solution set.
                 */
                final INamedSolutionSetRef namedSetSourceRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_SOURCE_REF);

                src = context.getAlternateSource(namedSetSourceRef);
                
            } else if (bsFromBindingsSetSource != null) {

                /**
                 * We handle the BINDINGS_SETS_SOURCE case as follows: the
                 * binding sets on the source are treated as input. Given that
                 * in this case no inner query is set, we consider the
                 * BINDINGS_SETS_SOURCE as the result of the query instead.
                 * It is extracted here and passed in as a parameter.
                 */
                src = context.getSource();
                
            } else {

                throw new UnsupportedOperationException(
                        "Source was not specified");
                
            }
            
            
            ((PipelinedHashJoinUtility)state).acceptAndOutputSolutions(
               unsyncBuffer, src, stats, joinConstraints, subquery,
               bsFromBindingsSetSource, projectInVars, askVar,
               context.isLastInvocation(), distinctProjectionBufferThreshold,
               incomingBindingsBufferThreshold, context);
            
            
            unsyncBuffer.flush();

            sink.flush();

        }

    } // ControllerTask

}
