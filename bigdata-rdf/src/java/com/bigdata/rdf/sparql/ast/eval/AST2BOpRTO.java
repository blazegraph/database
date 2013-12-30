/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Dec 23, 2013
 */
package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.joinGraph.rto.JGraph;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;

/**
 * Integration with the Runtime Optimizer (RTO).
 * 
 * TODO The initial integration aims to run only queries that are simple join
 * groups with filters. Once we have this integrated so that it can be enabled
 * with a query hint, then we can look into handling subgroups, materialization,
 * etc. Even handling filters will be somewhat tricky due to the requirement for
 * conditional materialization of variable bindings in advance of certain
 * {@link IValueExpression} depending on the {@link INeedsMaterialization}
 * interface. Therefore, the place to start is with simple join groups and
 * filters whose {@link IValueExpression}s do not require materialization.
 * 
 * TODO We need a way to inspect the RTO behavior. It will get logged, but it
 * would be nice to attach it to the query plan. Likewise, it would be nice to
 * surface this to the caller so the RTO can be used to guide query construction
 * UIs.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/64">Runtime
 *      Query Optimization</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/258">Integrate
 *      RTO into SAIL</a>
 * @see <a
 *      href="http://www-db.informatik.uni-tuebingen.de/files/research/pathfinder/publications/rox-demo.pdf">
 *      ROX </a>
 * @see JoinGraph
 * @see JGraph
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class AST2BOpRTO extends AST2BOpJoins {

    /**
     * Inspect the remainder of the join group. If we can isolate a join graph
     * and filters, then we will push them down into an RTO JoinGroup. Since the
     * joins have already been ordered by the static optimizer, we can accept
     * them in sequence along with any attachable filters and (if we get at
     * least 3 predicates) push them down into an RTO join group.
     * <p>
     * Note: Two predicates in a join group is not enough for the RTO to provide
     * a different join ordering. Both the static optimizer and the RTO will
     * always choose the AP with the smaller cardinality to run first. If there
     * are only 2 predicates, then the other predicate will run second. You need
     * at least three predicates before the RTO could provide a different
     * answer.
     */
    static protected PipelineOp convertRTOJoinGraph(PipelineOp left,
            final JoinGroupNode joinGroup, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx, final AtomicInteger start) {

        final int arity = joinGroup.arity();

        // The predicates for the RTO join group.
        final Set<StatementPatternNode> sps = new LinkedHashSet<StatementPatternNode>();
        @SuppressWarnings("rawtypes")
        final Set<Predicate> preds = new LinkedHashSet<Predicate>();
        final List<IConstraint> constraints = new LinkedList<IConstraint>();

        // Examine the remaining joins, stopping at the first non-SP.
        for (int i = start.get(); i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) joinGroup
                    .get(i);

            if (child instanceof StatementPatternNode) {
                // SP
                final StatementPatternNode sp = (StatementPatternNode) child;
                final boolean optional = sp.isOptional();
                if(optional) {
                    /*
                     * TODO Handle optional SPs in joinGraph (by ordering them
                     * in the tail so as to minimize the cost function).
                     */
                    break;
                }
                
                final List<IConstraint> attachedConstraints = getJoinConstraints(sp);
                
                @SuppressWarnings("rawtypes")
                final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization =
                        new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

                getJoinConstraints(attachedConstraints, needsMaterialization);
                
                if (!needsMaterialization.isEmpty()) {
                    /*
                     * At least one variable requires (or might require)
                     * materialization. This is not currently handled by
                     * the RTO so we break out of the loop.
                     * 
                     * TODO Handle materialization patterns within the RTO.
                     */
                    break;
                }
                
//                // Add constraints to the join for that predicate.
//                anns.add(new NV(JoinAnnotations.CONSTRAINTS, getJoinConstraints(
//                        constraints, needsMaterialization)));

//                /*
//                 * Pull off annotations before we clear them from the predicate.
//                 */
//                final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);
//
//                // true iff this is a quads access path.
//                final boolean quads = pred.getProperty(Annotations.QUADS,
//                        Annotations.DEFAULT_QUADS);
//
//                // pull of the Sesame dataset before we strip the annotations.
//                final DatasetNode dataset = (DatasetNode) pred
//                        .getProperty(Annotations.DATASET);

                // Something the RTO can handle.
                sps.add(sp);
                /*
                 * FIXME Handle Triples vs Quads, Default vs Named Graph, and
                 * DataSet. This probably means pushing more logic down into 
                 * the RTO from AST2BOpJoins.
                 */
                final Predicate<?> pred = AST2BOpUtility.toPredicate(sp, ctx);
//              final int joinId = ctx.nextId();
//
//              // annotations for this join.
//              final List<NV> anns = new LinkedList<NV>();
//
//              anns.add(new NV(BOp.Annotations.BOP_ID, joinId));
                preds.add(pred);
                if (attachedConstraints != null) {
                    // RTO will figure out where to attach these constraints.
                    constraints.addAll(attachedConstraints);
                }

            } else {
                // Non-SP.
                break;
            }

        }

        if (sps.size() < 3) {

            /*
             * There are not enough joins for the RTO.
             * 
             * TODO For incremental query construction UIs, it would be useful
             * to run just the RTO and to run it with even a single join. This
             * will give us sample values as well as estimates cardinalities. If
             * the UI has triple patterns that do not join (yet), then those
             * should be grouped.
             */
            return left;

        }

        /*
         * Figure out which variables are projected out of the RTO.
         * 
         * TODO This should only include things that are not reused later in the
         * query.
         */
        final Set<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        {
            
            for (StatementPatternNode sp : sps) {

                // Note: recursive only matters for complex nodes, not SPs.
                ctx.sa.getDefinitelyProducedBindings(sp, selectVars, true/* recursive */);
                
            }
            
        }
        
        /*
         * FIXME When running the RTO as anything other than the top-level join
         * group in the query plan and for the *FIRST* joins in the query plan,
         * we need to flow in any solutions that are already in the pipeline
         * (unless we are going to run the RTO "bottom up") and build a hash
         * index. When the hash index is ready, we can execute the join group.
         */
        final SampleType sampleType = joinGroup.getProperty(
                QueryHints.RTO_SAMPLE_TYPE, QueryHints.DEFAULT_RTO_SAMPLE_TYPE);
        left = new JoinGraph(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(BOp.Annotations.CONTROLLER, true),// Drop "CONTROLLER" annotation?
                // new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                // new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                new NV(JoinGraph.Annotations.SELECTED, selectVars
                        .toArray(new IVariable[selectVars.size()])),//
                new NV(JoinGraph.Annotations.VERTICES,
                        preds.toArray(new Predicate[preds.size()])),//
                new NV(JoinGraph.Annotations.CONSTRAINTS, constraints
                        .toArray(new IConstraint[constraints.size()])),//
                new NV(JoinGraph.Annotations.LIMIT,
                        JoinGraph.Annotations.DEFAULT_LIMIT),//
                new NV(JoinGraph.Annotations.NEDGES,
                        JoinGraph.Annotations.DEFAULT_NEDGES),//
                new NV(JoinGraph.Annotations.SAMPLE_TYPE, sampleType.name())//
        );

        // These joins were consumed.
        start.addAndGet(sps.size());

        return left;
        
    }
    
}
