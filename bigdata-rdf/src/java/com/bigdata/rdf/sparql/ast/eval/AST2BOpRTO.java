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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BadBOpIdTypeException;
import com.bigdata.bop.DuplicateBOpIdException;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NoBOpIdException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.joinGraph.rto.JGraph;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.bop.joinGraph.rto.Path;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.NT;

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

    public interface Annotations extends AST2BOpJoins.Annotations {
        
        /**
         * Annotation is used to tag the {@link StatementPatternNode}s in a
         * {@link JoinGroupNode} with the identified assigned to the
         * corresponding {@link IPredicate}. This makes it possible to lookup
         * the SPO from the {@link IPredicate} when the RTO hands us back an
         * ordered join path.
         */
        String PREDICATE_ID = "PredicateId";
        
    }
    
    /**
     * When <code>true</code>, the RTO will only accept simple joins into the
     * join graph. Simple joins includes triples-mode joins and filters that do
     * not have materialization requirements. Non-simple joins includes
     * quads-mode joins and filters that have materialization contexts.
     * 
     * TODO Eventually we can drop this. It is being used while we refactor the
     * RTO to support more complex joins and variable materialization for
     * filters.
     */
    static private final boolean onlySimpleJoins = false;
    
    /**
     * When <code>true</code>, the RTO will not accept OPTIONAL joins into the
     * join graph. Optional joins can not decrease the intermediate cardinality
     * since they can not eliminate solutions, but they can increase the
     * intermediate cardinality by finding multiple bindings in the OPTIONAL
     * join. Therefore, it makes sense to execute optional in an order that
     * defers as long as possible any increase in the intermediate cardinality.
     * This is very much the same as ordering required joins, but the OPTIONAL
     * joins need to be handled AFTER the required joins.
     * 
     * TODO RTO OPTIONALS: Handle optional SPs in joinGraph by ordering them in
     * the tail so as to minimize the cost function. Once implemented, we can
     * drop this field.
     */
    static private final boolean onlyRequiredJoins = true;
    
    /**
     * When <code>true</code>, the RTO will only accept statement patterns into
     * the join graph. When <code>false</code>, it will attempt to handle non-SP
     * joins, e.g., sub-query, exists, property paths, etc.
     * 
     * TODO Eventually we can drop this.
     */
    static private final boolean onlySPs = true;
    
    /**
     * When <code>true</code>, the RTO will be applied as in we were doing
     * bottom-up query optimization. In this case, it WILL NOT receive any
     * solutions from the upstream operators in the pipeline when it performs
     * its runtime sampling and it will ignore the <code>doneSet</code> for the
     * context in which it is invoked. When run in this manner, the RTO *could*
     * be run before the main query is executed. The only way to facilitate this
     * at this time would be to lift out the joins on which the RTO would be run
     * into a named subquery and then optimize that named subquery before the
     * rest of the query.
     * <p>
     * When <code>false</code>, the RTO solutions from upstream operators will
     * flow into the RTO.
     * 
     * TODO We could still pass in exogenous solutions for bottom up evaluation.
     * This would help constraint the RTOs exploration.
     * 
     * TODO The RTO is not operating 100% in either an left-to-right or a
     * bottom-up fashion, primarily because we are not passing in either
     * exogenous bindings or meaningfully using the bindings from the upstream
     * operator when exploring the join graph. In fact, the RTO could accept a
     * new sample from the upstream operator in each iteration drawing from
     * amoung those solutions which had already been materialized by the
     * upstream operator.
     */
    static private final boolean bottomUp = true;
    
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

        /*
         * Snapshot of the doneSet on entry. This gets passed into the RTO.
         */
        final Set<IVariable<?>> doneSetIn = Collections
                .unmodifiableSet(doneSet);

        if (onlySimpleJoins && ctx.isQuads()) {

            return left;

        }

        /*
         * Consider the join group. See if it is complex enough to warrant
         * running the RTO.
         * 
         * TODO Can we also make a decision based on whether there is uncertain,
         * e.g., a swing in stakes, about the cardinality estimates for the
         * predicates in the join graph, etc.? This could give us a means to
         * avoid using the RTO if the join graph is known to run quickly or the
         * ordering of the joins generated by the static query optimizer is
         * known to be good.
         * 
         * TODO The static optimizer could simply annotation join groups for
         * which it recognizes that it could have a bad join plan.
         */
        final int arity = joinGroup.arity();

        /*
         * Create a JoinGroup just for the pieces that the RTO will handle.
         * 
         * FIXME These joins are CLONED right now since they also exist in the
         * caller's joinGroup. Eventually, this code should be moved into an AST
         * rewrite and we can then destructively move the children from the
         * original JoinGroupNode into a JoinGraphNode that will be the AST side
         * of the RTO.
         */
        final JoinGroupNode rtoJoinGroup = new JoinGroupNode();
        rtoJoinGroup.setQueryHints(joinGroup.getQueryHints());
        
//        final Set<StatementPatternNode> sps = new LinkedHashSet<StatementPatternNode>();
        // The predicates for the join graph.
        @SuppressWarnings("rawtypes")
        final LinkedList<Predicate> preds = new LinkedList<Predicate>();
        // The constraints for the join graph.
        final List<IConstraint> constraints = new LinkedList<IConstraint>();
        // The #of JOINs accepted into the RTO's join group.
        int naccepted = 0;
        {
            /*
             * The [doneSet] will be modified iff we actually accept JOINs into the
             * RTO. Therefore, we also make a temporary copy that we will use to
             * avoid side-effects if this join group is not complex enough to run
             * the RTO.
             */
            final Set<IVariable<?>> doneSetTmp = new LinkedHashSet<IVariable<?>>(
                    doneSet);
            
            // Examine the remaining joins, stopping at the first non-SP.
            for (int i = start.get(); i < arity; i++) {
    
                final IGroupMemberNode child = (IGroupMemberNode) joinGroup
                        .get(i);
    
                if (child instanceof StatementPatternNode) {
                    // SP
                    StatementPatternNode sp = (StatementPatternNode) child;
                    final boolean optional = sp.isOptional();
                    if (onlyRequiredJoins && optional) {
                        /*
                         * TODO The RTO does not order optional joins yet so we
                         * can not accept this join into the join graph.
                         */
                        break;
                    }
                    
                    final List<IConstraint> attachedConstraints = getJoinConstraints(sp);
                    
                    @SuppressWarnings("rawtypes")
                    final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization =
                            new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();
    
                    getJoinConstraints(attachedConstraints, needsMaterialization);
                    
                    if (onlySimpleJoins && !needsMaterialization.isEmpty()) {
                        /*
                         * At least one variable requires (or might require)
                         * materialization. This is not currently handled by the RTO
                         * so we break out of the loop.
                         * 
                         * TODO Handle materialization patterns within the RTO, in
                         * which case we need to collect up the doneSet here, but
                         * only conditionally since we might not actually execute the
                         * RTO depending on the number of SPs that we find.
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
                    sp = (StatementPatternNode) sp.clone();// TODO Use destructive move.
                    rtoJoinGroup.addChild(sp); // add to group.
                    naccepted++;
                    /*
                     * FIXME RTO: Handle Triples vs Quads, Default vs Named
                     * Graph, and DataSet. This probably means pushing more
                     * logic down into the RTO from AST2BOpJoins.
                     * Path.cutoffJoin() will need to be call through to logic
                     * on this class that does the right thing with named graph
                     * joins, default graph joins, triples mode joins, remote AP
                     * joins, etc. This is the same code that we call through to
                     * when we take the selected join path from the RTO and
                     * compile it into a query plan to fully execute the join
                     * group.
                     * 
                     * Note: This assigns ids to the predicates as a
                     * side-effect. Those ids are assigned by the
                     * AST2BOpContext's BOpIdFactory. You can not rely on
                     * specific ID values being assigned, so you need to build a
                     * map and track the correspondence between the SPs and the
                     * predicates.
                     */
                    final Predicate<?> pred = AST2BOpUtility.toPredicate(sp,
                            ctx);
                    preds.add(pred);
                    // tag the SP with predicate's ID.
                    sp.setProperty(Annotations.PREDICATE_ID, pred.getId());
                    if (attachedConstraints != null) {
                        /*
                         * The RTO will figure out where to attach these
                         * constraints.
                         */
                        constraints.addAll(attachedConstraints);
                    }
    
                } else {
                    // Non-SP.
                    if (onlySPs)
                        break;
                    /*
                     * TODO Handle non-SPs in the RTO. See convertJoinGroup()
                     * for how we handle non-SPs during normal query plan
                     * conversion. All of that would also have to be handled
                     * here for the RTO to allow in non-SPs.
                     */
                    throw new UnsupportedOperationException();
                }
    
            }

            if (naccepted < 3) {

                /*
                 * There are not enough joins for the RTO.
                 * 
                 * TODO For incremental query construction UIs, it would be
                 * useful to run just the RTO and to run it with even a single
                 * join. This will give us sample values as well as estimates
                 * cardinalities. If the UI has triple patterns that do not join
                 * (yet), then those should be grouped.
                 */
                return left;

            }

            /*
             * Since we will run the RTO, we now record any variables that are
             * known to be materialized in order to support the FILTERs
             * associated with the join group that we feed into the RTO.
             */
            doneSet.addAll(doneSetTmp);

        }
        
        /*
         * Figure out which variables are projected out of the RTO.
         * 
         * TODO This should only include things that are not reused later in the
         * query.
         */
        final Set<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        {
            
            for (IGroupMemberNode child : rtoJoinGroup.getChildren()) {

                if (!(child instanceof IBindingProducerNode))
                    continue;

                // Note: recursive only matters for complex nodes, not SPs.
                ctx.sa.getDefinitelyProducedBindings(
                        (IBindingProducerNode) child, selectVars, true/* recursive */);

            }
            
        }
        
        /*
         * FIXME RTO: When running the RTO as anything other than the top-level join
         * group in the query plan and for the *FIRST* joins in the query plan,
         * we need to flow in any solutions that are already in the pipeline
         * (unless we are going to run the RTO "bottom up") and build a hash
         * index. When the hash index is ready, we can execute the join group.
         */

        final SampleType sampleType = joinGroup.getProperty(
                QueryHints.RTO_SAMPLE_TYPE, QueryHints.DEFAULT_RTO_SAMPLE_TYPE);
        
        final int limit = joinGroup.getProperty(QueryHints.RTO_LIMIT,
                QueryHints.DEFAULT_RTO_LIMIT);
        
        final int nedges = joinGroup.getProperty(QueryHints.RTO_NEDGES,
                QueryHints.DEFAULT_RTO_NEDGES);
        
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
                new NV(JoinGraph.Annotations.JOIN_GROUP, rtoJoinGroup),//
                new NV(JoinGraph.Annotations.LIMIT, limit),//
                new NV(JoinGraph.Annotations.NEDGES, nedges),//
                new NV(JoinGraph.Annotations.SAMPLE_TYPE, sampleType.name()),//
                new NV(JoinGraph.Annotations.DONE_SET, doneSetIn),//
                new NV(JoinGraph.Annotations.NT, new NT(ctx.getNamespace(),
                        ctx.getTimestamp()))//
        );

        // These joins were consumed.
        start.addAndGet(naccepted);

        return left;
        
    }

    /**
     * Compile a join graph into a query plan.
     * 
     * @param queryEngine
     *            The {@link QueryEngine} on which the RTO has been executing
     *            and on which the returned query plan may be executed.
     * @param joinGraph
     *            The operator that executed the RTO.
     * @param path
     *            The join path that was selected for full execution by the RTO
     *            based on deep sampling of the join graph.
     * 
     * @return The query plan to fully execute that join graph.
     */
    public static PipelineOp compileJoinGraph(final QueryEngine queryEngine,
            final JoinGraph joinGraph, final Path path) {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        
        if (joinGraph == null)
            throw new IllegalArgumentException();

        if (path == null)
            throw new IllegalArgumentException();

        final IVariable<?>[] selected = joinGraph.getSelected();

        final IPredicate<?>[] predicates = path.getPredicates();

        final IConstraint[] constraints = joinGraph.getConstraints();
        
        if (onlySimpleJoins) {

            /*
             * This is the old code. It does not handle variable materialization
             * for filters.
             */

            // Factory avoids reuse of bopIds assigned to the predicates.
            final BOpIdFactory idFactory = new BOpIdFactory();

            return PartitionedJoinGroup.getQuery(idFactory,
                    false/* distinct */, selected, predicates, constraints);

        }

        /*
         * TODO The RTO is ignoring the doneSet so it always runs all
         * materialization steps even if some variable is known to be
         * materialized on entry.
         */
        final Set<IVariable<?>> doneSet = joinGraph.getDoneSet();

        /*
         * The AST JoinGroupNode for the joins and filters that we are running
         * through the RTO.
         */
        final JoinGroupNode rtoJoinGroup = (JoinGroupNode) joinGraph
                .getRequiredProperty(JoinGraph.Annotations.JOIN_GROUP);

        // Build an index over the bopIds in that JoinGroupNode.
        final Map<Integer, StatementPatternNode> index = getIndex(rtoJoinGroup);

        // Factory avoids reuse of bopIds assigned to the predicates.
        final BOpIdFactory idFactory = new BOpIdFactory();

        /*
         * Reserve ids used by the join graph or its constraints.
         */
        idFactory.reserveIds(predicates, constraints);

        /*
         * Figure out which constraints are attached to which predicates.
         * 
         * TODO Can we reconcile this code with the filter assignment code in
         * AST2BOpFilter? If so, then we can get rid of the
         * PartitionedJoinGroup.
         */
        final IConstraint[][] assignedConstraints = PartitionedJoinGroup
                .getJoinGraphConstraints(predicates, constraints,
                        null/* knownBound */, true/* pathIsComplete */);

        // Create an execution context for the query.
        final AST2BOpContext ctx = getExecutionContext(queryEngine,
                // Identifies the KB instance (namespace and timestamp).
                (NT) joinGraph.getRequiredProperty(JoinGraph.Annotations.NT));

        // Start with an empty plan.
        PipelineOp left = null;

        for (int i = 0; i < predicates.length; i++) {

            final Predicate<?> pred = (Predicate<?>) predicates[i];

            final IConstraint[] attachedJoinConstraints = assignedConstraints[i];

            final boolean optional = pred.isOptional();

            // Lookup the AST node for that predicate.
            final StatementPatternNode sp = index.get(pred.getId());
            
            left = join(left, //
                    pred, //
                    optional ? new LinkedHashSet<IVariable<?>>(doneSet)
                            : doneSet, //
                    attachedJoinConstraints == null ? null : Arrays
                            .asList(attachedJoinConstraints),//
                    sp.getQueryHints(),//
                    ctx);

        }

        if (selected != null && selected.length != 0) {

            // Drop variables that are not projected out.
            left = applyQueryHints(new ProjectionOp(//
                    leftOrEmpty(left), //
                    new NV(ProjectionOp.Annotations.BOP_ID, idFactory.nextId()),//
                    new NV(ProjectionOp.Annotations.SELECT, selected)//
                    ), rtoJoinGroup, ctx);

        }
        
        return left;

    }

    /**
     * Return an execution context that may be used to execute a cutoff join
     * during sampling or the entire join path once it has been identified.
     * 
     * @param queryEngine
     *            The query engine on which the RTO is executing.
     * @param nt
     *            The namespace and timestamp of the KB view against which the
     *            RTO is running.
     * 
     * @throws RuntimeException
     *             if there is no such KB instance.
     */
    private static AST2BOpContext getExecutionContext(
            final QueryEngine queryEngine, final NT nt) {

        // The index manager that can be used to resolve the KB view.
        final IIndexManager indexManager = queryEngine.getFederation() == null ? queryEngine
                .getIndexManager() : queryEngine.getFederation();

        // Resolve the KB instance.
        final AbstractTripleStore db = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(nt.getName(), nt.getTimestamp());

        if (db == null)
            throw new RuntimeException("No such KB? " + nt);

        /*
         * An empty container. You can not use any of the top-level SPARQL query
         * conversion routines with this container, but it is enough for the
         * low-level things that we need to run the RTO.
         */
        final ASTContainer astContainer = new ASTContainer(BOp.NOARGS,
                BOp.NOANNS);
        
        return new AST2BOpContext(astContainer, db);

    }

    /**
     * Return a map from the {@link Annotations#PREDICATE_ID} to the
     * corresponding {@link StatementPatternNode}.
     * 
     * @param op
     *            The join group.
     * 
     * @return The index, which is immutable and thread-safe.
     * 
     * @throws DuplicateBOpIdException
     *             if there are two or more {@link BOp}s having the same
     *             {@link Annotations#PREDICATE_ID}.
     * @throws BadBOpIdTypeException
     *             if the {@link Annotations#PREDICATE_ID} is not an
     *             {@link Integer}.
     * @throws NoBOpIdException
     *             if a {@link StatementPatternNode} does not have a
     *             {@link Annotations#PREDICATE_ID}.
     */
    static private Map<Integer, StatementPatternNode> getIndex(
            final JoinGroupNode op) {
        if (op == null)
            throw new IllegalArgumentException();
        final LinkedHashMap<Integer, StatementPatternNode> map = new LinkedHashMap<Integer, StatementPatternNode>();
        final Iterator<IGroupMemberNode> itr = op.iterator();
        while (itr.hasNext()) {
            final BOp t = itr.next();
            if(!(t instanceof StatementPatternNode)) {
                // Skip non-SP nodes.
                continue;
            }
            final StatementPatternNode sp = (StatementPatternNode) t;
            final Object x = t.getProperty(Annotations.PREDICATE_ID);
            if (x == null) {
                throw new NoBOpIdException(t.toString());
            }
            if (!(x instanceof Integer)) {
                throw new BadBOpIdTypeException("Must be Integer, not: "
                        + x.getClass() + ": " + Annotations.PREDICATE_ID);
            }
            final Integer id = (Integer) x;
            final BOp conflict = map.put(id, sp);
            if (conflict != null) {
                /*
                 * BOp appears more than once. This is not allowed for
                 * pipeline operators. If you are getting this exception for
                 * a non-pipeline operator, you should remove the bopId.
                 */
                throw new DuplicateBOpIdException("duplicate id=" + id
                        + " for " + conflict + " and " + t);
            }
        }
        // wrap to ensure immutable and thread-safe.
        return Collections.unmodifiableMap(map);
    }

}
