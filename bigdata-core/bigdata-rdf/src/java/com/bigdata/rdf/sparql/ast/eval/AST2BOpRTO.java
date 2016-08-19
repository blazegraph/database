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
 * Created on Dec 23, 2013
 */
package com.bigdata.rdf.sparql.ast.eval;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BadBOpIdTypeException;
import com.bigdata.bop.Constant;
import com.bigdata.bop.DuplicateBOpIdException;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NoBOpIdException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoinStats;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.joinGraph.rto.EdgeSample;
import com.bigdata.bop.joinGraph.rto.EstimateEnum;
import com.bigdata.bop.joinGraph.rto.JGraph;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.bop.joinGraph.rto.Path;
import com.bigdata.bop.joinGraph.rto.SampleBase;
import com.bigdata.bop.joinGraph.rto.VertexSample;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.util.NT;

/**
 * Integration with the Runtime Optimizer (RTO).
 * <p>
 * Note: The RTO currently uses bottom-up evaluation to solve the join graph and
 * generate a sub-query plan with an optimized join ordering. It uses
 * left-to-right evaluation to pass pipeline solutions through the optimized
 * subquery.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/64">Runtime
 *      Query Optimization</a>
 * 
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

    public static final transient Logger log = Logger
            .getLogger(AST2BOpRTO.class);

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
     * The RTO is most useful for queries with high latency and a large number
     * of joins. You do not need to apply the RTO if there is only a single
     * triple pattern since there are no joins. You do not need to apply the RTO
     * if there are two triple patterns since we will run the most selective one
     * first. The RTO only starts to be useful when there are at least three
     * triple patterns - that gives you three vertices and two joins.
     * 
     * TODO For incremental query construction UIs, it would be useful to run
     * just the RTO and to run it with even a single join. This will give us
     * sample values as well as estimates cardinalities. If the UI has triple
     * patterns that do not join (yet), then those should be grouped. This could
     * be done if we provided a simple SPARQL query with a query hint to run the
     * RTO, trapped the {@link IRunningQuery} to get the detailed RTO results,
     * and specified the minimum threashold for running the RTO for that query
     * as TWO joins, basically just overriding this field.
     */
    static private final int RTO_MIN_JOINS = 3;
    
    /**
     * When <code>true</code>, the RTO will only accept simple joins into the
     * join graph. Simple joins includes triples-mode joins and filters that do
     * not have materialization requirements. Non-simple joins includes
     * quads-mode joins and filters that have materialization contexts.
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
     * FIXME RTO: OPTIONALS: Handle optional SPs in joinGraph by ordering them
     * in the tail so as to minimize the cost function. Once implemented, we can
     * drop this field.
     */
    static private final boolean onlyRequiredJoins = true;
    
    /**
     * When <code>true</code>, the RTO will only accept statement patterns into
     * the join graph. When <code>false</code>, it will attempt to handle non-SP
     * joins, e.g., sub-query, exists, property paths, etc.
     */
    static private final boolean onlySPs = true;
    
    /**
     * When <code>true</code>, even simple JOINs will run through the code path
     * for the evaluation of complex joins.
     * <p>
     * Note: The complex join evaluation code is written to also allow
     * evaluation of a simple JOIN. This makes it easier to test this code path
     * by testing it against a single {@link PipelineJoin} and comparing with
     * the historical behavior for cutoff evaluation of the {@link PipelineJoin}
     * . However, that only works if you also disable the reordering of access
     * paths in the JOIN.
     * <p>
     * Note: access path reordering is allowed for simple JOINS when we use
     * {@link #runSimpleJoin(QueryEngine, SampleBase, int, PipelineJoin)}.
     * 
     * @see PipelineJoin.Annotations#REORDER_ACCESS_PATHS
     * @see AST2BOpJoins
     * @see #runSimpleJoin(QueryEngine, SampleBase, int, PipelineJoin)
     * @see #runComplexJoin(QueryEngine, SampleBase, int, PipelineOp)
     * 
     *      TODO RTO: Measure performance when using the complex join code path
     *      as opposed to the simple join code path. If there is a performance
     *      win for the simple join code path, then set this to false in
     *      committed code. If not, then we might as well run everything in the
     *      same fashion.
     *      <p>
     *      Note: The simple and complex cutoff join code do currently differ
     *      somewhat in the end result that they produce. This can be observed
     *      in BSBM Q1 on the pc100 data set and the BAR query.
     */
    static final boolean runAllJoinsAsComplexJoins = false;

    /**
     * When <code>true</code>, out of order evaluation will cause the RTO to
     * fail. When <code>false</code>, out of order evaluation is silently
     * ignored.
     * <p>
     * Out of order evaluation makes it impossible to accurately determine the
     * estimated cardinality of the join since we can not compute the join hit
     * ratio without knowing the #of solutions in required to produce a given
     * #of solutions out.
     * <p>
     * There are several possible root causes for out of order evaluation. One
     * is reordering of access paths in the pipeline join. Another is reordering
     * of solutions when they are output from an operator. Another is an
     * operator which uses the altSink and thus does not always route solutions
     * along a single path.
     * 
     * @see #checkQueryPlans
     */
    static final private boolean failOutOfOrderEvaluation = true;
    
    /**
     * When <code>true</code>, the generated query plans for cutoff evaluation
     * will be checked to verify that the query plans do not permit reordering
     * of solutions.
     * 
     * @see #failOutOfOrderEvaluation
     */
    static final private boolean checkQueryPlans = false; // Note: Make [false] in committed code!
    
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
         * TODO The static optimizer could simply annotate join groups for which
         * it recognizes that it could have a bad join plan.
         */
        final int arity = joinGroup.arity();

        /*
         * Create a JoinGroup just for the pieces that the RTO will handle.
         * 
         * TODO These joins are CLONED right now since they also exist in the
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
                        /* Do not include the OPTTIONAL join in the join graph. */
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
                         * materialization.
                         */
                        break;
                    }

                    // Something the RTO can handle.
                    sp = (StatementPatternNode) sp.clone();// TODO Use destructive move.
                    rtoJoinGroup.addChild(sp); // add to group.
                    naccepted++;
                    /*
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

            if (naccepted < RTO_MIN_JOINS) {

                // There are not enough joins for the RTO.
                return left;

            }

            /*
             * Since we will run the RTO, we now record any variables that are
             * known to be materialized in order to support the FILTERs
             * associated with the join group that we feed into the RTO.
             */
            doneSet.addAll(doneSetTmp);

        }
        
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
//                new NV(JoinGraph.Annotations.SELECTED, selectVars
//                        .toArray(new IVariable[selectVars.size()])),//
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
     * 
     *         FIXME RTO: Modify this to use AST2BOpUtility#convertJoinGroup().
     *         We would have to reorder the joins in the
     *         {@link JoinGraph.Annotations#JOIN_GROUP} into the ordering
     *         specified in the {@link Path} and make sure that
     *         convertJoinGroup() did not attempt to recursively reapply the
     *         RTO. This will get rid of one of the few remaining uses of
     *         {@link PartitionedJoinGroup}.
     */
    public static PipelineOp compileJoinGraph(final QueryEngine queryEngine,
            final JoinGraph joinGraph, final Path path) {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        
        if (joinGraph == null)
            throw new IllegalArgumentException();

        if (path == null)
            throw new IllegalArgumentException();

//        final IVariable<?>[] selected = joinGraph.getSelected();

        final IPredicate<?>[] predicates = path.getPredicates();

        final IConstraint[] constraints = joinGraph.getConstraints();

        final Set<IVariable<?>> doneSet = new LinkedHashSet<IVariable<?>>(
                joinGraph.getDoneSet());

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

        // Reserve ids used by the vertices of the join graph.
        idFactory.reserveIds(predicates);

        // Reserve ids used by the constraints on the join graph.
        idFactory.reserveIds(constraints);

        /*
         * Figure out which constraints are attached to which predicates.
         */
        final IConstraint[][] constraintAttachmentArray = PartitionedJoinGroup
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

            final IConstraint[] attachedJoinConstraints = constraintAttachmentArray[i];

            final boolean optional = pred.isOptional();

            // Lookup the AST node for that predicate.
            final StatementPatternNode sp = index.get(pred.getId());
            
            left = join(left, //
                    pred, //
                    optional ? new LinkedHashSet<IVariable<?>>(doneSet)
                            : doneSet, //
                    attachedJoinConstraints == null ? null : Arrays
                            .asList(attachedJoinConstraints),//
                    null, // cutoff join limit
                    sp.getQueryHints(),//
                    ctx);

        }

//        if (selected != null && selected.length != 0) {
//
//            // Drop variables that are not projected out.
//            left = applyQueryHints(new ProjectionOp(//
//                    leftOrEmpty(left), //
//                    new NV(ProjectionOp.Annotations.BOP_ID, idFactory.nextId()),//
//                    new NV(ProjectionOp.Annotations.SELECT, selected)//
//                    ), rtoJoinGroup, ctx);
//
//        }
        
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

    /**
     * Cutoff join of the last vertex in the join path.
     * <p>
     * <strong>The caller is responsible for protecting against needless
     * re-sampling.</strong> This includes cases where a sample already exists
     * at the desired sample limit and cases where the sample is already exact.
     * 
     * @param queryEngine
     *            The query engine.
     * @param joinGraph
     *            The pipeline operator that is executing the RTO. This defines
     *            the join graph (vertices, edges, and constraints) and also
     *            provides access to the AST and related metadata required to
     *            execute the join graph.
     * @param limit
     *            The limit for the cutoff join.
     * @param predicates
     *            The path segment, which must include the target vertex as the
     *            last component of the path segment.
     * @param constraints
     *            The constraints declared for the join graph (if any). The
     *            appropriate constraints will be applied based on the variables
     *            which are known to be bound as of the cutoff join for the last
     *            vertex in the path segment.
     * @param pathIsComplete
     *            <code>true</code> iff all vertices in the join graph are
     *            incorporated into this path.
     * @param sourceSample
     *            The input sample for the cutoff join. When this is a one-step
     *            estimation of the cardinality of the edge, then this sample is
     *            taken from the {@link VertexSample}. When the edge (vSource,
     *            vTarget) extends some {@link Path}, then this is taken from
     *            the {@link EdgeSample} for that {@link Path}.
     * 
     * @return The result of sampling that edge.
     */
    //synchronized// FIXME REMOVE synchronized. This forces single threading for debugging purposes when chasing out-of-order evaluation exceptions.
    static public EdgeSample cutoffJoin(//
            final QueryEngine queryEngine,//
            final JoinGraph joinGraph,//
            final int limit,//
            final IPredicate<?>[] predicates,//
            final IConstraint[] constraints,//
            final boolean pathIsComplete,//
            final SampleBase sourceSample//
    ) throws Exception {

        if (predicates == null)
            throw new IllegalArgumentException();

        if (limit <= 0)
            throw new IllegalArgumentException();

        // The access path on which the cutoff join will read.
        final IPredicate<?> pred = predicates[predicates.length - 1];

        if (pred == null)
            throw new IllegalArgumentException();

        if (sourceSample == null)
            throw new IllegalArgumentException();

        if (sourceSample.getSample() == null)
            throw new IllegalArgumentException();

        PipelineOp query = null;
        try {

            /*
             * Generate the query plan for cutoff evaluation of that JOIN. The
             * complexity of the query plan depends on whether or not there are
             * FILTERs "attached" to the join that have variable materialization
             * requirements.
             */
            query = getCutoffJoinQuery(queryEngine, joinGraph,
                    limit, predicates, constraints, pathIsComplete, sourceSample);

            if (!runAllJoinsAsComplexJoins && (query instanceof PipelineJoin)
                    && query.arity() == 0) {

                /*
                 * Simple JOIN.
                 * 
                 * Old logic for query execution. Relies on the ability of
                 * the PipelineJoin
                 */
                return runSimpleJoin(queryEngine, sourceSample, limit,
                        (PipelineJoin<?>) query);

            } else {

                /*
                 * Complex JOIN involving variable materialization, conditional
                 * routing operators, filters, and a SLICE to limit the output.
                 */

                return runComplexJoin(queryEngine, sourceSample, limit, query);

            }
        } catch (Throwable ex) {

            /*
             * Add some more information. This gives us the specific predicate.
             * However, it does not tell us the constraints that were attached
             * to that predicate. That information is only available when we are
             * compiling the query plan. At this point, the constraints have
             * been turned into query plan operators.
             */

            throw new RuntimeException("cause=" + ex + "\npred="
                    + BOpUtility.toString(pred) + "\nconstraints="
                    + Arrays.toString(constraints) + (query==null?"":"\nquery="
                    + BOpUtility.toString(query)), ex);

        }

    }

    /**
     * Implementation handles the general case. The general case can result in a
     * physical query plan consisting of multiple operators, including variable
     * materialization requirements, JOINs with remote access paths, etc. The
     * logic for complex joins is handled by {@link AST2BOpJoins} and
     * {@link AST2BOpFilters}s.
     * <p>
     * In the general case, the generated physical query plan has multiple
     * operators and needs to be submitted to the {@link QueryEngine} for
     * evaluation. In order to respect the semantics of cutoff evaluation, the
     * first operator must use an index, e.g., a {@link PipelineJoin}. The
     * remaining operators should not increase the cardinality, e.g., a FILTER
     * never increases the cardinality of the JOIN to which it is attached, but
     * there can be hidden JOINs, notably the {@link ChunkedMaterializationOp}
     * which is basically a JOIN against the dictionary indices.
     * 
     * @see AST2BOpJoins#join(PipelineOp, Predicate, Set, Collection,
     *      Properties, AST2BOpContext)
     */
    private static PipelineOp getCutoffJoinQuery(//
            final QueryEngine queryEngine,//
            final JoinGraph joinGraph,//
            final int limit,//
            final IPredicate<?>[] predicates,//
            final IConstraint[] constraints,//
            final boolean pathIsComplete,//
            final SampleBase sourceSample//
    ) throws Exception {

        // Note: Arguments are checked by caller : cutoffJoin().
        
        /*
         * Create an execution context for the query.
         */
        final AST2BOpContext ctx = getExecutionContext(queryEngine,
                // Identifies the KB instance (namespace and timestamp).
                (NT) joinGraph.getRequiredProperty(JoinGraph.Annotations.NT));

        // Figure out which constraints attach to each predicate.
        // TODO RTO: Replace with StaticAnalysis.
        final IConstraint[][] constraintAttachmentArray = PartitionedJoinGroup
                .getJoinGraphConstraints(predicates, constraints,
                        null/* knownBound */, pathIsComplete);

        // The constraint(s) (if any) for this join.
        final IConstraint[] c = constraintAttachmentArray[predicates.length - 1];

        // The access path on which the cutoff join will read.
        @SuppressWarnings("rawtypes")
        final Predicate pred = (Predicate) predicates[predicates.length - 1];

        // The constraints attached to that join.
        final IConstraint[] attachedJoinConstraints = constraintAttachmentArray[predicates.length - 1];

        /*
         * The AST JoinGroupNode for the joins and filters that we are running
         * through the RTO.
         */
        final JoinGroupNode rtoJoinGroup = (JoinGroupNode) joinGraph
                .getRequiredProperty(JoinGraph.Annotations.JOIN_GROUP);

        // Build an index over the bopIds in that JoinGroupNode.
        final Map<Integer, StatementPatternNode> index = getIndex(rtoJoinGroup);
        
        // Lookup the AST node for that predicate.
        final StatementPatternNode sp = index.get(pred.getId());

        /*
         * Setup factory for bopIds with reservations for ones already in use.
         */
        final BOpIdFactory idFactory = new BOpIdFactory();

        // Reservation for the bopId used by the predicate.
        idFactory.reserve(pred.getId());

        // Reservations for the bopIds used by the constraints.
        idFactory.reserveIds(c);

        final boolean optional = pred.isOptional();

        /*
         * FIXME RTO: doneSet: We should also include in the doneSet anything
         * that is known to have been materialized based on an analysis of the
         * join path (as executed) up to this point in the path. This will let
         * us potentially do less work. This will require tracking the doneSet
         * in the Path and passing the Path into cutoffJoin(). The simplest way
         * to manage this is to just annotate the Path as we go, which means
         * making the Path more AST aware - or at least doneSet aware. Or we can
         * just apply the analysis to each step in the path to figure out what
         * is done each time we setup cutoff evaluation of an operator.
         */
        final Set<IVariable<?>> doneSet = new LinkedHashSet<IVariable<?>>(
                joinGraph.getDoneSet());

        // Start with an empty plan.
        PipelineOp left = null;

        /*
         * Add the JOIN to the query plan.
         * 
         * Note: The generated query plan will potentially include variable
         * materialization to support constraints that can not be evaluated
         * directly by the JOIN (because they depend on variable bindings that
         * are not known to be materialized.)
         */
        left = join(left, //
                pred, //
                optional ? new LinkedHashSet<IVariable<?>>(doneSet)
                        : doneSet, //
                attachedJoinConstraints == null ? null : Arrays
                        .asList(attachedJoinConstraints),//
                Long.valueOf(limit),// cutoff join limit.
                sp.getQueryHints(),//
                ctx);

        /*
         * [left] is now the last operator in the query plan.
         */

        if (!((left instanceof PipelineJoin) && left.arity() == 0)) {

            /*
             * The query plan contains multiple operators.
             */

            /*
             * Add a SLICE to terminate the query when we have [limit] solution
             * out.
             */
            left = applyQueryHints(new SliceOp(leftOrEmpty(left),//
                    new NV(SliceOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SliceOp.Annotations.OFFSET, "0"),//
                    new NV(SliceOp.Annotations.LIMIT, Integer.toString(limit)),//
                    new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(SliceOp.Annotations.PIPELINED, true),//
                    new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(SliceOp.Annotations.REORDER_SOLUTIONS,false),//
                    new NV(SliceOp.Annotations.SHARED_STATE, true)//
                    ), rtoJoinGroup, ctx);

        }

        if (log.isDebugEnabled())
            log.debug("RTO cutoff join query::\n" + BOpUtility.toString(left)
                    + "\npred::" + pred);

        if (checkQueryPlans) {

            checkQueryPlan(left);
            
        }

       return left;
        
    }
    
    /**
     * Debug code checks the query plan for patterns that could cause
     * {@link OutOfOrderEvaluationException}s.
     * <p>
     * <ul>
     * <li>This looks for query plans that use the alternate sink. Operators
     * that route some solutions to the default sink and some solutions to the
     * alternate sink can cause out of order evaluation. Out of order evaluation
     * is not compatible with cutoff JOIN evaluation.</li>
     * <li>This looks for operators that do not restrict
     * {@link PipelineOp.Annotations#MAX_PARALLEL} to ONE (1).</li>
     * </ul>
     * 
     * TODO We probably need an interface to determine whether an operator (as
     * configured) guarantee in order evaluation. Some operators can not be
     * configured for in order evaluation. Others, including a hash join using a
     * linked hash map for the buckets, can preserve order of the source
     * solutions in their output. This would also be useful for creating query
     * plans that are order preserving when an index order corresponds to the
     * desired output order.
     * 
     * TODO Extend this to check the RTO plan for ordered evaluation and then
     * use this in the test suite. The plan must be provable oredered. We could
     * also use this for order preserving query plans for other purposes.
     */
    private static void checkQueryPlan(final PipelineOp left) {

        final Iterator<PipelineOp> itr = BOpUtility.visitAll(left,
                PipelineOp.class);

        while (itr.hasNext()) {

            final PipelineOp tmp = itr.next();

            if (tmp.getProperty(PipelineOp.Annotations.ALT_SINK_REF) != null) {

                // alternative sink is disallowed.
                throw new RuntimeException("Query plan uses altSink: op="
                        + tmp.toShortString());

            }

            if (tmp.getMaxParallel() != 1) {

                // parallel execution of an operator is disallowed.
                throw new RuntimeException("RTO "
                        + PipelineOp.Annotations.MAX_PARALLEL
                        + ": expected=1, actual=" + tmp.getMaxParallel()
                        + ", op=" + tmp.toShortString());

            }


            if (tmp.isReorderSolutions()) {

                // reordering of solutions is disallowed.
                throw new RuntimeException("RTO "
                        + PipelineOp.Annotations.REORDER_SOLUTIONS
                        + ": expected=false, actual="
                        + tmp.isReorderSolutions() + ", op="
                        + tmp.toShortString());

            }

            if (tmp instanceof PipelineJoin) {

                final PipelineJoin<?> t = (PipelineJoin<?>) tmp;

                final int maxParallelChunks = t.getMaxParallelChunks();

                if (maxParallelChunks != 0) {

                    throw new RuntimeException("PipelineJoin: "
                            + PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS
                            + "=" + maxParallelChunks
                            + " but must be ZERO (0):: op=" + t.toShortString());

                }

                final boolean coalesceDuplicateAccessPaths = t
                        .getProperty(
                                PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS,
                                PipelineJoin.Annotations.DEFAULT_COALESCE_DUPLICATE_ACCESS_PATHS);

                if (coalesceDuplicateAccessPaths) {

                    throw new RuntimeException(
                            "PipelineJoin: "
                                    + PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS
                                    + "=" + coalesceDuplicateAccessPaths
                                    + " but must be false:: op="
                                    + t.toShortString());

                }
                
            }
            
        }

    }

    /**
     * Run a simple cutoff join on the {@link QueryEngine}. This method relies
     * on the ability to control the {@link PipelineJoin} such that it does not
     * reorder the solutions.
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     * @param sourceSample
     *            The source sample (from the upstream vertex).
     * @param limit
     *            The cuttoff join limit.
     * @param joinOp
     *            A query plan consisting of exactly one join. The join must
     *            support cutoff evaluation, e.g., a {@link PipelineJoin}. The
     *            join MUST be marked with the {@link JoinAnnotations#LIMIT}.
     * 
     * @return The sample from the cutoff join of that edge.
     */
    private static EdgeSample runSimpleJoin(//
            final QueryEngine queryEngine,//
            final SampleBase sourceSample,//
            final int limit,//
            final PipelineJoin<?> joinOp) throws Exception {

        if (log.isInfoEnabled())
            log.info("limit=" + limit + ", sourceSample=" + sourceSample
                    + ", query=" + joinOp.toShortString());
        
        // The cutoff limit. This annotation MUST exist on the JOIN.
        if (limit != ((Long) joinOp.getRequiredProperty(JoinAnnotations.LIMIT))
                .intValue())
            throw new AssertionError();
        
        final int joinId = joinOp.getId();
                
        final PipelineOp queryOp = joinOp;
        
        // run the cutoff sampling of the edge.
        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(//
                queryId,//
                queryOp,//
                null,// attributes
//                new LocalChunkMessage(queryEngine, queryId,
//                        joinOp.getId()/* startId */, -1 /* partitionId */,
//                        sourceSample.getSample())
                sourceSample.getSample()
                );

        final List<IBindingSet> result = new LinkedList<IBindingSet>();
        try {
            int nresults = 0;
            try {
                IBindingSet bset = null;
                // Figure out the #of source samples consumed.
                final Iterator<IBindingSet> itr = new Dechunkerator<IBindingSet>(
                        runningQuery.iterator());
                while (itr.hasNext()) {
                    bset = itr.next();
                    result.add(bset);
                    if (nresults++ >= limit) {
                        // Break out if cutoff join over produces!
                        break;
                    }
                }
            } finally {
                // ensure terminated regardless.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            // verify no problems.
            if (runningQuery.getCause() != null) {
                // wrap throwable from abnormal termination.
                throw new RuntimeException(runningQuery.getCause());
            }
        }

        // The join hit ratio can be computed directly from these stats.
        final PipelineJoinStats joinStats = (PipelineJoinStats) runningQuery
                .getStats().get(joinId);

        if (log.isTraceEnabled())
            log.trace(//Arrays.toString(BOpUtility.getPredIds(predicates)) + ": "+
                     joinStats.toString());

        // #of solutions in.
        final int inputCount = (int) joinStats.inputSolutions.get();

        // #of solutions out.
        final long outputCount = joinStats.outputSolutions.get();

        // cumulative range count of the sampled access paths.
        final long sumRangeCount = joinStats.accessPathRangeCount.get();

        /*
         * The #of tuples read from the sampled access paths. This is part of
         * the cost of the join path, even though it is not part of the expected
         * cardinality of the cutoff join.
         * 
         * Note: While IOs is a better predictor of latency, it is possible to
         * choose a pipelined join versus a hash join once the query plan has
         * been decided. Their IO profiles are both correlated to the #of tuples
         * read.
         */
        final long tuplesRead = joinStats.accessPathUnitsIn.get();

        return newEdgeSample(limit, inputCount, outputCount, sumRangeCount,
                tuplesRead, sourceSample, result);

    }

    /**
     * Run a complex cutoff join on the {@link QueryEngine}. This approach is
     * suitable for complex query plans. It tracks each solution in by assigning
     * a row identifier to the source solutions. It then observes the solutions
     * out and figures out how many source solutions were required in order to
     * produce the solutions out.
     * <p>
     * Note: This approach relies on the query plan NOT reordering the source
     * solutions.
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     * @param sourceSample
     *            The source sample (from the upstream vertex).
     * @param limit
     *            The cuttoff join limit.
     * @param query
     *            The query plan for cutoff evaluation of a JOIN with optional
     *            variable materialization requirements and FILTERs.
     * 
     * @return The sample from the cutoff join of that edge.
     * 
     * @throws OutOfOrderEvaluationException
     *             This exception is thrown if reordering of the source
     *             solutions can be observed. This is detected if we observe a
     *             row identifier in the output solutions that is LT a
     *             previously observed row identifier in the output solutions.
     *             If this problem is observed, then you must carefully review
     *             the manner in which the query plan is constructed and the
     *             parallelism in the query plan. Any parallelism or reordering
     *             will trip this error.
     * 
     *             TODO If we hit the {@link OutOfOrderEvaluationException} for
     *             some kinds of access paths quads mode access paths, then we
     *             might need to look at the {@link DataSetJoin} in more depth
     *             and the way in which named graph and default graph joins are
     *             being executed for both local and scale-out deployments. One
     *             fall back position is to feed the input solutions in one at a
     *             time in different running queries. This will give us the
     *             exact output cardinality for a given source solution while
     *             preserving parallel evaluation over some chunk of source
     *             solutions. However, this approach can do too much work and
     *             will incur more overhead than injecting a rowid column into
     *             the source solutions.
     */
    private static EdgeSample runComplexJoin(//
            final QueryEngine queryEngine,//
            final SampleBase sourceSample,//
            final int limit,//
            final PipelineOp query) throws Exception {
        
        if (log.isInfoEnabled())
            log.info("limit=" + limit + ", sourceSample=" + sourceSample
                    + ", query=" + query.toShortString());
        
        // Anonymous variable used for the injected column.
        final IVariable<?> rtoVar = Var.var();
        
        // Inject column of values [1:NSAMPLES].
        final IBindingSet[] in = injectRowIdColumn(rtoVar, 1/* start */,
                sourceSample.getSample());
        
        // run the cutoff sampling of the edge.
        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(//
                queryId,//
                query,//
                null,// attributes
                in// bindingSet[]
//                new LocalChunkMessage(queryEngine, queryId,
//                        joinOp.getId()/* startId */, -1 /* partitionId */,
//                        sourceSample.getSample())
                );

        /*
         * The assumption is that solutions in are converted into solutions out
         * in an ordered preserving manner. This requires that there is no
         * reordering of the solutions in. If this assumption holds, then we can
         * scan the solutions *out*, counting the #of solutions produced until
         * we reach the limit and tracking the number of solutions *in* required
         * to achieve the limit. The SLICE will cutoff the query once the limit
         * is reached.
         * 
         * Note: we check for both overproduction and out of order evaluation
         * for sanity. If either of those assumptions is violated, then that
         * breaks the foundations on which we are attempting to perform the
         * cutoff evaluation of the JOIN.
         */
        final List<IBindingSet> result = new LinkedList<IBindingSet>();
        final int inputCount; // #of solutions in.
        final long outputCount; // #of solutions out.
        try {
            long nout = 0; // #of solutions out.
            try {
                IBindingSet bset = null;
                // Figure out the #of source samples consumed.
                final Iterator<IBindingSet> itr = new Dechunkerator<IBindingSet>(
                        runningQuery.iterator());
                /*
                 * Injected row id variable.
                 * 
                 * Note: Starts at ZERO in case NO solutions out.
                 */
                int lastRowId = 0; 
                while (itr.hasNext()) {
                    bset = itr.next();
                    final int rowid = ((XSDNumericIV) bset.get(rtoVar).get())
                            .intValue();
//log.warn("rowId="+rowid+",lastRowId="+lastRowId+",bset="+bset);
                    if (rowid < lastRowId && failOutOfOrderEvaluation) {
                        /*
                         * Out of order evaluation makes it impossible to
                         * accurately determine the estimated cardinality of the
                         * join since we can not compute the join hit ratio
                         * without knowing the #of solutions in required to
                         * produce a given #of solutions out.
                         */
                        throw new OutOfOrderEvaluationException();
                    }
                    lastRowId = rowid;
                    bset.clear(rtoVar); // drop injected variable.
                    result.add(bset); // accumulate results.
                    if (nout++ >= limit) {
                        // limit is satisfied.
                        break;
                    }
                }
                inputCount = lastRowId;
                outputCount = nout;
            } finally {
                // ensure terminated regardless.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            // verify no problems.
            if (runningQuery.getCause() != null) {
                // wrap throwable from abnormal termination.
                throw new RuntimeException(runningQuery.getCause());
            }
        }

        // There should be a single JOIN.
        final PipelineJoin<?> joinOp = BOpUtility.getOnly(query,
                PipelineJoin.class);

        /*
         * We need to work with the correlation between the input solutions and
         * the output solutions that flow through the query.
         */

        // Stats for the JOIN.
        final PipelineJoinStats joinStats = (PipelineJoinStats) runningQuery
                .getStats().get(joinOp.getId());

        if (log.isTraceEnabled())
            log.trace(//Arrays.toString(BOpUtility.getPredIds(predicates)) + ": "+
            "join::" + joinStats);

        // cumulative range count of the sampled access paths.
        final long sumRangeCount = joinStats.accessPathRangeCount.get();

        /*
         * The #of tuples read from the sampled access paths. This is part of
         * the cost of the join path, even though it is not part of the expected
         * cardinality of the cutoff join.
         * 
         * Note: While IOs is a better predictor of latency, it is possible to
         * choose a pipelined join versus a hash join once the query plan has
         * been decided. Their IO profiles are both correlated to the #of tuples
         * read.
         * 
         * TODO Include tuples read for ChunkedMaterializationOp? This does not
         * currently override the newStats() nor track the tuples read. It will
         * need to be modified to do that. This gives us a better measure of the
         * IO cost associated with a JOIN requiring materialization of variable
         * bindings. However, it is only important to make this visible if it is
         * a factor in the relative cost of two path segments or deciding
         * between an index nested joina and a hash join against an access path.
         */
        final long tuplesRead = joinStats.accessPathUnitsIn.get();

        return newEdgeSample(limit, inputCount, outputCount, sumRangeCount,
                tuplesRead, sourceSample, result);

    }

    /**
     * Create a new {@link EdgeSample} from the cutoff evaluation of some join.
     * 
     * @param limit
     *            The cutoff evaluation limit.
     * @param inputCount
     *            The number of input solutions consumed to produce the observed
     *            number of solutions out.
     * @param outputCount
     *            The observed number of solutions out.
     * @param sumRangeCount
     *            The sum of the fast range counts for the as-bound invocations
     *            of the JOIN.
     * @param tuplesRead
     *            The #of tuples read from the indices for the JOIN and for any
     *            variable materialization operator(s).
     * @param sourceSample
     *            The input sample to the cutoff join.
     * @param result
     *            The solutions produced by cutoff evaluation of that join.
     *            
     * @return The new {@link EdgeSample}.
     */
    private static EdgeSample newEdgeSample(final int limit,
            final int inputCount, final long outputCount,
            final long sumRangeCount, final long tuplesRead,
            final SampleBase sourceSample,
            final List<IBindingSet> result) {

        if (log.isInfoEnabled())
            log.info("inputCount=" + inputCount + ", outputCount="
                    + outputCount + ", sumRangeCount=" + sumRangeCount
                    + ", tuplesRead=" + tuplesRead + ", sourceSample="
                    + sourceSample);

        // #of solutions out as adjusted for various edge conditions.
        final long adjustedCard;
        
        final EstimateEnum estimateEnum;
        if (sourceSample.estimateEnum == EstimateEnum.Exact
                && outputCount < limit) {
            /*
             * Note: If the entire source vertex is being fed into the cutoff
             * join and the cutoff join outputCount is LT the limit, then the
             * sample is the actual result of the join. That is, feeding all
             * source solutions into the join gives fewer than the desired
             * number of output solutions.
             */
            estimateEnum = EstimateEnum.Exact;
            adjustedCard = outputCount;
        } else if (inputCount == 1 && outputCount == limit) {
            /*
             * If the inputCount is ONE (1) and the outputCount is the limit,
             * then the estimated cardinality is a lower bound as more than
             * outputCount solutions might be produced by the join when
             * presented with a single input solution.
             * 
             * However, this condition suggests that the sum of the sampled
             * range counts is a much better estimate of the cardinality of this
             * join.
             * 
             * For example, consider a join feeding a rangeCount of 16 into a
             * rangeCount of 175000. With a limit of 100, we estimated the
             * cardinality at 1600L (lower bound). In fact, the cardinality is
             * 16*175000. This falsely low estimate can cause solutions which
             * are really better to be dropped.
             */
            // replace outputCount with the sum of the sampled range counts.
            adjustedCard = sumRangeCount;
            estimateEnum = EstimateEnum.LowerBound;
        } else if ((sourceSample.estimateEnum != EstimateEnum.Exact)
        /*
         * && inputCount == Math.min(sourceSample.limit,
         * sourceSample.estimatedCardinality)
         */&& outputCount == 0) {
            /*
             * When the source sample was not exact, the inputCount is EQ to the
             * lesser of the source range count and the source sample limit, and
             * the outputCount is ZERO (0), then feeding in all source solutions
             * is not sufficient to generate any output solutions. In this case,
             * the estimated join hit ratio appears to be zero. However, the
             * estimation of the join hit ratio actually underflowed and the
             * real join hit ratio might be a small non-negative value. A real
             * zero can only be identified by executing the full join.
             * 
             * Note: An apparent join hit ratio of zero does NOT imply that the
             * join will be empty (unless the source vertex sample is actually
             * the fully materialized access path - this case is covered above).
             * 
             * path sourceCard * f ( in read out limit adjCard) = estCard :
             * sumEstCard joinPath 15 4800L * 0.00 ( 200 200 0 300 0) = 0 : 3633
             * [ 3 1 6 5 ]
             */
            estimateEnum = EstimateEnum.Underflow;
            adjustedCard = outputCount;
        } else {
            estimateEnum = EstimateEnum.Normal;
            adjustedCard = outputCount;
        }

        /*
         * Compute the hit-join ratio based on the adjusted cardinality
         * estimate.
         */
        final double f = adjustedCard == 0 ? 0
                : (adjustedCard / (double) inputCount);
        // final double f = outputCount == 0 ? 0
        // : (outputCount / (double) inputCount);

        // estimated output cardinality of fully executed operator.
        final long estCard = (long) (sourceSample.estCard * f);

        /*
         * estimated tuples read for fully executed operator
         * 
         * TODO RTO: estRead: The actual IOs depend on the join type (hash join
         * versus pipeline join) and whether or not the file has index order
         * (segment versus journal). A hash join will read once on the AP. A
         * pipeline join will read once per input solution. A key-range read on
         * a segment uses multi-block IO while a key-range read on a journal
         * uses random IO. Also, remote access path reads are more expensive
         * than sharded or hash partitioned access path reads in scale-out.
         */
        final long estRead = (long) (sumRangeCount * f);

        final EdgeSample edgeSample = new EdgeSample(//
                sourceSample,//
                inputCount,//
                tuplesRead,//
                sumRangeCount,//
                outputCount, //
                adjustedCard,//
                f, //
                // args to SampleBase
                estCard, // estimated output cardinality if fully executed.
                estRead, // estimated tuples read if fully executed.
                limit, //
                estimateEnum,//
                result.toArray(new IBindingSet[result.size()]));

        if (log.isDebugEnabled())
            log.debug(//Arrays.toString(BOpUtility.getPredIds(predicates))
                    "newSample=" + edgeSample);

        return edgeSample;
    
    }
    
    /**
     * Inject (or replace) an {@link Integer} "rowId" column. This does not have
     * a side-effect on the source {@link IBindingSet}s.
     * 
     * @param var
     *            The name of the column.
     * @param start
     *            The starting value for the identifier.
     * @param in
     *            The source {@link IBindingSet}s.
     * 
     * @return The modified {@link IBindingSet}s.
     */
    private static IBindingSet[] injectRowIdColumn(final IVariable<?> var,
            final int start, final IBindingSet[] in) {

        if (in == null)
            throw new IllegalArgumentException();

        final IBindingSet[] out = new IBindingSet[in.length];

        for (int i = 0; i < out.length; i++) {

            final IBindingSet bset = in[i].clone();

            bset.set(var, new Constant<IV>(new XSDNumericIV(start + i)));

            out[i] = bset;

        }

        return out;

    }

}
