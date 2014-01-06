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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.controller.AbstractSubqueryOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpRTO;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.util.NT;
import com.bigdata.util.concurrent.Haltable;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations. This boils down to a
 * collection of {@link IPredicate}s (selects on relations), shared variables
 * (which identify joins), and {@link IConstraint}s (which limit solutions).
 * Operators other than standard joins (including optional joins, sort, order
 * by, etc.) must be handled downstream from the join graph in a "tail plan".
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see JGraph
 */
public class JoinGraph extends PipelineOp {

//	private static final transient Logger log = Logger
//			.getLogger(JoinGraph.class);

	private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger
            .getLogger(JoinGraph.class);

    /**
	 * Known annotations.
	 */
	public interface Annotations extends PipelineOp.Annotations {

        /**
         * The variables to be projected out of the join graph (optional). When
         * <code>null</code>, all variables will be projected out.
         */
        String SELECTED = JoinGraph.class.getName() + ".selected";
		
        /**
         * The vertices of the join graph, expressed an an {@link IPredicate}[]
         * (required).
         */
        String VERTICES = JoinGraph.class.getName() + ".vertices";

        /**
         * The constraints on the join graph, expressed an an
         * {@link IConstraint}[] (optional, defaults to no constraints).
         */
        String CONSTRAINTS = JoinGraph.class.getName() + ".constraints";

        /**
         * The initial limit for cutoff sampling (default
         * {@value #DEFAULT_LIMIT}).
         */
        String LIMIT = JoinGraph.class.getName() + ".limit";

		int DEFAULT_LIMIT = 100;

        /**
         * The <i>nedges</i> edges of the join graph having the lowest
         * cardinality will be used to generate the initial join paths (default
         * {@value #DEFAULT_NEDGES}). This must be a positive integer. The edges
         * in the join graph are sorted in order of increasing cardinality and
         * up to <i>nedges</i> of those edges having the lowest cardinality are
         * used to form the initial set of join paths. For each edge selected to
         * form a join path, the starting vertex will be the vertex of that edge
         * having the lower cardinality.
         */
		String NEDGES = JoinGraph.class.getName() + ".nedges";

		int DEFAULT_NEDGES = 2;
		
        /**
         * The type of sample to take (default {@value #DEFAULT_SAMPLE_TYPE)}.
         * 
         * @see SampleIndex.SampleType
         */
        String SAMPLE_TYPE = JoinGraph.class.getName() + ".sampleType";
        
        String DEFAULT_SAMPLE_TYPE = SampleType.RANDOM.name();

        /**
         * The set of variables that are known to have already been materialized
         * in the context in which the RTO was invoked.
         * 
         * FIXME In order to support left-to-right evaluation fully, the
         * {@link JGraph} needs to accept this, track it as it binds variables,
         * and pass it through when doing cutoff joins to avoid pipeline
         * materialization steps for variables that are already known to be
         * materialized. Otherwise the RTO will assume that it needs to
         * materialize everything that needs to be materialized for a FILTER and
         * thus do too much work (which is basically the assumption of bottom-up
         * evaluation, or if you prefer that it is executing in its own little
         * world).
         */
        String DONE_SET = JoinGraph.class.getName() + ".doneSet";

//        /**
//         * The query hints from the dominating AST node (if any). These query
//         * hints will be passed through and made available when we compile the
//         * query plan once the RTO has decided on the join ordering. While the
//         * RTO is running, it needs to override many of the query hints for the
//         * {@link IPredicate}s, {@link PipelineJoin}s, etc. in order to ensure
//         * that the cutoff evaluation semantics are correctly applied while it
//         * is exploring the plan state space for the join graph.
//         */
//        String AST_QUERY_HINTS = JoinGraph.class.getName() + ".astQueryHints";

        /**
         * The AST {@link JoinGroupNode} for the joins and filters that we are
         * running through the RTO (required).
         * 
         * FIXME This should be set by an ASTRTOOptimizer. That class should
         * rewrite the original join group, replacing some set of joins with a
         * JoinGraphNode which implements {@link IJoinNode} and gets hooked into
         * AST2BOpUtility#convertJoinGroup() normally rather than through
         * expectional processing. This will simplify the code and adhere to the
         * general {@link IASTOptimizer} pattern and avoid problems with cloning
         * children out of the {@link JoinGroupNode} when we set it up to run
         * the RTO. [Eventually, we will need to pass this in rather than the
         * {@link IPredicate}[] in order to handle JOINs that are not SPs, e.g.,
         * sub-selects, etc.]
         */
        String JOIN_GROUP = JoinGraph.class.getName() + ".joinGroup";

        /**
         * An {@link NT} object specifying the namespace and timestamp of the KB
         * view against which the RTO is running. This is necessary in order to
         * reconstruct the {@link AST2BOpContext} when it comes time to evaluate
         * either a cutoff join involving filters that need materialization or
         * the selected join path.
         */
        String NT = JoinGraph.class.getName() + ".nt";
        
	}

    /**
     * {@link IQueryAttributes} names for the {@link JoinGraph}. The fully
     * qualified name of the attribute is formed by appending the attribute name
     * to the "bopId-", where <code>bopId</code> is the value returned by
     * {@link BOp#getId()}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface Attributes {

        /**
         * The join path selected by the RTO (output).
         */
        String PATH = JoinGraph.class.getName() + ".path";

        /**
         * The samples associated with join path selected by the RTO (output).
         */
        String SAMPLES = JoinGraph.class.getName() + ".samples";

        /**
         * The physical query plan generated from the RTO determined best join
         * ordering (output). This is used to specify the query plan to be
         * executed by a downstream operator.
         */
        String QUERY_PLAN = JoinGraph.class.getName() + ".queryPlan";

	}
	
    /*
     * JoinGraph operator annotations.
     */
    
	/**
	 * @see Annotations#SELECTED
	 */
	public IVariable<?>[] getSelected() {

		return (IVariable[]) getRequiredProperty(Annotations.SELECTED);

	}

	/**
	 * @see Annotations#VERTICES
	 */
	public IPredicate<?>[] getVertices() {

		return (IPredicate[]) getRequiredProperty(Annotations.VERTICES);

	}

    /**
     * @see Annotations#CONSTRAINTS
     */
    public IConstraint[] getConstraints() {

        return (IConstraint[]) getProperty(Annotations.CONSTRAINTS, null/* none */);

    }

	/**
	 * @see Annotations#LIMIT
	 */
	public int getLimit() {

		return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);

	}

	/**
	 * @see Annotations#NEDGES
	 */
	public int getNEdges() {

		return getProperty(Annotations.NEDGES, Annotations.DEFAULT_NEDGES);

	}

	/**
	 * @see Annotations#SAMPLE_TYPE
	 */
	public SampleType getSampleType() {

	    return SampleType.valueOf(getProperty(Annotations.SAMPLE_TYPE,
                Annotations.DEFAULT_SAMPLE_TYPE));
	    
	}

    /**
     * Return the set of variables that are known to have already been
     * materialized at the point in the overall query plan where the RTO is
     * being executed.
     * 
     * @see Annotations#DONE_SET
     */
	@SuppressWarnings("unchecked")
    public Set<IVariable<?>> getDoneSet() {
	    
	    return (Set<IVariable<?>>) getRequiredProperty(Annotations.DONE_SET);
	    
	}
	
	/*
	 * IQueryAttributes
	 */
	
	/**
	 * Return the computed join path.
	 * 
	 * @see Attributes#PATH
	 */
    public Path getPath(final IRunningQuery q) {

        return (Path) q.getAttributes().get(getId() + "-" + Attributes.PATH);

    }

    /**
     * Return the samples associated with the computed join path.
     * 
     * @see Annotations#SAMPLES
     */
    @SuppressWarnings("unchecked")
    public Map<PathIds, EdgeSample> getSamples(final IRunningQuery q) {

        return (Map<PathIds, EdgeSample>) q.getAttributes().get(
                getId() + "-" + Attributes.SAMPLES);

    }    

    private void setPath(final IRunningQuery q, final Path p) {

        q.getAttributes().put(getId() + "-" + Attributes.PATH, p);
        
    }

    private void setSamples(final IRunningQuery q,
            final Map<PathIds, EdgeSample> samples) {
        
        q.getAttributes().put(getId() + "-" + Attributes.SAMPLES, samples);
        
    }

    /**
     * Return the query plan to be executed based on the RTO determined join
     * ordering.
     * 
     * @see Attributes#QUERY_PLAN
     */
    public PipelineOp getQueryPlan(final IRunningQuery q) {

        return (PipelineOp) q.getAttributes().get(
                getId() + "-" + Attributes.QUERY_PLAN);

    }

    private void setQueryPlan(final IRunningQuery q,
            final PipelineOp queryPlan) {
        
        q.getAttributes().put(getId() + "-" + Attributes.QUERY_PLAN, queryPlan);
        
    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public JoinGraph(final JoinGraph op) {
    
        super(op);
        
    }

    public JoinGraph(final BOp[] args, final NV... anns) {

        this(args, NV.asMap(anns));

	}

    public JoinGraph(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        // optional property.
//        final IVariable<?>[] selected = (IVariable[]) getProperty(Annotations.SELECTED);
//
//        if (selected == null)
//            throw new IllegalArgumentException(Annotations.SELECTED);
//
//        if (selected.length == 0)
//            throw new IllegalArgumentException(Annotations.SELECTED);

        // required property.
        final IPredicate<?>[] vertices = (IPredicate[]) getProperty(Annotations.VERTICES);

        if (vertices == null)
            throw new IllegalArgumentException(Annotations.VERTICES);

        if (vertices.length == 0)
            throw new IllegalArgumentException(Annotations.VERTICES);

        if (getLimit() <= 0)
            throw new IllegalArgumentException(Annotations.LIMIT);

        if (getNEdges() <= 0)
            throw new IllegalArgumentException(Annotations.NEDGES);

        /*
         * TODO Check DONE_SET, NT, JOIN_NODES. These annotations are required
         * for the new code path. We should check for their presence. However,
         * the old code path is used by some unit tests which have not yet been
         * updated and do not supply these annotations.
         */
//        // Required.
//        getDoneSet();
//        
//        // Required.
//        getRequiredProperty(Annotations.NT);
        
        if (!isController())
            throw new IllegalArgumentException();

        switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new IllegalArgumentException(Annotations.EVALUATION_CONTEXT
                    + "=" + getEvaluationContext());
        }

	}

    @Override
	public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

		return new FutureTask<Void>(new JoinGraphTask(context));

	}

	/**
	 * Evaluation of a {@link JoinGraph}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	private class JoinGraphTask implements Callable<Void> {

	    private final BOpContext<IBindingSet> context;

	    JoinGraphTask(final BOpContext<IBindingSet> context) {

	        if (context == null)
	            throw new IllegalArgumentException();

	        this.context = context;

	    }

        /**
         * {@inheritDoc}
         * 
         * FIXME When run as sub-query, we need to fix point the upstream
         * solutions and then flood them into the join graph. Samples of the
         * known bound variables can be pulled from those initial solutions.
         */
	    @Override
	    public Void call() throws Exception {
	        
	        final long begin = System.nanoTime();
	        
            // Create the join graph.
            final JGraph g = new JGraph(getVertices(), getConstraints(),
                    getSampleType());

            /*
             * This map is used to associate join path segments (expressed as an
             * ordered array of bopIds) with edge sample to avoid redundant effort.
             * 
             * FIXME RTO: HEAP MANAGMENT : This map holds references to the cutoff
             * join samples. To ensure that the map has the minimum heap footprint,
             * it must be scanned each time we prune the set of active paths and any
             * entry which is not a prefix of an active path should be removed.
             * 
             * TODO RTO: MEMORY MANAGER : When an entry is cleared from this map,
             * the corresponding allocation in the memory manager (if any) must be
             * released. The life cycle of the map needs to be bracketed by a
             * try/finally in order to ensure that all allocations associated with
             * the map are released no later than when we leave the lexicon scope of
             * that clause.
             */
            final Map<PathIds, EdgeSample> edgeSamples = new LinkedHashMap<PathIds, EdgeSample>();

            // Find the best join path.
            final Path path = g.runtimeOptimizer(context.getRunningQuery()
                    .getQueryEngine(), getLimit(), getNEdges(), edgeSamples);

            // Set attribute for the join path result.
            setPath(context.getRunningQuery(), path);

            // Set attribute for the join path samples.
            setSamples(context.getRunningQuery(), edgeSamples);

	        final long mark = System.nanoTime();
	        
	        final long elapsed_queryOptimizer = mark - begin;
	        
            /*
             * Generate the query from the selected join path.
             */
            final PipelineOp queryOp = AST2BOpRTO.compileJoinGraph(context
                    .getRunningQuery().getQueryEngine(), JoinGraph.this, path);

            // Set attribute for the join path samples.
            setQueryPlan(context.getRunningQuery(), queryOp);

            // Run the query, blocking until it is done.
	        JoinGraph.runSubquery(context, queryOp);

	        final long elapsed_queryExecution = System.nanoTime() - mark;
	        
            if (log.isInfoEnabled())
                log.info("RTO: queryOptimizer="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryOptimizer)
                        + ", queryExecution="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryExecution));

	        return null;

	    }

	} // class JoinGraphTask

    /**
     * Execute the selected join path.
     * <p>
     * Note: When executing the query, it is actually being executed as a
     * subquery. Therefore we have to take appropriate care to ensure that the
     * results are copied out of the subquery and into the parent query. See
     * {@link AbstractSubqueryOp} for how this is done.
     * 
     * @throws Exception
     * 
     * @todo When we execute the query, we should clear the references to the
     *       samples (unless they are exact, in which case they can be used as
     *       is) in order to release memory associated with those samples if the
     *       query is long running. Samples must be held until we have
     *       identified the final join path since each vertex will be used by
     *       each maximum length join path and we use the samples from the
     *       vertices to re-sample the surviving join paths in each round. [In
     *       fact, the samples are not being provided to this evaluation context
     *       right now.]
     * 
     * @todo If there are source binding sets then they need to be applied above
     *       (when we are sampling) and below (when we evaluate the selected
     *       join path).
     */
    static private void runSubquery(
            final BOpContext<IBindingSet> parentContext,
            final PipelineOp queryOp) throws Exception {

        final QueryEngine queryEngine = parentContext.getRunningQuery()
                .getQueryEngine();

        /*
         * Run the query.
         * 
         * TODO Pass in the source binding sets here and also when sampling the
         * vertices? Otherwise it is as if we are doing bottom-up evaluation (in
         * which case the doneSet should be empty on entry).
         */

        ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;

        final IRunningQuery runningSubquery = queryEngine.eval(queryOp);

        try {

            // Declare the child query to the parent.
            ((AbstractRunningQuery) parentContext.getRunningQuery())
                    .addChild(runningSubquery);

            // Iterator visiting the subquery solutions.
            subquerySolutionItr = runningSubquery.iterator();

            // Copy solutions from the subquery to the query.
            final long nout = BOpUtility.copy(subquerySolutionItr,
                    parentContext.getSink(), null/* sink2 */,
                    null/* mergeSolution */, null/* selectVars */,
                    null/* constraints */, null/* stats */);

            // verify no problems.
            runningSubquery.get();

        } catch (Throwable t) {

            if (Haltable.isTerminationByInterrupt(t)) {

                // normal termination.
                return;

            }

            // log.error(t,t);

            /*
             * Propagate the error to the parent and rethrow the first cause
             * error out of the subquery.
             */
            throw new RuntimeException(parentContext.getRunningQuery().halt(t));

        } finally {

            runningSubquery.cancel(true/* mayInterruptIfRunning */);

            if (subquerySolutionItr != null)
                subquerySolutionItr.close();

        }

    }

}
