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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

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
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpRTO;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.util.NT;
import com.bigdata.util.concurrent.Haltable;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of access
 * paths reading on relations (the vertices of the join graph) and joins which
 * connect those relations (the edges of the join graph). This boils down to a
 * collection of {@link IPredicate}s (access paths reading on on relations),
 * shared variables (which identify joins), and {@link IConstraint}s (which may
 * reject some solutions for those joins). Operators other than standard joins
 * (including optional joins, sort, order by, etc.) must be handled downstream
 * from the join graph in a "tail plan".
 * <p>
 * The {@link JoinGraph} operator works in two phases. On its first invocation,
 * it constructs a {@link JGraph join graph} and identifies a join path having a
 * low cost join ordering. This join path is converted into a query plan and set
 * as the {@link Attributes#QUERY_PLAN} attribute on the {@link IRunningQuery}.
 * The upstream solutions are then flooded into sub-query that executes the
 * chosen query plan. The solutions from the sub-query are simply copied to the
 * output sink of the {@link JoinGraph} operator. Once the query plan has been
 * identified by the first invocation, subsequent invocations of this operator
 * simply push more data into the sub-query using the pre-identified query plan.
 * 
 * TODO This approach amounts to bottom-up evaluation of the {@link JGraph}.
 * Thus, the RTO is not using information from the upstream query when it
 * decides on a query plan. Therefore, we could lift-out the RTO sections of the
 * query into named subqueries, run them first in parallel, and then INCLUDE
 * their results into the main query. This would require an AST optimizer to
 * modify the AST. (Currently the RTO is integrated when the query plan is
 * generated in {@link AST2BOpUtility} rather than as an AST optimizer.)
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

//    private static final transient Logger log = Logger
//            .getLogger(JoinGraph.class);

    /**
	 * Known annotations.
	 */
	public interface Annotations extends PipelineOp.Annotations {

//        /**
//         * The variables to be projected out of the join graph (optional). When
//         * <code>null</code>, all variables will be projected out.
//         */
//        String SELECTED = JoinGraph.class.getName() + ".selected";
		
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
         */
        String DONE_SET = JoinGraph.class.getName() + ".doneSet";

        /**
         * The AST {@link JoinGroupNode} for the joins and filters that we are
         * running through the RTO (required).
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
    
//	/**
//	 * @see Annotations#SELECTED
//	 */
//	public IVariable<?>[] getSelected() {
//
//		return (IVariable[]) getRequiredProperty(Annotations.SELECTED);
//
//	}

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

	    @Override
	    public Void call() throws Exception {
	        
            if (getQueryPlan(context.getRunningQuery()) == null) {
                
                /*
                 * Use the RTO to generate a query plan.
                 * 
                 * TODO Make sure that the JoinGraph can not be triggered
                 * concurrently, e.g., that the CONTROLLER attribute prevents
                 * concurrent evaluation, just like MAX_PARALLEL.
                 */
                
                // final long begin = System.nanoTime();

                // Create the join graph.
                final JGraph g = new JGraph(JoinGraph.this);

                /*
                 * This map is used to associate join path segments (expressed
                 * as an ordered array of bopIds) with edge sample to avoid
                 * redundant effort.
                 */
                final Map<PathIds, EdgeSample> edgeSamples = new LinkedHashMap<PathIds, EdgeSample>();

                // Find the best join path.
                final Path path = g.runtimeOptimizer(context.getRunningQuery()
                        .getQueryEngine(), edgeSamples);

                /*
                 * Release samples.
                 * 
                 * TODO If we have fully sampled some vertices or edges, then we
                 * could replace the JOIN with the sample. For this to work, we
                 * would need to access path that could read the sample and we
                 * would have to NOT release the samples until the RTO was done
                 * executing sub-queries against the generated query plan. Since
                 * we can flow multiple chunks into the sub-query, this amounts
                 * to having a LAST_PASS annotation.
                 */
                
                for (EdgeSample s : edgeSamples.values()) {

                    s.releaseSample();
                    
                }
 
                for (Vertex v : g.getVertices()) {

                    if (v.sample != null) {
                        v.sample.releaseSample();
                    
                    }
                    
                }
                
                // Set attribute for the join path result.
                setPath(context.getRunningQuery(), path);

                // Set attribute for the join path samples.
                setSamples(context.getRunningQuery(), edgeSamples);

                // final long mark = System.nanoTime();
                //
                // final long elapsed_queryOptimizer = mark - begin;

                /*
                 * Generate the query from the selected join path.
                 */
                final PipelineOp queryOp = AST2BOpRTO.compileJoinGraph(context
                        .getRunningQuery().getQueryEngine(), JoinGraph.this,
                        path);

                // Set attribute for the join path samples.
                setQueryPlan(context.getRunningQuery(), queryOp);

            }
	        
            // The query plan.
            final PipelineOp queryOp = getQueryPlan(context.getRunningQuery());
            
            // Run the query, blocking until it is done.
	        JoinGraph.runSubquery(context, queryOp);

//	        final long elapsed_queryExecution = System.nanoTime() - mark;
//	        
//            if (log.isInfoEnabled())
//                log.info("RTO: queryOptimizer="
//                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryOptimizer)
//                        + ", queryExecution="
//                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryExecution));

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
     */
    static private void runSubquery(
            final BOpContext<IBindingSet> parentContext,
            final PipelineOp queryOp) throws Exception {

        if(parentContext==null)
            throw new IllegalArgumentException();
        
        if(queryOp==null)
            throw new IllegalArgumentException();
        
        final QueryEngine queryEngine = parentContext.getRunningQuery()
                .getQueryEngine();

        /*
         * Run the sub-query.
         */

        ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;

        // Fully materialize the upstream solutions.
        final IBindingSet[] bindingSets = BOpUtility.toArray(
                parentContext.getSource(), parentContext.getStats());

        /*
         * Run on all available upstream solutions.
         * 
         * Note: The subquery will run for each chunk of upstream solutions, so
         * it could make sense to increase the vector size or to collect all
         * upstream solutions into a SolutionSet and then flood them into the
         * sub-query.
         * 
         * Note: We do not need to do a hash join with the output of the
         * sub-query. This amounts to pipelined evaluation. Solutions flow into
         * a subquery and then back out. The only reason for a hash join would
         * be if we project in only a subset of the variables that were in scope
         * in the parent context and then needed to pick up the correlated
         * variables after running the query plan generated by the RTO.
         */
        final IRunningQuery runningSubquery = queryEngine.eval(queryOp,
                bindingSets);

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
