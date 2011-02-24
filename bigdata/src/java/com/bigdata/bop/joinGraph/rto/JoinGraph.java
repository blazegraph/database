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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.controller.AbstractSubqueryOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.util.concurrent.Haltable;

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

	/**
	 * Known annotations.
	 */
	public interface Annotations extends PipelineOp.Annotations {

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
		 * {@value #DEFAULT_NEDGES}). This must be a positive integer.
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
	
    public JoinGraph(final BOp[] args, final NV... anns) {

        this(args, NV.asMap(anns));

	}

    public JoinGraph(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

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

	//  private final JGraph g;

	    final private int limit;
	    
	    final private int nedges;
	    
	    private final SampleType sampleType;

	    JoinGraphTask(final BOpContext<IBindingSet> context) {

	        if (context == null)
	            throw new IllegalArgumentException();

	        this.context = context;

	        // The initial cutoff sampling limit.
	        limit = getLimit();

	        // The initial number of edges (1 step paths) to explore.
	        nedges = getNEdges();

	        sampleType = getSampleType();
	        
//	      if (limit <= 0)
//	          throw new IllegalArgumentException();
	//
//	      if (nedges <= 0)
//	          throw new IllegalArgumentException();

//	        g = new JGraph(getVertices(), getConstraints());

	    }

	    public Void call() throws Exception {

	        // Create the join graph.
            final JGraph g = new JGraph(getVertices(), getConstraints(),
                    sampleType);

	        // Find the best join path.
	        final Path p = g.runtimeOptimizer(context.getRunningQuery()
	                .getQueryEngine(), limit, nedges);

	        // Factory avoids reuse of bopIds assigned to the predicates.
	        final BOpIdFactory idFactory = new BOpIdFactory();

	        // Generate the query from the join path.
	        final PipelineOp queryOp = PartitionedJoinGroup.getQuery(idFactory,
	                p.getPredicates(), getConstraints());

	        // Run the query, blocking until it is done.
	        JoinGraph.runSubquery(context, queryOp);

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
     * 
     *       FIXME runQuery() is not working correctly. The query is being
     *       halted by a {@link BufferClosedException} which appears before it
     *       has materialized the necessary results.
     */
    static private void runSubquery(
            final BOpContext<IBindingSet> parentContext,
            final PipelineOp queryOp) throws Exception {

        final QueryEngine queryEngine = parentContext.getRunningQuery()
                .getQueryEngine();

        /*
         * Run the query.
         * 
         * @todo pass in the source binding sets here and also when sampling the
         * vertices.
         */

        IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;

        final IRunningQuery runningQuery = queryEngine.eval(queryOp);

        try {

            // Iterator visiting the subquery solutions.
            subquerySolutionItr = runningQuery.iterator();

            // Copy solutions from the subquery to the query.
            final long nout = BOpUtility.copy(subquerySolutionItr,
                    parentContext.getSink(), null/* sink2 */,
                    null/* constraints */, null/* stats */);

//            System.out.println("nout=" + nout);

            // verify no problems.
            runningQuery.get();

//            System.out.println("Future Ok");

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

            runningQuery.cancel(true/* mayInterruptIfRunning */);

            if (subquerySolutionItr != null)
                subquerySolutionItr.close();

        }

    }

}
