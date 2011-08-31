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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * Hash join with subquery.
 * <p>
 * All source solutions are fully materialized in a hash table. The keys of the
 * hash table are the as-bound join variable(s). The values in the hash table is
 * the list of solutions having a specific value for the as-bound join
 * variables. Once all solutions are materialized, the subquery is evaluated
 * once. For each solution materialized by the subquery, the operator probes the
 * hash table using the as-bound join variables for the subquery solution. If
 * there is a hit in the hash table, then operator then outputs the cross
 * product of the subquery solution with the solutions list found under that key
 * in the hash table, applying any optional CONSTRAINTS.
 * <p>
 * In order to support OPTIONAL semantics for the subquery, a bit flag must be
 * carried for each entry in the hash table. Once the subquery solutions have
 * been exhausted, if the bit was never set for some entry and the subquery is
 * optional, then the solutions associated with that entry are output, applying
 * any optional CONSTRAINTS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: HTreeHashJoinOp.java 5033 2011-08-16 19:02:04Z thompsonbry $
 */
public class SubqueryHashJoinOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(SubqueryHashJoinOp.class);

    public interface Annotations extends SubqueryJoinAnnotations,
            HashJoinAnnotations {
	
    }
    
    /**
     * Deep copy constructor.
     */
    public SubqueryHashJoinOp(final SubqueryHashJoinOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SubqueryHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

		}

		getRequiredProperty(Annotations.SUBQUERY);

		assertAtOnceJavaHeapOp();

    }

    public SubqueryHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }

    @Override
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    /**
     * Evaluation task.
     */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The operator which is being evaluated.
         */
        private final SubqueryHashJoinOp joinOp;
        
        /**
         * The join variables.
         * 
         * @see SubqueryHashJoinOp.Annotations#JOIN_VARS
         */
        private final IVariable<?>[] joinVars;

        /**
         * The variables to be retained by the join operator. Variables not
         * appearing in this list will be stripped before writing out the
         * binding set onto the output sink(s).
         * 
         * @see SubqueryHashJoinOp.Annotations#SELECT
         */
        final private IVariable<?>[] selectVars;

        /**
         * An array of constraints to be applied to the generated solutions
         * (optional).
         * 
         * @see SubqueryHashJoinOp.Annotations#CONSTRAINTS
         */
        final private IConstraint[] constraints;
        
        /**
         * The subquery to be evaluated.
         * 
         * @see SubqueryHashJoinOp.Annotations#SUBQUERY
         */
        private final PipelineOp subquery;
        
        /**
         * <code>true</code> iff the subquery has OPTIONAL semantics.
         * 
         * @see IPredicate.Annotations#OPTIONAL
         */
        private final boolean optional;

        /**
         * Where the join results are written.
         * <p>
         * Solutions are written on a {@link UnsyncLocalOutputBuffer}, which
         * converts them into chunks. Those {@link UnsyncLocalOutputBuffer}
         * overflows onto the {@link #sink}.
         */
        final private IBlockingBuffer<IBindingSet[]> sink;

        /**
         * The alternative sink to use when the join is {@link #optional} AND
         * {@link BOpContext#getSink2()} returns a distinct buffer for the
         * alternative sink. The binding sets from the source are copied onto
         * the alternative sink for an optional join if the join fails. Normally
         * the {@link BOpContext#getSink()} can be used for both the joins which
         * succeed and those which fail. The alternative sink is only necessary
         * when the failed join needs to jump out of a join group rather than
         * routing directly to the ancestor in the operator tree.
         */
        final private IBlockingBuffer<IBindingSet[]> sink2;

        public ControllerTask(final SubqueryHashJoinOp joinOp,
                final BOpContext<IBindingSet> context) {

            if (joinOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.joinOp = joinOp;
            
            this.joinVars = (IVariable<?>[]) joinOp
                    .getRequiredProperty(Annotations.JOIN_VARS);

            this.selectVars = (IVariable<?>[]) joinOp
                    .getProperty(Annotations.SELECT);

            this.constraints = joinOp.getProperty(
                    Annotations.CONSTRAINTS, null/* defaultValue */);

            this.subquery = (PipelineOp) joinOp
                    .getRequiredProperty(Annotations.SUBQUERY);

            this.optional = joinOp.getProperty(Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();
            
        }

        public Void call() throws Exception {

            if (log.isDebugEnabled())
                log.debug("Evaluating subquery hash join: " + joinOp);

            final BaseJoinStats stats = (BaseJoinStats) context.getStats();

            final QueryEngine queryEngine = context.getRunningQuery()
                    .getQueryEngine();

            try {

                /*
                 * Materialize the binding sets and populate a hash map.
                 */
                final IBindingSet[] all = BOpUtility.toArray(context
                        .getSource(), stats);

                if (log.isDebugEnabled())
                    log.debug("Materialized: " + all.length
                            + " source solutions.");

                final Map<Key, Bucket> map = new LinkedHashMap<Key, Bucket>();

                for (IBindingSet bset : all) {

                    final Key key = makeKey(bset);

                    Bucket b = map.get(key);
                    
                    if(b == null) {
                        
                        map.put(key, b = new Bucket(bset));
                        
                    } else {
                        
                        b.add(bset);
                        
                    }

                }

                if (log.isDebugEnabled())
                    log.debug("There are : " + map.size()
                            + " distinct combinations of the join vars: "
                            + Arrays.toString(joinVars));

                /*
                 * Run the subquery once.
                 * 
                 * TODO We may want to use hash-joins at a position other than
                 * the head of the query plan, in which case we would invoke the
                 * hash join once per input binding set and the input bindings
                 * would be passed into the subquery. [I do not believe that
                 * this can be reconciled with "at-once" evaluation]
                 */

                final IRunningQuery runningSubquery = queryEngine.eval(
                        (PipelineOp) subquery);

                try {

                    if (log.isDebugEnabled())
                        log.debug("Running subquery...");
                    
                    /*
                     * For each solution for the subquery, probe the hash map.
                     * If there is a hit, output the cross product of the
                     * solution with the solutions in the map having the same
                     * as-bound values for their join vars.
                     * 
                     * When outputting a solution, first test the constraints.
                     * If they are satisfied, then output the SELECTed
                     * variables.
                     */

                    final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                            joinOp.getChunkCapacity(), sink);

                    // Thread-local buffer iff optional sink is in use.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? null
                            : new UnsyncLocalOutputBuffer<IBindingSet>(
                                    joinOp.getChunkCapacity(), sink2);

                    // The iterator draining the subquery
                    final IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = runningSubquery
                            .iterator();

                    if (log.isDebugEnabled()) {
                    	if (!subquerySolutionItr.hasNext())
                    		log.debug("Subquery produced no solutions");
                    }
                    
                    while (subquerySolutionItr.hasNext()) {

                        final IBindingSet[] chunk = subquerySolutionItr.next();

                        if (log.isDebugEnabled())
                            log.debug("Considering chunk of " + chunk.length
                                    + " solutions from the subquery");

                        for (IBindingSet subquerySolution : chunk) {

//                          stats.accessPathUnitsIn.increment();

                            if (log.isDebugEnabled())
                                log.debug("Considering " + subquerySolution);

                            final Key key = makeKey(subquerySolution);

                            // Probe the hash map.
                            Bucket b = map.get(key);

                            if (b == null)
                                continue;
                         
                            for(SolutionHit src : b.solutions) {

                                /*
                                 * #of elements accepted for this binding set.
                                 * 
                                 * Note: We count binding sets as accepted
                                 * before we apply the constraints. This has the
                                 * effect that an optional join which produces
                                 * solutions that are then rejected by a FILTER
                                 * associated with the optional predicate WILL
                                 * NOT pass on the original solution even if ALL
                                 * solutions produced by the join are rejected
                                 * by the filter.
                                 */

                                if (log.isDebugEnabled())
                                    log.debug("Join with " + src);

                                /*
                                 * Clone this binding set since it is tested for
                                 * each element visited and would otherwise be
                                 * modified by a side-effect of the join.
                                 */
                                final IBindingSet outSolution = src.solution
                                        .clone();

                                // propagate bindings from the subquery
                                if (!BOpContext
                                        .bind(subquerySolution/* src */,
                                                constraints, selectVars,
                                                outSolution/* dst */)) {
                                    // Join failed.
                                    continue;
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Output solution: " + outSolution);

                                src.nhits++;

                                // Accept this binding set.
                                unsyncBuffer.add(outSolution);

                            }

                        }

                    }
                    
                    if (optional) {

                        /*
                         * Note: when NO subquery solutions joined for a given
                         * source binding set AND the subquery is OPTIONAL then
                         * we output the _original_ binding set to the sink join
                         * task(s) and DO NOT apply the CONSTRAINT(s).
                         */

                        for(Bucket b : map.values()) {
                            
                            for(SolutionHit hit : b.solutions) {

                                if (hit.nhits > 0)
                                    continue;

                                final IBindingSet bs = hit.solution;

                                if (log.isDebugEnabled())
                                    log.debug("Optional solution: " + bs);

                                if (log.isTraceEnabled())
                                    log.trace("Output optional solution: " + bs);

                                if (unsyncBuffer2 == null) {
                                    // use the default sink.
                                    unsyncBuffer.add(bs);
                                } else {
                                    // use the alternative sink.
                                    unsyncBuffer2.add(bs);
                                }

                            }
                            
                        }

                        if (sink2 != null) {
                            unsyncBuffer2.flush();
                            sink2.flush();
                        }

                    } // if(optional)
                    
                    /*
                     * Flush the output.
                     */
                    unsyncBuffer.flush();
                    sink.flush();
                    
                } catch (Throwable t) {

                    if (runningSubquery.getCause() != null) {
                        /*
                         * If a subquery fails (due to abnormal termination),
                         * then propagate the error to the parent and rethrow
                         * the first cause error out of the subquery.
                         * 
                         * Note: IHaltable#getCause() considers exceptions
                         * triggered by an interrupt to be normal termination.
                         * Such exceptions are NOT propagated here and WILL NOT
                         * cause the parent query to terminate.
                         */

                        throw new RuntimeException(runningSubquery.getCause());

                    }

                } finally {

                    runningSubquery.cancel(true/* mayInterruptIfRunning */);

                }

                // done.
                return null;

            } finally {

                sink.close();
                if (sink2 != null)
                    sink2.close();

            }

        }

        /**
         * Return an array of constants corresponding to the as-bound values of
         * the join variables for the given solution.
         * 
         * @param bset
         *            The solution.
         *            
         * @return The as-bound values for the join variables for that solution.
         */
        private Key makeKey(final IBindingSet bset) {
            
            final IConstant<?>[] vals = new IConstant<?>[joinVars.length];

            for (int i = 0; i < joinVars.length; i++) {

                final IVariable<?> v = joinVars[i];
                
                vals[i] = bset.get(v);

            }

            return new Key(vals);
            
        }
        
    } // ControllerTask

    /**
     * Wrapper for the keys in the hash table. This is necessary for the hash
     * table to compare the keys as equal and also provides a efficiencies in
     * the hash code and equals() methods.
     */
    private static class Key {
        
        private final int hash;

        private final IConstant<?>[] vals;

        public Key(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Key)) {
                return false;
            }
            final Key t = (Key) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }
    }
    
    /**
     * An input solution and a hit counter.
     */
    private static class SolutionHit {

        /**
         * The input solution.
         */
        final public IBindingSet solution;

        /**
         * The #of hits on that input solution when processing the join against
         * the subquery.
         */
        public int nhits = 0;
        
        private SolutionHit(final IBindingSet solution) {
            
            if(solution == null)
                throw new IllegalArgumentException();
            
            this.solution = solution;
            
        }
        
        public String toString() {

            return getClass().getName() + "{nhits=" + nhits + ",solution="
                    + solution + "}";

        }
        
    } // class SolutionHit

    /**
     * A group of solutions having the same as-bound values for their join vars.
     * Each solution is paired with a hit counter so we can support OPTIONAL
     * semantics for the join.
     */
    private static class Bucket {

        /**
         * A set of solutions (and their hit counters) which have the same
         * as-bound values for the join variables.
         */
        final List<SolutionHit> solutions = new LinkedList<SolutionHit>(); 

        public String toString() {
            return super.toString() + //
                    "{#solutions=" + solutions.size() + //
                    "}";
        }
        
        public Bucket(final IBindingSet solution) {

            add(solution);
            
        }

        public void add(final IBindingSet solution) {
         
            if (solution == null)
                throw new IllegalArgumentException();
            
            solutions.add(new SolutionHit(solution));
            
        }

    } // Bucket

}
