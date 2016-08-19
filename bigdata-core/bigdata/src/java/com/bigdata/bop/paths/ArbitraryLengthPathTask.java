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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.paths;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.ConcurrentHashMapAnnotations;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.IDistinctFilter;
import com.bigdata.bop.join.JVMDistinctFilter;
import com.bigdata.bop.paths.ArbitraryLengthPathOp.Annotations;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Execute a subquery that represents an arbitrary length path between a single
 * input variable and a single output variable. Continue this in rounds, using 
 * the output of the previous round as the input of the next round. This has
 * the effect of producing the transitive closure of the subquery operation.
 * 
 * IMPORTANT: The input binding set is expected to be a distinct projection over
 * the variables that are bound through the operator; in the general case, this
 * requires a {@link JVMDistinctBindingSetsOp} over these variable(s) prior to
 * calling the operator. In particular, this operator does *not* join with
 * incoming bindings, but discards all variables that are not bound by the
 * associated ALP node.
 * 
 * <p>
 * The basic idea behind this operator is to run a series of rounds until the
 * solutions produced by each round reach a fixed point. Regardless of the the
 * actual schematics of the arbitrary length path (whether there are constants
 * or variables on the left and right side), we use two transitivity variables
 * to keep the operator moving. Depending on the schematics of the arbitrary
 * length path, we can run on forward (left side is input) or reverse (right
 * side is input). For each intermediate solution, the binding for the
 * transitivity variable on the output side is re-mapped to input for the next
 * round.
 * <p>
 * This operator does not use internal parallelism, but it is thread-safe and
 * multiple instances of this operator may be run in parallel by the query
 * engine for parallel evaluation of different binding set chunks flowing
 * through the pipeline. However, there are much more efficient query plan
 * patterns for most use cases. E.g., (a) creating a hash index with all source
 * solutions, (b) flooding a sub-section of the query plan with the source
 * solutions from the hash index; and (c) hash joining the solutions from the
 * sub-section of the query plan back against the hash index to reunite the
 * solutions from the subquery with those in the parent context.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 *         TODO There should be two version of this operator. One for the JVM
 *         heap and another for the native heap. This will help when large
 *         amounts of data are materialized by the internal collections.
 *         
 *         TODO think about whether the whole SolutionKey mechanism is required
 *         at all, now that we have a distinct projection at the end. It might
 *         well be enough to store the input for the previous rounds in a map.
 *         This would also be more "precise" than remembering the solutions:
 *         for paths such as s1 -p-> s2 -p-> s3 and s1 -p-> s3 and an ALP such
 *         as s1 p* ?x, we currently visit s3 twice, once in the first round
 *         and once in the second round. This is unnecessary overhead and it
 *         might help saving a lot in case of cycles (where we currently run
 *         through over and over again).
 */
public class ArbitraryLengthPathTask implements Callable<Void> {

    private static final Logger log = Logger.getLogger(ArbitraryLengthPathOp.class);
    
    private final BOpContext<IBindingSet> context;
    private final PipelineOp subquery;
    private final Gearing forwardGearing, reverseGearing;
    private final long lowerBound, upperBound;
    private final UnsynchronizedArrayBuffer<IBindingSet> out;
    private IDistinctFilter distinctVarFilter;
    private final Set<IVariable<?>> varsToRetain;
    private Set<IVariable<?>> projectInVars;
    private final IVariableOrConstant<?> middleTerm;
    private final IVariable<?> edgeVar;
    private final List<IVariable<?>> dropVars;

    public ArbitraryLengthPathTask(
            final ArbitraryLengthPathOp controllerOp,
            final BOpContext<IBindingSet> context) {

        if (controllerOp == null)
            throw new IllegalArgumentException();

        if (context == null)
            throw new IllegalArgumentException();

        this.context = context;

        this.subquery = (PipelineOp) controllerOp
                .getRequiredProperty(Annotations.SUBQUERY);

        final IVariableOrConstant<?> leftTerm = (IVariableOrConstant<?>) 
                controllerOp.getProperty(Annotations.LEFT_TERM);

        final IVariable<?> leftVar = leftTerm.isVar() ? (IVariable<?>) 
                leftTerm : null;

        final IConstant<?> leftConst = leftTerm.isConstant() ? (IConstant<?>) 
                leftTerm : null;

        final IVariableOrConstant<?> rightTerm = (IVariableOrConstant<?>) 
                controllerOp.getProperty(Annotations.RIGHT_TERM);

        final IVariable<?> rightVar = rightTerm.isVar() ? (IVariable<?>) 
                rightTerm : null;

        final IConstant<?> rightConst = rightTerm.isConstant() ? (IConstant<?>) 
                rightTerm : null;

        final IVariable<?> tVarLeft = (IVariable<?>) controllerOp
                .getProperty(Annotations.TRANSITIVITY_VAR_LEFT);

        final IVariable<?> tVarRight = (IVariable<?>) controllerOp
                .getProperty(Annotations.TRANSITIVITY_VAR_RIGHT);

        this.forwardGearing = new Gearing(leftVar, rightVar, leftConst,
                rightConst, tVarLeft, tVarRight);

        this.reverseGearing = forwardGearing.reverse();

        this.lowerBound = (Long) controllerOp
                .getProperty(Annotations.LOWER_BOUND);

        this.upperBound = (Long) controllerOp
                .getProperty(Annotations.UPPER_BOUND);

        this.projectInVars = new LinkedHashSet<IVariable<?>>();
        this.projectInVars.addAll(Arrays
                .asList((IVariable<?>[]) controllerOp
                        .getProperty(Annotations.PROJECT_IN_VARS)));

        if (log.isDebugEnabled()) {
            log.debug("project in vars: " + projectInVars);
        }

        /*
         * buffer forms chunks which get flushed onto the sink.
         */
        out = new UnsynchronizedArrayBuffer<IBindingSet>(context.getSink(),
                IBindingSet.class, controllerOp.getChunkCapacity());

        edgeVar = (IVariable<?>) controllerOp.getProperty(Annotations.EDGE_VAR);
        middleTerm = (IVariableOrConstant<?>) controllerOp.getProperty(Annotations.MIDDLE_TERM);

        if (log.isDebugEnabled()) {
            log.debug("predVar: " + edgeVar);
            log.debug("middleTerm: " + middleTerm);
        }
        
        if (edgeVar != null && middleTerm == null) {
            throw new IllegalArgumentException("Must provide a middle term when edge var is present");
        }

        /*
         * Compute the variables that are retained by this operator and set
         * up a distinct filter for these variables (this is necessary
         * because the ArbitraryLengthPath operator as defined by the W3C
         * returns distinct solutions only.
         * 
         * As per the changes introduced in https://jira.blazegraph.com/browse/BLZG-2042,
         * we also need to consider variables-constant hybrids, i.e. variables that are
         * represented as constants because they are known to be statically bound to a
         * given value.
         */
        varsToRetain = new LinkedHashSet<IVariable<?>>();
        if (leftVar != null) {
            varsToRetain.add(leftVar);
        } else {
            final IVariable<?> leftVarInConst = getConstantHybridVariable(leftConst);
            if (leftVarInConst!=null) {
                varsToRetain.add(leftVarInConst);
            }
        }
        
        if (rightVar != null) {
            varsToRetain.add(rightVar);
        } else {
            final IVariable<?> rightVarInConst = getConstantHybridVariable(rightConst);
            if (rightVarInConst!=null) {
                varsToRetain.add(rightVarInConst);
            }
        }
         
        if (edgeVar != null)
            varsToRetain.add(edgeVar);
        
        varsToRetain.addAll(projectInVars);
        final IVariable<?>[] varsToRetainList = varsToRetain
                .toArray(new IVariable<?>[varsToRetain.size()]);

        if (log.isDebugEnabled()) {
            log.debug("vars to retain: " + varsToRetain);
        }

        /**
         * The distinct var filter is responsible for removing duplicate
         * solutions.
         */
        distinctVarFilter = new JVMDistinctFilter(varsToRetainList, //
                controllerOp.getProperty(Annotations.INITIAL_CAPACITY,
                        Annotations.DEFAULT_INITIAL_CAPACITY),//
                controllerOp.getProperty(Annotations.LOAD_FACTOR,
                        Annotations.DEFAULT_LOAD_FACTOR),//
                ConcurrentHashMapAnnotations.DEFAULT_CONCURRENCY_LEVEL);
        
        this.dropVars = (List<IVariable<?>>) controllerOp.getProperty(
                Annotations.DROP_VARS, new ArrayList<IVariable<?>>());
        
        if (log.isDebugEnabled()) {
            log.debug("vars to drop: " + dropVars);
        }

    }
  
    @Override
    public Void call() throws Exception {
        
        try {

            final ICloseableIterator<IBindingSet[]> sitr = context
                    .getSource();
            
            if (!sitr.hasNext()) {
                
                processChunk(new IBindingSet[0]);
                
            } else {

                while (sitr.hasNext()) {
                    
                  final IBindingSet[] chunk = sitr.next();
                    processChunk(chunk);
                    
                }
                
            }
            
            // Now that we know the subqueries ran Ok, flush the sink.
            if (!out.isEmpty()) {
               out.flush();                   
            }
            context.getSink().flush();
            
            // Done.
            return null;

        } finally {
            
            context.getSource().close();

            context.getSink().close();
            
            if (context.getSink2() != null)
                context.getSink2().close();

        }
        
    }
    
    private void processChunk(final IBindingSet[] chunkIn) throws Exception {

        final Map<SolutionKey, IBindingSet> solutions = 
                new LinkedHashMap<SolutionKey, IBindingSet>();
        
        final QueryEngine queryEngine = this.context.getRunningQuery()
                .getQueryEngine();

        /*
         * The input to each round of transitive chaining.
         */
        final Set<IBindingSet> nextRoundInput = new LinkedHashSet<IBindingSet>();

        /*
         * Decide based on the schematics of the path and the incoming data
         * whether to run in forward or reverse gear.
         * 
         * TODO Break the incoming chunk into two chunks - one to be run in
         * forward gear and one to be run in reverse. This is an extremely
         * unlikely scenario.
         */
        final Gearing gearing = chooseGearing(chunkIn);

        if (log.isDebugEnabled()) {
            log.debug("gearing: " + gearing);
        }

        for (IBindingSet parentSolutionIn : chunkIn) {

            if (log.isDebugEnabled())
                log.debug("parent solution in: " + parentSolutionIn);

            final IBindingSet childSolutionIn = parentSolutionIn.clone();

            /*
             * The seed is either a constant on the input side of the
             * property path or a bound value for the property path's input
             * variable from the incoming binding set.
             */
            final IConstant<?> seed = gearing.inConst != null ? gearing.inConst
                    : childSolutionIn.get(gearing.inVar);

            if (log.isDebugEnabled())
                log.debug("seed: " + seed);

            if (seed != null) {

                childSolutionIn.set(gearing.tVarIn, seed);

                /*
                 * Add a zero length path from the seed to itself. By
                 * handling this here (instead of in a separate operator) we
                 * get the cardinality right. Except in the case on nested
                 * arbitrary length paths, we are getting too few solutions
                 * from that (over-filtering). See the todo below. Again,
                 * this seems to be a very esoteric problem stemming from an
                 * unlikely scenario. Not going to fix it for now.
                 * 
                 * TODO Add a binding for the bop id for the subquery that
                 * generated this solution and use that as part of the
                 * solution key somehow? This would allow duplicates from
                 * nested paths to remain in the outbound solutions, which
                 * seems to be the problem with the TCK query:
                 * 
                 * :a (:p*)* ?y
                 */
                if (lowerBound == 0
                        && canBind(gearing, childSolutionIn, seed)) {

                    final IBindingSet bs = parentSolutionIn.clone();

                    bs.set(gearing.tVarIn, seed);

                    bs.set(gearing.tVarOut, seed);

                    storeAndEmit(bs, gearing, solutions);

                    if (log.isDebugEnabled()) {
                        log.debug("added a zero length path: " + bs);
                    }

                }

            }

            nextRoundInput.add(childSolutionIn);

        }

        if (log.isDebugEnabled()) {
            for (IBindingSet childSolutionIn : nextRoundInput)
                log.debug("first round input: " + childSolutionIn);
        }

        // go into iteration
        doIterate(solutions, queryEngine, nextRoundInput, gearing);

    } // processChunk method


    /**
     * Performs up to upperBound iterations (or stops if a fixed point has
     * been reached), to detect new bindings for the property paths.
     * Detected bindings are flushed immediately and stored in the solutions
     * map, in order to avoid duplicate work (and break cycles in the
     * graph).
     * 
     * @param solutions
     *            map to store solutions
     * @param queryEngine
     *            the query engine to execute the driver subquery
     * @param nextRoundInput
     *            input for the first iteration
     * @param gearing
     *            the given gearing
     */
    private void doIterate(final Map<SolutionKey, IBindingSet> solutions,
            final QueryEngine queryEngine,
            final Set<IBindingSet> nextRoundInput, final Gearing gearing) {

        /*
         * If we are collecting edge vars and we have an upper bound, we need 
         * to do one extra iteration, a bonus round, to collect edges between 
         * nodes at the max distance away.
         */
        final boolean bonusRound = 
                upperBound < Long.MAX_VALUE && edgeVar != null;
        
        final long n = upperBound + (bonusRound ? 1 : 0);
        
        /*
         * This set collects visited nodes.  It will only be used if we are
         * doing a bonus round.
         */
        final Set<IConstant<?>> visited = bonusRound ?
                new LinkedHashSet<IConstant<?>>() : null;
        
        for (int i = 0; i < n; i++) {

            long sizeBefore = solutions.size();

            // The subquery
            IRunningQuery runningSubquery = null;

            // The iterator draining the subquery
            ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;

            try {

                /*
                 * TODO Replace with code that does the PipelineJoins
                 * manually. Unrolling these iterations can be a major
                 * performance benefit. Another possibility is to use the
                 * GASEngine to expand the paths.
                 */
                runningSubquery = queryEngine.eval(subquery, nextRoundInput
                        .toArray(new IBindingSet[nextRoundInput.size()]));

                long subqueryChunksOut = 0L; // #of chunks read from subquery
                long subquerySolutionsOut = 0L; // #of solutions read from subquery
                
                try {

                    // Declare the child query to the parent.
                    ((AbstractRunningQuery) context.getRunningQuery())
                            .addChild(runningSubquery);

                    // clear the input set to make room for the next round
                    nextRoundInput.clear();

                    // Iterator visiting the subquery solutions.
                    subquerySolutionItr = runningSubquery.iterator();

                    while (subquerySolutionItr.hasNext()) {

                        final IBindingSet[] chunk = subquerySolutionItr
                                .next();

                        subqueryChunksOut++;
                        if (Thread.interrupted()) throw new InterruptedException();
                        for (IBindingSet bs : chunk) {

                            /**
                             * @see <a
                             *      href="http://trac.blazegraph.com/ticket/865">
                             *      OutOfMemoryError instead of Timeout for
                             *      SPARQL Property Paths </a>
                             */
                            if (subquerySolutionsOut++ % 10 == 0
                                    && Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                            if (log.isDebugEnabled()) {
                                log.debug("round " + i + " solution: " + bs);
                            }

                            if (gearing.inVar != null
                                    && !bs.isBound(gearing.inVar)) {

                                /*
                                 * Must be the first round. The first round
                                 * when there are no incoming binding (from
                                 * the parent or previous rounds) is the
                                 * only time the inVar won't be set.
                                 */
                                bs.set(gearing.inVar,
                                        bs.get(gearing.tVarIn));

                                if (log.isDebugEnabled()) {
                                    log.debug("adding binding for inVar: "
                                            + bs);
                                }

                            }
                            
                            /*
                             * If the edgeVar is bound coming in then we need
                             * to check whether it matches the value for
                             * the middle transitive var.  No match, no solution.
                             */
                            if (edgeVar != null && bs.get(edgeVar) != null) {

                                final IConstant<?> edge = middleTerm.isConstant() ? 
                                        (IConstant<?>) middleTerm : 
                                            bs.get((IVariable<?>) middleTerm);
                                        
                                if (!bs.get(edgeVar).equals(edge)) {
                                    continue;
                                }

                            }
                            
                            /*
                             * Do not project any new nodes from the bonus round,
                             * only edges that connect visited nodes.
                             */
                            if (bonusRound) {
                                final IConstant<?> out = bs.get(gearing.tVarOut);
                                if (i+1 == n && !visited.contains(out)) {
                                    /*
                                     * Bonus round + new node, skip
                                     */
                                    continue;
                                }
                                visited.add(out);
                            }

                            storeAndEmit(bs, gearing, solutions);

                            /*
                             * No need to remap solutions, there is no next
                             * round.
                             */
                            if (i+1 == n) {
                                continue;
                            }
                            
                            /*
                             * Copy the binding set as input for next round;
                             * this is necessary, because the storeAndEmit
                             * method below modifies the binding set as a
                             * side effect
                             */
                            final IBindingSet input = bs.clone();

                            input.set(gearing.tVarIn,
                                    bs.get(gearing.tVarOut));
                            input.clear(gearing.tVarOut);

//                            /*
//                             * We also have to filter out anonymous
//                             * variables introduced in this run, taking care
//                             * we do not remove potential anonymous
//                             * variables driving the evaluation.
//                             */
//                            @SuppressWarnings("rawtypes")
//                            final Iterator<IVariable> vit = input.vars();
//                            Set<IVariable<?>> anonymousVars = new LinkedHashSet<IVariable<?>>();
//                            while (vit.hasNext()) {
//
//                                final IVariable<?> var = vit.next();
//                                if (var.isAnonymous()
//                                        && !var.equals(gearing.inVar)
//                                        && !var.equals(gearing.tVarIn)) {
//                                    anonymousVars.add(var);
//                                }
//                            }
//
//                            if (log.isDebugEnabled()) {
//                                log.debug("anonymous vars: "
//                                        + anonymousVars);
//                            }
//
//                            for (IVariable<?> anonymousVar : anonymousVars) {
//                                if (!projectInVars.contains(anonymousVar)
//                                        && !varsToRetain
//                                                .contains(anonymousVar)) {
//                                    input.clear(anonymousVar);
//                                }
//                            }

                            /*
                             * Drop intermediate variables.
                             */
                            for (IVariable<?> var : dropVars) {
                                if (!projectInVars.contains(var)
                                     && !varsToRetain.contains(var)
                                     && !var.equals(gearing.inVar)
                                     && !var.equals(gearing.tVarIn)) {
                                    input.clear(var);
                                }
                            }

                            nextRoundInput.add(input);

                            if (log.isDebugEnabled()) {
                                log.debug("remapped as input for next round: "
                                        + input);
                            }

                        }

                    } // end while

                    // finished with the iterator
                    subquerySolutionItr.close();

                    // wait for the subquery to halt / test for errors.
                    runningSubquery.get();

                    if (log.isDebugEnabled()) {
                        log.debug("done with round " + i + ", count="
                                + subqueryChunksOut + ", totalBefore="
                                + sizeBefore + ", totalAfter="
                                + solutions.size() + ", totalNew="
                                + (solutions.size() - sizeBefore));
                    }

                    // we've reached fixed point
                    if (solutions.size() == sizeBefore) {

                        break;

                    }

                } catch (InterruptedException ex) {

                    // this thread was interrupted, so cancel the subquery.
                    runningSubquery.cancel(true/* mayInterruptIfRunning */);

                    // rethrow the exception.
                    throw ex;

                }

            } catch (Throwable t) {

                /*
                 * If things fail before we start the subquery, or if a subquery
                 * fails (due to abnormal termination), then propagate the error
                 * to the parent and rethrow the first cause error out of the
                 * subquery.
                 * 
                 * Note: IHaltable#getCause() considers exceptions triggered by
                 * an interrupt to be normal termination. Such exceptions are
                 * NOT propagated here and WILL NOT cause the parent query to
                 * terminate.
                 */
                final Throwable cause = (runningSubquery != null && runningSubquery
                        .getCause() != null) ? runningSubquery.getCause() : t;

                throw new RuntimeException(ArbitraryLengthPathTask.this.context
                        .getRunningQuery().halt(cause));

            } finally {

                try {

                    // ensure subquery is halted.
                    if (runningSubquery != null)
                        runningSubquery
                                .cancel(true/* mayInterruptIfRunning */);

                } finally {

                    // ensure the subquery solution iterator is closed.
                    if (subquerySolutionItr != null)
                        subquerySolutionItr.close();

                }

            }

        } // fixed point for loop         
     
        /*
         * Handle the case where there is a constant on the output side of
         * the subquery. Make sure the solution's transitive output variable
         * matches. Filter out solutions where tVarOut != outConst.
         */
        if (gearing.outConst != null) {

            final Iterator<Map.Entry<SolutionKey, IBindingSet>> it = solutions
                    .entrySet().iterator();

            while (it.hasNext()) {

                final IBindingSet bs = it.next().getValue();

                if (!bs.get(gearing.tVarOut).equals(gearing.outConst)) {

                    if (log.isDebugEnabled()) {
                        log.debug("transitive output does not match output const, dropping");
                        log.debug(bs.get(gearing.tVarOut));
                        log.debug(gearing.outConst);
                    }

                    it.remove();

                }

            }

        }

        /*
         * Add the necessary zero-length path solutions for the case where
         * there are variables on both side of the operator.
         */
        if (lowerBound == 0
                && (gearing.inVar != null && gearing.outVar != null)) {

            final Map<SolutionKey, IBindingSet> zlps = 
                    new LinkedHashMap<SolutionKey, IBindingSet>();

            for (IBindingSet bs : solutions.values()) {

                /*
                 * Do not handle the case where the out var is bound by the
                 * incoming solutions.
                 */
                if (bs.isBound(gearing.outVar)) {

                    continue;

                }

                { // left to right

                    final IBindingSet zlp = bs.clone();

                    zlp.set(gearing.tVarOut, zlp.get(gearing.inVar));

                    final SolutionKey key = newSolutionKey(gearing, zlp);

                    if (!solutions.containsKey(key)) {

                        zlps.put(key, zlp);

                    }

                }

                { // right to left

                    final IBindingSet zlp = bs.clone();

                    zlp.set(gearing.inVar, zlp.get(gearing.tVarOut));

                    final SolutionKey key = newSolutionKey(gearing, zlp);

                    if (!solutions.containsKey(key)) {

                        zlps.put(key, zlp);

                    }

                }

            }

            for (SolutionKey key : zlps.keySet()) {
                storeAndEmit(key, zlps.get(key), gearing, solutions);
            }

        }

    }      

    /**
     * Is it possible to bind the out of the gearing to the seed?
     * This may be because it is an unbound variable, or it may be that it is already the seed 
     * (either as a const or as a var) 
     */
    @SuppressWarnings("unchecked")
    private boolean canBind(final Gearing gearing, 
            final IBindingSet childSolutionIn, final IConstant<?> seed) {
        if (gearing.outVar == null) 
            return seed.equals(gearing.outConst);
        if (!childSolutionIn.isBound(gearing.outVar)) 
            return true;
        return seed.equals(childSolutionIn.get(gearing.outVar));
    }
    
    /**
     * Choose forward or reverse gear based on the scematics of the operator
     * and the incoming binding sets.
     */
    private Gearing chooseGearing(final IBindingSet[] bsets) {
        
        /*
         * By just taking the first binding set we are assuming that all
         * the binding sets in this chunk are best served by the same
         * gearing.
         * 
         * TODO Challenge this assumption?
         */
        final IBindingSet bs = (bsets != null && bsets.length > 0) ? 
                bsets[0] : EmptyBindingSet.INSTANCE;
        
        if (forwardGearing.inConst != null) {
            
            if (log.isDebugEnabled())
                log.debug("forward gear");
            
            // <X> (p/p)* ?o or <X> (p/p)* <Y>
            return forwardGearing;
            
        } else if (forwardGearing.outConst != null) {
            
            if (log.isDebugEnabled())
                log.debug("reverse gear");
            
            // ?s (p/p)* <Y>
            return reverseGearing;
            
        } else {
            
            if (bs.isBound(forwardGearing.inVar)) {
                
                if (log.isDebugEnabled())
                    log.debug("forward gear");
                
                // ?s (p/p)* ?o and ?s is bound in incoming binding set
                return forwardGearing;
                
            } else if (bs.isBound(forwardGearing.outVar)) {
                
                if (log.isDebugEnabled())
                    log.debug("reverse gear");
                
                // ?s (p/p)* ?o and ?o is bound in incoming binding set
                return reverseGearing;
                
            } else {
                
                if (log.isDebugEnabled())
                    log.debug("forward gear");
                
                // ?s (p/p)* ?o and neither ?s nor ?o are bound in incoming binding set
                return forwardGearing;
                
            }
            
        }
        
    }
        
   
    /**
     * Need to filter the duplicates per the spec:
     * 
     * "Such connectivity matching does not introduce duplicates (it does
     * not incorporate any count of the number of ways the connection can be
     * made) even if the repeated path itself would otherwise result in
     * duplicates.
     * 
     * The graph matched may include cycles. Connectivity matching is
     * defined so that matching cycles does not lead to undefined or
     * infinite results."
     * 
     * We handle this by keeping the solutions in a Map with a solution key
     * that keeps duplicates from getting in.
     */
    private SolutionKey newSolutionKey(final Gearing gearing,
            final IBindingSet bs) {

        if (edgeVar == null || middleTerm.isConstant()) {
            if (gearing.inVar != null && gearing.outVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.inVar), bs.get(gearing.outVar),
                        bs.get(gearing.tVarOut) });
            } else if (gearing.inVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.inVar), bs.get(gearing.tVarOut) });
            } else if (gearing.outVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.outVar), bs.get(gearing.tVarOut) });
            } else {
                return new SolutionKey(
                        new IConstant<?>[] { bs.get(gearing.tVarOut) });
            }
        } else {
            final IConstant<?> edge = middleTerm.isConstant() ? 
                    (IConstant<?>) middleTerm : bs.get((IVariable<?>) middleTerm);
                    
            if (gearing.inVar != null && gearing.outVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.inVar), bs.get(gearing.outVar),
                        bs.get(gearing.tVarOut), edge });
            } else if (gearing.inVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.inVar), 
                        bs.get(gearing.tVarOut), edge });
            } else if (gearing.outVar != null) {
                return new SolutionKey(new IConstant<?>[] {
                        bs.get(gearing.outVar), 
                        bs.get(gearing.tVarOut), edge });
            } else {
                return new SolutionKey(
                        new IConstant<?>[] { 
                        bs.get(gearing.tVarOut), edge });
            }
            
        }

    }

    /**
     * Generates a new solution key from the binding set and the gearing and
     * adds this combination to the solutions map. Once this has been done,
     * the solution is emitted (it will still run through a distinct filter,
     * taking care that we don't emit solutions that have been emited before
     * already).
     * 
     * @param bs
     *            the binding set representing the solution
     * @param gearing
     *            the associated gearing
     * @param solutions
     *            the solutions map where to store bindings
     */
    private void storeAndEmit(final IBindingSet bs, final Gearing gearing,
            final Map<SolutionKey, IBindingSet> solutions) {

        final SolutionKey solutionKey = newSolutionKey(gearing, bs);
        if (log.isDebugEnabled()) {
            log.debug("solution key: " + solutionKey);
        }
        storeAndEmit(solutionKey, bs, gearing, solutions);

    }

    /**
     * Stores the given solution key, binding set, and associated gearing
     * and adds this combination to the solutions map. Once this has been
     * done, the solution is emitted (it will still run through a distinct
     * filter, taking care that we don't emit solutions that have been
     * emitted before already).
     * 
     * @param solution
     *            the key for the solution
     * @param bs
     *            the binding set representing the solution
     * @param gearing
     *            the associated gearing
     * @param solutions
     *            the solutions map where to store bindings
     */
    private void storeAndEmit(SolutionKey solutionKey, IBindingSet bs,
            final Gearing gearing,
            final Map<SolutionKey, IBindingSet> solutions) {

        solutions.put(solutionKey, bs);
        emitSolutions(bs, gearing);

    }

    /**
     * Flushes a solution to the output buffer, in case it is not a
     * duplicate.
     * 
     * @param bs
     * @param gearing
     */
    private void emitSolutions(final IBindingSet bs, final Gearing gearing) {

        // create a local copy of bs, which can be manipulated
        IBindingSet bset = bs.clone();

        /*
         * Handle the cases where we have hybrid constant-variable input or
         * output variables. In that case, the binding for the respective
         * variable is the constant that it reflects, and we need to add
         * this binding to the binding set
         * 
         * See https://jira.blazegraph.com/browse/BLZG-2042.
         */
        if (gearing.inVar==null) {
            final IVariable<?> constantHybridVar = getConstantHybridVariable(gearing.inConst);
            if (constantHybridVar!=null) {
                bset.set(constantHybridVar, gearing.inConst);
            }
        }
        if (gearing.outVar==null) {
            final IVariable<?> constantHybridVar = getConstantHybridVariable(gearing.outConst);
            if (constantHybridVar!=null) {
                bset.set(constantHybridVar, gearing.outConst);
            }
        }

        /*
         * Set the binding for the outVar if necessary.
         */
        if (gearing.outVar != null) {

            final IConstant<?> out = bset.get(gearing.tVarOut);
            if (out != null) {
                bset.set(gearing.outVar, out);
            }

        }
        
        /*
         * Set the edgeVar if necessary.
         */
        if (edgeVar != null) {
            
            final IConstant<?> edge = middleTerm.isConstant() ? 
                    (IConstant<?>) middleTerm : bs.get((IVariable<?>) middleTerm);
            
            if (edge != null) {
                bset.set(edgeVar, edge);
            }
            
        }

        /**
         * The filter projects the relevant variables as a side effect
         */
        if ((bset = distinctVarFilter.accept(bset)) != null) {

            out.add(bset);
        }
    }


    /**
     * Returns the variable hidden behind the constant if the constant is a
     * constant-variable hybrid. Returns null in all other cases (also if the
     * input constant is null).
     * 
     * @param c the constant
     * @return the variable contained possibly in the constant, null otherwise
     */
    private IVariable<?> getConstantHybridVariable(final IConstant c) {
     
        return c instanceof Constant ? ((Constant<?>) c).getVar() : null;
    }

    /**
     * This operator can work in forward or reverse gear. In forward gear,
     * the left side of the path is the input and the right side is output.
     * In reverse it's the opposite. Each side, input and output, will have
     * one term, either a variable or a constant. Although there are two
     * variables for each side, only one can be non-null. The transitivity
     * variables must always be non-null;
     */
    private final static class Gearing {

        private final IVariable<?> inVar, outVar;
        private final IConstant<?> inConst, outConst;
        private final IVariable<?> tVarIn, tVarOut;

        public Gearing(final IVariable<?> inVar, final IVariable<?> outVar,
                final IConstant<?> inConst, final IConstant<?> outConst,
                final IVariable<?> tVarIn, final IVariable<?> tVarOut) {

            if ((inVar == null && inConst == null)
                    || (inVar != null && inConst != null)) {
                throw new IllegalArgumentException();
            }

            if ((outVar == null && outConst == null)
                    || (outVar != null && outConst != null)) {
                throw new IllegalArgumentException();
            }

            if (tVarIn == null || tVarOut == null) {
                throw new IllegalArgumentException();
            }

            this.inVar = inVar;

            this.outVar = outVar;

            this.inConst = inConst;

            this.outConst = outConst;

            this.tVarIn = tVarIn;

            this.tVarOut = tVarOut;

        }

        public Gearing reverse() {

            return new Gearing(this.outVar, this.inVar, this.outConst,
                    this.inConst, this.tVarOut, this.tVarIn);

        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getSimpleName()).append(" [");
            sb.append("inVar=").append(inVar);
            sb.append(", outVar=").append(outVar);
            sb.append(", inConst=").append(inConst);
            sb.append(", outConst=").append(outConst);
            sb.append(", tVarIn=").append(suffix(tVarIn, 8));
            sb.append(", tVarOut=").append(suffix(tVarOut, 8));
            sb.append("]");

            return sb.toString();

        }

        public String suffix(final Object o, final int len) {

            final String s = o.toString();

            return s.substring(s.length() - len, s.length());

        }

    }

    /**
     * Lifted directly from the {@link JVMDistinctFilter}.
     * 
     * TODO Refactor to use {@link JVMDistinctFilter} directly iff possible
     * (e.g., a chain of the AALP operator followed by the DISTINCT
     * solutions operator)
     * 
     */
    private final static class SolutionKey {

        private final int hash;

        private final IConstant<?>[] vals;

        public SolutionKey(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof SolutionKey)) {
                return false;
            }
            final SolutionKey t = (SolutionKey) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                // @todo verify that this allows for nulls with a unit test.
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }
        
        public String toString() {
            return Arrays.toString(vals);
        }

    }
    
}
