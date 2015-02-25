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
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.JVMDistinctFilter;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Pipeline join incoming bindings against a special kind of subquery that
 * represents an arbitrary length path between a single input variable and a
 * single output variable. Continue this in rounds, using the output of the
 * previous round as the input of the next round. This has the effect of
 * producing the transitive closure of the subquery operation.
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
 * 
 *         TODO There should be two version of this operator. One for the JVM
 *         heap and another for the native heap. This will help when large
 *         amounts of data are materialized by the internal collections.
 */
public class ArbitraryLengthPathOp extends PipelineOp {

    private static final Logger log = Logger.getLogger(ArbitraryLengthPathOp.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends com.bigdata.bop.PipelineOp.Annotations {

    	/**
    	 * The subquery representing the path between left and right.
    	 */
    	String SUBQUERY = Annotations.class.getName() + ".subquery";
    	
    	/**
    	 * The left term - can be a variable or a constant.
    	 */
        String LEFT_TERM = Annotations.class.getName() + ".leftTerm";

    	/**
    	 * The right term - can be a variable or a constant.
    	 */
        String RIGHT_TERM = Annotations.class.getName() + ".rightTerm";

        /**
         * The left transitivity variable.
         */
        String TRANSITIVITY_VAR_LEFT = Annotations.class.getName() + ".transitivityVarLeft";

        /**
         * The right transitivity variable.
         */
        String TRANSITIVITY_VAR_RIGHT = Annotations.class.getName() + ".transitivityVarRight";
        
        /**
         * The lower bound on the number of rounds to run.  Can be zero (0) or
         * one (1).  A lower bound of zero is a special kind of path - the
         * Zero Length Path.  A zero length path connects a vertex to itself
         * (in graph parlance).  In the context of arbitrary length paths it
         * means we bind the input onto the output regardless of whether they
         * are actually connected via the path or not.
         */
        String LOWER_BOUND =  Annotations.class.getName() + ".lowerBound";

        /**
         * The upper bound on the number of rounds to run.
         */
        String UPPER_BOUND =  Annotations.class.getName() + ".upperBound";
        
        /**
         * Variables to dop in between rounds.  This should be set to the
         * internal variables produced by the path subquery.  Each run of the
         * subquery should be run "fresh", that is without its produced bindings
         * already set.
         */
        String VARS_TO_DROP = Annotations.class.getName() + ".varsToDrop";
        
    }

    /**
     * Deep copy constructor.
     */
    public ArbitraryLengthPathOp(final ArbitraryLengthPathOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public ArbitraryLengthPathOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.SUBQUERY);

        getRequiredProperty(Annotations.LEFT_TERM);

        getRequiredProperty(Annotations.RIGHT_TERM);

        getRequiredProperty(Annotations.TRANSITIVITY_VAR_LEFT);

        getRequiredProperty(Annotations.TRANSITIVITY_VAR_RIGHT);

        getRequiredProperty(Annotations.LOWER_BOUND);

        getRequiredProperty(Annotations.UPPER_BOUND);
        
        getRequiredProperty(Annotations.VARS_TO_DROP);

    }
    
    public ArbitraryLengthPathOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ArbitraryLengthPathTask(this, context));
        
    }
    
    private static class ArbitraryLengthPathTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        private final PipelineOp subquery;
        private final Gearing forwardGearing, reverseGearing;
        private final long lowerBound, upperBound;
        private IVariable<?>[] varsToDrop;
        
        public ArbitraryLengthPathTask(final ArbitraryLengthPathOp controllerOp,
                final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.subquery = (PipelineOp) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERY);
            
            final IVariableOrConstant<?> leftTerm = (IVariableOrConstant<?>) controllerOp
                    .getProperty(Annotations.LEFT_TERM);
            
        	final IVariable<?> leftVar = leftTerm.isVar() ? (IVariable<?>) leftTerm : null;
        	
        	final IConstant<?> leftConst = leftTerm.isConstant() ? (IConstant<?>) leftTerm : null;
            	
            final IVariableOrConstant<?> rightTerm = (IVariableOrConstant<?>) controllerOp
                    .getProperty(Annotations.RIGHT_TERM);
            
            final IVariable<?> rightVar = rightTerm.isVar() ? (IVariable<?>) rightTerm : null;
            
        	final IConstant<?> rightConst = rightTerm.isConstant() ? (IConstant<?>) rightTerm : null;
            	
        	final IVariable<?> tVarLeft = (IVariable<?>) controllerOp
                    .getProperty(Annotations.TRANSITIVITY_VAR_LEFT);

        	final IVariable<?> tVarRight = (IVariable<?>) controllerOp
                    .getProperty(Annotations.TRANSITIVITY_VAR_RIGHT);
        	
        	this.forwardGearing = new Gearing(
        			leftVar, rightVar, leftConst, rightConst, tVarLeft, tVarRight);
        	
        	this.reverseGearing = forwardGearing.reverse();

            this.lowerBound = (Long) controllerOp
            		.getProperty(Annotations.LOWER_BOUND);
            
            this.upperBound = (Long) controllerOp
            		.getProperty(Annotations.UPPER_BOUND);
            
        	this.varsToDrop = (IVariable<?>[]) controllerOp
                    .getProperty(Annotations.VARS_TO_DROP);
        	
        	if (log.isDebugEnabled()) {
        	    for (IVariable v : varsToDrop)
        	        log.debug(v);
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

//                    	for (IBindingSet bs : chunk)
//	                    	processChunk(new IBindingSet[] { bs });
						
                	}
                	
                }
                
                // Now that we know the subqueries ran Ok, flush the sink.
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
        
        @SuppressWarnings("unchecked")
        private void processChunk(final IBindingSet[] chunkIn) throws Exception {
        
            final Map<SolutionKey, IBindingSet> solutionsOut = 
            		new LinkedHashMap<SolutionKey, IBindingSet>();
            
            final QueryEngine queryEngine = this.context
                    .getRunningQuery().getQueryEngine();

            /*
             * The input to each round of transitive chaining.
             */
            final Set<IBindingSet> nextRoundInput = new LinkedHashSet<IBindingSet>();

            /*
             * Decide based on the schematics of the path and the
             * incoming data whether to run in forward or reverse gear.
             * 
             * TODO Break the incoming chunk into two chunks - one to be run
             * in forward gear and one to be run in reverse.  This is an
             * extremely unlikely scenario.
             */
            final Gearing gearing = chooseGearing(chunkIn);
            
            if (log.isDebugEnabled()) {
            	log.debug("gearing: " + gearing);
            }
            
            final boolean noInput = chunkIn == null || chunkIn.length == 0 ||
            		(chunkIn.length == 1 && chunkIn[0].isEmpty());
            
            /*
             * We need to keep a collection of parent solutions to join
             * against the output from the fixed point operation.
             */
            final Map<IConstant<?>, List<IBindingSet>> parentSolutionsToJoin = 
            		noInput ? null : new LinkedHashMap<IConstant<?>, List<IBindingSet>>();

            /*
             * The join var is what we use to join the parent solutions to the
             * output from the fixed point operation.
             */
            final IVariable<?> joinVar = gearing.inVar != null ? 
            		gearing.inVar : 
            			(gearing.outVar != null ? gearing.outVar : null);
            
            if (log.isDebugEnabled()) {
            	log.debug("join var: " + joinVar);
            }
            
            if (!noInput) {
                
                long chunksIn = 0L;
            	    
                for (IBindingSet parentSolutionIn : chunkIn) {
		            	
                    /**
                     * @see <a href="http://trac.bigdata.com/ticket/865"
                     *      >OutOfMemoryError instead of Timeout for SPARQL
                     *      Property Paths </a>
                     */
                    if (chunksIn++ % 10 == 0 && Thread.interrupted()) {
                        throw new InterruptedException();
                    }

                    final IConstant<?> key = joinVar != null ? parentSolutionIn.get(joinVar) : null;
	            	
	                if (log.isDebugEnabled()) {
	                	log.debug("adding parent solution for joining: " + parentSolutionIn);
	                	log.debug("join key: " + key);
	                }
	                
	            	if (!parentSolutionsToJoin.containsKey(key)) {
	            		parentSolutionsToJoin.put(key, new ArrayList<IBindingSet>());
	            	}
	            	
	            	parentSolutionsToJoin.get(key).add(parentSolutionIn);
	            	
	            }
	            
            }
            
            for (IBindingSet parentSolutionIn : chunkIn) {
            
	            if (log.isDebugEnabled())
	            	log.debug("parent solution in: " + parentSolutionIn);
        
                final IBindingSet childSolutionIn = parentSolutionIn.clone();
                
                /*
                 * The seed is either a constant on the input side of
                 * the property path or a bound value for the property 
                 * path's input variable from the incoming binding set.
                 */
                final IConstant<?> seed = gearing.inConst != null ?
            		gearing.inConst : childSolutionIn.get(gearing.inVar);
                
                if (log.isDebugEnabled())
                	log.debug("seed: " + seed);
                
                if (seed != null) {
                	
                	childSolutionIn.set(gearing.tVarIn, seed);
                	
                	/*
                	 * Dirty hack for zero length paths. Add a zero length
                	 * path from the seed to itself.  By handling this here
                	 * (instead of in a separate operator) we get the 
                	 * cardinality right.  Except in the case on nested
                	 * arbitrary length paths, we are getting too few solutions
                	 * from that (over-filtering).  See the todo below.  Again,
                	 * this seems to be a very esoteric problem stemming from
                	 * an unlikely scenario.  Not going to fix it for now.
                	 * 
                	 * TODO Add a binding for the bop id for the
                	 * subquery that generated this solution and use
                	 * that as part of the solution key somehow?  This
                	 * would allow duplicates from nested paths to
                	 * remain in the outbound solutions, which seems to
                	 * be the problem with the TCK query:
                	 * 
                	 * :a (:p*)* ?y
                	 */
                	if (lowerBound == 0 && canBind(gearing, childSolutionIn, seed)) {
                		
                		final IBindingSet bs = parentSolutionIn.clone();
                		
                		/*
                		 * Setting the outVar seems to produce duplicates
                		 * when we do chunk at a time.
                		 */
//	               		bs.set(gearing.outVar, seed);
                		
                		bs.set(gearing.tVarIn, seed);
                		
                		bs.set(gearing.tVarOut, seed);
                		
                		solutionsOut.put(newSolutionKey(gearing, bs), bs);
                		
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
        	
            for (int i = 0; i < upperBound; i++) {
            	
				long sizeBefore = solutionsOut.size();

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;

                try {
            	
                	    /*
                     * TODO Replace with code that does the PipelineJoins
                     * manually. Unrolling these iterations can be a major
                     * performance benefit. Another possibility is to use
                     * the GASEngine to expand the paths.
                     */
                    runningSubquery = queryEngine.eval(subquery,
                            nextRoundInput.toArray(new IBindingSet[nextRoundInput.size()]));

					long subqueryChunksOut = 0L; // #of chunks read from subquery
					try {

                        // Declare the child query to the parent.
                        ((AbstractRunningQuery) context.getRunningQuery())
                                .addChild(runningSubquery);

                        // clear the input set to make room for the next round
	                    nextRoundInput.clear();
	                    
						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();
						
				        while (subquerySolutionItr.hasNext()) {

				            final IBindingSet[] chunk = subquerySolutionItr.next();

			            	for (IBindingSet bs : chunk) {
				            	
		                    /**
		                     * @see <a href="http://trac.bigdata.com/ticket/865"
		                     *      >OutOfMemoryError instead of Timeout for SPARQL
		                     *      Property Paths </a>
		                     */
                            if (subqueryChunksOut++ % 10 == 0 && Thread.interrupted()) {
			            		    throw new InterruptedException();
			            		}
			            		
			            		if (log.isDebugEnabled()) {
			            			log.debug("round " + i + " solution: " + bs);
			            		}
			            		
			            		if (gearing.inVar != null && !bs.isBound(gearing.inVar)) {
	
				            		/*
				            		 * Must be the first round.  The first
				            		 * round when there are no incoming
				            		 * binding (from the parent or previous
				            		 * rounds) is the only time the inVar
				            		 * won't be set.
				            		 */
				            		bs.set(gearing.inVar, bs.get(gearing.tVarIn));
					            	
				            		if (log.isDebugEnabled()) {
				            			log.debug("adding binding for inVar: " + bs);
				            		}
				            		
				            	}
			            		
			            		// drop the intermediate variables
			            		dropVars(bs, true);
			            		
//				            	solutionsOut.add(solution);
			            		solutionsOut.put(newSolutionKey(gearing, bs), bs);
			            		
			            		/*
			            		 * Remap the solution as input to the next round.
			            		 */
			            		final IBindingSet input = bs.clone();
			            		
                                dropVars(input, false);
                                
			            		input.set(gearing.tVarIn, bs.get(gearing.tVarOut));
			            		
			            		input.clear(gearing.tVarOut);
			            		
			            		nextRoundInput.add(input);

			            		if (log.isDebugEnabled()) {
			            			log.debug("remapped as input for next round: " + input);
			            		}
			            		
				            }
				            
				        }
				        
				        // finished with the iterator
				        subquerySolutionItr.close();

						// wait for the subquery to halt / test for errors.
						runningSubquery.get();
						
				        if (log.isDebugEnabled()) {
				        	log.debug("done with round " + i + 
				        			", count=" + subqueryChunksOut + 
				        			", totalBefore=" + sizeBefore + 
				        			", totalAfter=" + solutionsOut.size() +
				        			", totalNew=" + (solutionsOut.size() - sizeBefore));
				        }
				        
						// we've reached fixed point
						if (solutionsOut.size() == sizeBefore) {
							
							break;
							
						}
					
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
                } catch (Throwable t) {

					if (runningSubquery == null
							|| runningSubquery.getCause() != null) {
						/*
						 * If things fail before we start the subquery, or if a
						 * subquery fails (due to abnormal termination), then
						 * propagate the error to the parent and rethrow the
						 * first cause error out of the subquery.
						 * 
						 * Note: IHaltable#getCause() considers exceptions
						 * triggered by an interrupt to be normal termination.
						 * Such exceptions are NOT propagated here and WILL NOT
						 * cause the parent query to terminate.
						 */
                        throw new RuntimeException(ArbitraryLengthPathTask.this.context
                                .getRunningQuery().halt(
                                        runningSubquery == null ? t
                                                : runningSubquery.getCause()));
                    }
					
//					return runningSubquery;
                    
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
    		 * the subquery.  Make sure the solution's transitive output 
    		 * variable matches. Filter out solutions where tVarOut != outConst.
             */
        	if (gearing.outConst != null) {
        		
	            final Iterator<Map.Entry<SolutionKey, IBindingSet>> it = 
	            		solutionsOut.entrySet().iterator();
	            
	            while (it.hasNext()) {
	            	
	            	final IBindingSet bs = it.next().getValue();
	            	
            		/*
            		 */
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
        	if (lowerBound == 0 && (gearing.inVar != null && gearing.outVar != null)) {
        	
        		final Map<SolutionKey, IBindingSet> zlps = 
        				new LinkedHashMap<SolutionKey, IBindingSet>();
        		
        		for (IBindingSet bs : solutionsOut.values()) {
        			
        			/*
        			 * Do not handle the case where the out var is bound by
        			 * the incoming solutions.
        			 */
        			if (bs.isBound(gearing.outVar)) {
        				
        				continue;
        				
        			}
        			
        			{ // left to right
	        		
        				final IBindingSet zlp = bs.clone();
	        			
	        			zlp.set(gearing.tVarOut, zlp.get(gearing.inVar));
	        			
	        			final SolutionKey key = newSolutionKey(gearing, zlp);
	        			
	        			if (!solutionsOut.containsKey(key)) {
	        				
	        				zlps.put(key, zlp);
	        				
	        			}
	        			
        			}
        			
        			{ // right to left
    	        		
        				final IBindingSet zlp = bs.clone();
	        			
	        			zlp.set(gearing.inVar, zlp.get(gearing.tVarOut));
	        			
	        			final SolutionKey key = newSolutionKey(gearing, zlp);
	        			
	        			if (!solutionsOut.containsKey(key)) {
	        				
	        				zlps.put(key, zlp);
	        				
	        			}
	        			
        			}
        			
        		}
        		
        		solutionsOut.putAll(zlps);
        		
        	}
            
        	/*
        	 * We can special case when there was no input (and
        	 * thus no join is required).
        	 */
            if (noInput) {

            	for (IBindingSet bs : solutionsOut.values()) {

            		/*
            		 * Set the binding for the outVar if necessary.
            		 */
            		if (gearing.outVar != null) {
            			
            			bs.set(gearing.outVar, bs.get(gearing.tVarOut));
            			
            		}
            		
                	/*
                	 * Clear the intermediate variables before sending the output
                	 * down the pipeline.
                	 */
	            	bs.clear(gearing.tVarIn);
	            	bs.clear(gearing.tVarOut);
            		
            	}
            	
                final IBindingSet[] chunkOut =  
	        		solutionsOut.values().toArray(
	        				new IBindingSet[solutionsOut.size()]);
        
		        if (log.isDebugEnabled()) {
		        	log.debug("final output to sink:\n" + Arrays.toString(chunkOut).replace("}, ", "},\n"));
		        }
		        
		        // copy accepted binding sets to the default sink.
		        context.getSink().add(chunkOut);
            	
            } else {
            
	            final ArrayList<IBindingSet> finalOutput = new ArrayList<IBindingSet>();
	            
	            final Iterator<Map.Entry<SolutionKey, IBindingSet>> it = 
	            		solutionsOut.entrySet().iterator();
	            
	            while (it.hasNext()) {
	
	            	final Map.Entry<SolutionKey, IBindingSet> entry = it.next();
	            	
	            	final IBindingSet bs = entry.getValue();
	            	
	            	if (log.isDebugEnabled()) {
	            		log.debug("considering possible solution: " + bs);
	            	}
	            	
	            	final IConstant<?> key = joinVar != null ? bs.get(joinVar) : null;
	            	
	            	if (key != null && parentSolutionsToJoin.containsKey(key)) {
	            		
		            	final List<IBindingSet> parentSolutionsIn = 
		            			parentSolutionsToJoin.get(key);
		            	
		                if (log.isDebugEnabled()) {
		                	log.debug("join key: " + key);
		                	log.debug("parent solutions to join: " + parentSolutionsIn);
		                }
	                
	                	for (IBindingSet parentSolutionIn : parentSolutionsIn) {
	                
			            	if (gearing.outConst != null) {
			            		
		        				/*
		        				 * No need to clone, since we are not modifying the
		        				 * incoming parent solution in this case. The ALP
		        				 * is simply acting as a filter.
		        				 */
		        				finalOutput.add(parentSolutionIn);
		        				
			            	} else { // outVar != null
			            		
			                	/*
			                	 * Handle the case where the gearing.outVar was bound 
			                	 * coming in.  Again, make sure it matches the
			                	 * transitive output variable.
			                	 */
			            		if (parentSolutionIn.isBound(gearing.outVar)) {
			            			
			            			// do this now: note already known to be bound per test above.
			            			final IConstant<?> poutVar = parentSolutionIn.get(gearing.outVar);

			            			if (!poutVar.equals(bs.get(gearing.tVarOut))) {
    			            				
                                        if (log.isDebugEnabled()) {
                                            log.debug("transitive output does not match incoming binding for output var, dropping");
                                        }
			    	                	
			            				continue;
			            				
			            			} else {
				        				
				        				/*
				        				 * No need to clone, since we are not modifying the
				        				 * incoming parent solution in this case. The ALP
				        				 * is simply acting as a filter.
				        				 */
				        				finalOutput.add(parentSolutionIn);
				        				
//	                                    /*
//	                                     * Clone, modify, and accept. Do this to
//	                                     * pick up non-anonymous variables bound
//	                                     * by the alp service.
//	                                     */
//	                                    final IBindingSet join = parentSolutionIn.clone();
//	                                    
//	                                    /*
//	                                     * Copy any other non-intermediate variables.
//	                                     */
//	                                    @SuppressWarnings("rawtypes")
//	                                    final Iterator<IVariable> vit = bs.vars();
//	                                    while (vit.hasNext()) {
//	                                        @SuppressWarnings("rawtypes")
//	                                        final IVariable v = vit.next();
//	                                        if (v.isAnonymous()) {
//	                                            continue;
//	                                        }
//	                                        join.set(v, bs.get(v));
//	                                    }
//	                                    
//	                                    finalOutput.add(join);
	                                    
				        			}
			            			
			            		} else {
			            			
			            			/*
			            			 * Handle the normal case - when we simply
			            			 * need to copy the transitive output over to
			            			 * the real output.
			            			 */
		//	            			bs.set(gearing.outVar, bs.get(gearing.tVarOut));
			            			
			            			/*
			            			 * Clone, modify, and accept.
			            			 */
		            				final IBindingSet join = parentSolutionIn.clone();
		            				
		            				join.set(gearing.outVar, bs.get(gearing.tVarOut));
		            				
//		            				/*
//		            				 * Copy any other non-intermediate variables.
//		            				 */
//		            				@SuppressWarnings("rawtypes")
//                                    final Iterator<IVariable> vit = bs.vars();
//		            				while (vit.hasNext()) {
//		            				    @SuppressWarnings("rawtypes")
//                                        final IVariable v = vit.next();
//		            				    if (v.isAnonymous()) {
//		            				        continue;
//		            				    }
//		            				    join.set(v, bs.get(v));
//		            				}
		            				
		            				finalOutput.add(join);
			            			
			            		}
			            		
			            	}
			            	
			            	if (log.isDebugEnabled()) {
			            		log.debug("solution accepted");
			            	}
			            	
		                }
	                	
	                } 

	            	/*
	            	 * Always do the null solutions if there are any.
	            	 */
	            	if (parentSolutionsToJoin.containsKey(null)) {
	            		
		                /*
		                 * Join the null solutions.  These solutions represent 
		                 * a cross product (no shared variables with the ALP node).
		                 */
			        	final List<IBindingSet> nullSolutions = parentSolutionsToJoin.get(null);
			        	
			            if (log.isDebugEnabled()) {
			            	log.debug("null solutions to join: " + nullSolutions);
			            }
			            
		            	for (IBindingSet nullSolution : nullSolutions) {
		            
		            		final IBindingSet solution;
		            		
	//			            		if ((gearing.inVar != null && !nullSolution.isBound(gearing.inVar)) ||
	//			            				(gearing.outVar != null && !nullSolution.isBound(gearing.outVar))) {
		            		if (gearing.inVar != null || gearing.outVar != null) {
		            		
		            			solution = nullSolution.clone();
		            		
		            		} else {
		            			
		            			solution = nullSolution;
		            			
		            		}
		            		
		            		if (gearing.inVar != null) {
		            			
		            			if (solution.isBound(gearing.inVar)) {
		            				
		            				/*
		            				 * This should never happen.
		            				 */
		            				throw new RuntimeException();
		            				
		            			} else {
		            				
		            				solution.set(gearing.inVar, bs.get(gearing.inVar));
		            				
		            			}
		            			
		            		}
		            		
		            		if (gearing.outVar != null) {
		            			
		            			if (solution.isBound(gearing.outVar)) {
		            				
		            				/*
		            				 * This should never happen.
		            				 */
		            				throw new RuntimeException();
	//		            				if (!bs.get(gearing.tVarOut).equals(solution.get(gearing.outVar))) {
	//		            					
	//		            					// discard this solution;
	//		            					continue;
	//		            					
	//		            				}
		            				
		            			} else {
		            				
		            				solution.set(gearing.outVar, bs.get(gearing.tVarOut));
		            				
		            			}
		            			
		            		}
		            		
		            		finalOutput.add(solution);
		            		
			            	if (log.isDebugEnabled()) {
			            		log.debug("solution accepted");
			            	}
			            	
		                }
		            	
	            	}
		            	
	            }
	            
	            final IBindingSet[] chunkOut = finalOutput.toArray(new IBindingSet[finalOutput.size()]); 
	//            		solutionsOut.values().toArray(
	//            				new IBindingSet[solutionsOut.size()]);
	            
	            if (log.isDebugEnabled()) {
	            	log.debug("final output to sink:\n" + Arrays.toString(chunkOut).replace("}, ", "},\n"));
	            }
	            
	            // copy accepted binding sets to the default sink.
	            context.getSink().add(chunkOut);

            }
                
        } // processChunk method

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
		 * Drop vars bound by nested paths that are not meant to be external
		 * output.
		 */
        private void dropVars(final IBindingSet bs, final boolean anonOnly) {
        	
        	if (varsToDrop != null) {
        		
        		for (IVariable<?> v : varsToDrop) {

//        		    if (!anonOnly || v.isAnonymous()) {
        		        bs.clear(v);
//        		    }
        			
        		}
        		
        	}
        	
        }
        
        /**
		 * Need to filter the duplicates per the spec:
		 * 
		 * "Such connectivity matching does not introduce duplicates
		 * (it does not incorporate any count of the number of ways
		 * the connection can be made) even if the repeated path
		 * itself would otherwise result in duplicates.
		 * 
		 * The graph matched may include cycles. Connectivity
		 * matching is defined so that matching cycles does not lead
		 * to undefined or infinite results."
		 * 
		 * We handle this by keeping the solutions in a Map with a solution
		 * key that keeps duplicates from getting in.
		 */
        private SolutionKey newSolutionKey(final Gearing gearing, final IBindingSet bs) {
        	
        	if (gearing.inVar != null && gearing.outVar != null) {
        		return new SolutionKey(new IConstant<?>[] {
    				bs.get(gearing.inVar), bs.get(gearing.outVar), bs.get(gearing.tVarOut)
        		});
        	} else if (gearing.inVar != null) {
        		return new SolutionKey(new IConstant<?>[] {
    				bs.get(gearing.inVar), bs.get(gearing.tVarOut)
        		});
        	} else if (gearing.outVar != null) {
        		return new SolutionKey(new IConstant<?>[] {
    				bs.get(gearing.outVar), bs.get(gearing.tVarOut)
        		});
        	} else {
        		return new SolutionKey(new IConstant<?>[] {
    				bs.get(gearing.tVarOut)
        		});
        	}
        	
        }

        /**
         * This operator can work in forward or reverse gear.  In forward gear,
         * the left side of the path is the input and the right side is output.
         * In reverse it's the opposite.  Each side, input and output, will
         * have one term, either a variable or a constant.  Although there are
         * two variables for each side, only one can be non-null.  The
         * transitivity variables must always be non-null;
         */
        private final static class Gearing {
        	
            private final IVariable<?> inVar, outVar;
            private final IConstant<?> inConst, outConst;
            private final IVariable<?> tVarIn, tVarOut;
            
            public Gearing(
	            final IVariable<?> inVar, final IVariable<?> outVar,
	            final IConstant<?> inConst, final IConstant<?> outConst,
	    		final IVariable<?> tVarIn, final IVariable<?> tVarOut) {
            	
            	if ((inVar == null && inConst == null) ||
        			(inVar != null && inConst != null)) {
            		throw new IllegalArgumentException();
            	}
            	
            	if ((outVar == null && outConst == null) ||
        			(outVar != null && outConst != null)) {
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
            	
            	return new Gearing(
            			this.outVar, this.inVar, 
            			this.outConst, this.inConst, 
            			this.tVarOut, this.tVarIn);
            	
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
            	
            	return s.substring(s.length()-len, s.length());
            	
            }
            
        }
        
        /**
         * Lifted directly from the {@link JVMDistinctFilter}.
         * 
         * TODO Refactor to use {@link JVMDistinctFilter} directly iff possible
         * (e.g., a chain of the AALP operator followed by the DISTINCT
         * solutions operator)
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
            
        }

    } // ArbitraryLengthPathTask
        
}
