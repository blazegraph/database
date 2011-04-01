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
 * Created on Sep 5, 2010
 */

package com.bigdata.rdf.sail.sop;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.SubqueryHashJoinOp;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.sail.FreeTextSearchExpander;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;

public class SOp2BOpUtility {

    protected static final Logger log = Logger.getLogger(SOp2BOpUtility.class);

    public static PipelineOp convert(final SOpTree sopTree,
    		final AtomicInteger idFactory, final AbstractTripleStore db,
    		final QueryEngine queryEngine, final Properties queryHints) {
    	
    	if (log.isDebugEnabled()) {
    		log.debug("converting:\n" + sopTree);
    	}

    	final SOpGroup root = sopTree.getRoot();
    	return convert(root, idFactory, db, queryEngine, queryHints);
    	
    }
    
    public static PipelineOp convert(final SOpGroup sopGroup, 
    		final AtomicInteger idFactory, final AbstractTripleStore db,
    		final QueryEngine queryEngine, final Properties queryHints) {
    
    	if (isUnion(sopGroup)) {
			return union(sopGroup, idFactory, db, queryEngine, queryHints);
		} else {
			return join(sopGroup, idFactory, db, queryEngine, queryHints);
    	}
    	
    }
    
    /**
     * A Union always appears as a single Sesame operator within a group. Its
     * children are the things being Unioned together.
     */
    private static boolean isUnion(final SOpGroup sopGroup) {
    	
    	if (sopGroup.size() == 1) {
    		final SOp sop = sopGroup.getSingletonSOp();
    		return sop.getOperator() instanceof org.openrdf.query.algebra.Union;
    	}
    	return false;
    	
	}
    
    public static boolean isEmptyUnion(final SOpGroup sopGroup) {
    	
    	if (isUnion(sopGroup)) {
    		final SOpGroups children = sopGroup.getChildren();
    		if (children == null || children.size() == 0) {
    			return true;
    		}
    	}
    	return false;
    	
    }
    
    /**
     * Because of the way we parse the Sesame operator tree, the single
     * optional tails get placed in their own singleton subgroup without any
     * child subgroups of their own, and always on the right side of a LeftJoin.
     */
    private static boolean isSingleOptional(final SOpGroup sopGroup) {

    	if (sopGroup.size() == 1 && sopGroup.getChildren() == null) {
    		final SOp sop = sopGroup.getSingletonSOp();
    		return (sop.getOperator() instanceof StatementPattern) &&
    			sop.isRightSideLeftJoin();
    	}
    	return false;
    	
    }
    
    private static boolean isOptional(final SOpGroup sopGroup) {
    	
    	if (sopGroup.size() == 0) {
    		throw new IllegalArgumentException();
    	}
    	final SOp sop = sopGroup.iterator().next();
    	return sop.isRightSideLeftJoin();
    	
    }
    
    private static boolean isNonOptionalJoinGroup(final SOpGroup sopGroup) {
    	
    	return sopGroup.size() > 0 && 
    		!(isUnion(sopGroup) || isOptional(sopGroup));
    	
    }
    
    private static void collectPredicateVariables(
    		final Collection<IVariable<?>> variables, final SOpGroup group) {
    	
    	for (SOp sop : group) {
    		final BOp bop = sop.getBOp();
    		if (bop instanceof IPredicate) {
                for (BOp arg : bop.args()) {
                    if (arg instanceof IVariable<?>) {
                        final IVariable<?> v = (IVariable<?>) arg;
                        variables.add(v);
                    }
                }
    		}
    	}
 	
    }
    

    protected static PipelineOp join(final SOpGroup join, 
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

		/*
		 * These are constraints that use variables bound by optionals or
		 * subqueries, and thus cannot be attached to the non-optional
		 * predicates in this group. They are handled by ConditionalRoutingOps
		 * at the end of the group, after the subqueries have run.
		 */
    	final Collection<IConstraint> postConditionals = 
    		new LinkedList<IConstraint>();
    	
    	PipelineOp left = rule2BOp(join, postConditionals, 
    			idFactory, db, queryEngine, queryHints);
    	
//    	PipelineOp left = Rule2BOpUtility.convert(
//    			rule, preConditionals, idFactory, db, queryEngine, queryHints);
    	
    	/*
    	 * Start with left=<this join group> and add a SubqueryOp for each
    	 * sub group.
    	 */
//    	final SOpGroups children = join.getChildren();
//    	if (children != null) {
//	    	for (SOpGroup child : children) {
//	    		if (isSingleOptional(child)) {
//	    			// handled by the rule() conversion above
//	    			continue;
//	    		}
//	    		final PipelineOp subquery = convert(
//	    				child, idFactory, db, queryEngine, queryHints);
//	    		final boolean optional = isOptional(child);
//	    		final int subqueryId = idFactory.incrementAndGet();
//	    		left = new SubqueryOp(new BOp[]{left}, 
//	                    new NV(Predicate.Annotations.BOP_ID, subqueryId),//
//	                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),//
//	                    new NV(SubqueryOp.Annotations.OPTIONAL,optional)//
//	            );
//	    		if (log.isInfoEnabled()) {
//	    			log.info("adding a subquery: " + subqueryId + "\n" + left);
//	    		}
//	    	}
//    	}
    	
    	final SOpGroups children = join.getChildren();
    	if (children != null) {
        	/*
        	 * First do the non-optional subqueries (UNIONs) 
        	 */
	    	for (SOpGroup child : children) {
	    		if (!isUnion(child) || isEmptyUnion(child))
	    			continue;
	    		
	    		final boolean optional = isOptional(child);
	    		final PipelineOp subquery = union(
	    				child, idFactory, db, queryEngine, queryHints);
	    		final int subqueryId = idFactory.incrementAndGet();
	    		left = new SubqueryOp(new BOp[]{left}, 
	                    new NV(Predicate.Annotations.BOP_ID, subqueryId),//
	                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),//
	                    new NV(SubqueryOp.Annotations.OPTIONAL, optional)//
	            );
	    		if (log.isInfoEnabled()) {
	    			log.info("adding a subquery: " + subqueryId + "\n" + left);
	    		}
	    	}

	    	/*
	    	 * Next do the optional subqueries and optional tails 
	    	 */
	    	for (SOpGroup child : children) {
	    		if (isUnion(child))
	    			continue;
	    		
	    		if (isSingleOptional(child)) {
	    			final SOp sop = child.getSingletonSOp();
	    			final BOp bop = sop.getBOp();
					Predicate pred = (Predicate) bop.setProperty(
							IPredicate.Annotations.OPTIONAL, Boolean.TRUE);
					pred = pred.setBOpId(idFactory.incrementAndGet());
					left = Rule2BOpUtility.join(
							queryEngine, left, pred, 
							idFactory, 
							queryHints);
	    		} else {
		    		final PipelineOp subquery = convert(
		    				child, idFactory, db, queryEngine, queryHints);
		    		final boolean optional = isOptional(child);
		    		final int subqueryId = idFactory.incrementAndGet();
		    		left = new SubqueryOp(new BOp[]{left}, 
		                    new NV(Predicate.Annotations.BOP_ID, subqueryId),//
		                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),//
		                    new NV(SubqueryOp.Annotations.OPTIONAL,optional)//
		            );
		    		if (log.isInfoEnabled()) {
		    			log.info("adding a subquery: " + subqueryId + "\n" + left);
		    		}
	    		}
	    	}
    	}
    
	    for (IConstraint c : postConditionals) {
    		final int condId = idFactory.incrementAndGet();
            final PipelineOp condOp = 
            	new ConditionalRoutingOp(new BOp[]{left},
                    NV.asMap(new NV[]{//
                        new NV(BOp.Annotations.BOP_ID,condId),
                        new NV(ConditionalRoutingOp.Annotations.CONDITION, c),
                    }));
            left = condOp;
            if (log.isDebugEnabled()) {
            	log.debug("adding post-conditional routing op: " + condOp);
            }
	    }
    	
		if (!left.getEvaluationContext()
				.equals(BOpEvaluationContext.CONTROLLER)
				&& !(left instanceof SubqueryOp)) {
			/*
			 * Wrap with an operator which will be evaluated on the query
			 * controller so the results will be streamed back to the query
			 * controller in scale-out.
			 * 
			 * @todo For scale-out, we probably need to stream the results back
			 * to the node from which the subquery was issued. If the subquery
			 * is issued against the local query engine where the IBindingSet
			 * was produced, then the that query engine is the query controller
			 * for the subquery and a SliceOp on the subquery would bring the
			 * results for the subquery back to that query controller. There is
			 * no requirement that the query controller for the subquery and the
			 * query controller for the parent query be the same node. [I am not
			 * doing this currently in order to test whether there is a problem
			 * with SliceOp which interactions with SubqueryOp to allow
			 * incorrect termination under some circumstances.
			 */
            left = new SliceOp(new BOp[] { left }, NV.asMap(//
					new NV(BOp.Annotations.BOP_ID, idFactory
							.incrementAndGet()), //
					new NV(BOp.Annotations.EVALUATION_CONTEXT,
							BOpEvaluationContext.CONTROLLER),//
					new NV(PipelineOp.Annotations.SHARED_STATE, true)//
			));
        }
    	
    	return left;
    	
    }
    
    public static Union union(final SOpGroup union,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

		final SOpGroups children = union.getChildren();
		if (children == null) {
			throw new IllegalArgumentException();
		}
		
        // The bopId for the UNION or STEP.
        final int thisId = idFactory.incrementAndGet();
        final int arity = children.size();
        final BOp[] args = new BOp[arity];

        int i = 0;
        for (SOpGroup child : children) {
            // convert the child IStep
			args[i++] = convert(child, idFactory, db, queryEngine, queryHints);
        }
        
        final LinkedList<NV> anns = new LinkedList<NV>();
        anns.add(new NV(BOp.Annotations.BOP_ID, thisId));
        anns.add(new NV(Union.Annotations.SUBQUERIES,args));
//        anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
//                BOpEvaluationContext.CONTROLLER));
//        anns.add(new NV(Union.Annotations.CONTROLLER, true));
        
        final Union thisOp = new Union(new BOp[]{}, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        
        return thisOp;

    }

    protected static PipelineOp rule2BOp(final SOpGroup group,
    		final Collection<IConstraint> postConditionals,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {
    	
    	/*
    	 * Gather up all the variables used by predicates in non-optional parent
    	 * join groups
    	 * 
    	 * TODO fix this so that is captures variables used by non-optional
    	 * subqueries from parent join groups
    	 */
    	final Set<IVariable<?>> knownBound = new HashSet<IVariable<?>>();
    	SOpGroup parent = group;
    	while ((parent = parent.getParent()) != null) {
    		if (isNonOptionalJoinGroup(parent))
    			collectPredicateVariables(knownBound, parent);
    	}
    	
    	/*
    	 * Gather up all the predicates in this group.
    	 */
    	final Collection<Predicate> preds = new LinkedList<Predicate>();
    	for (SOp sop : group) {
    		final BOp bop = sop.getBOp();
    		if (bop instanceof Predicate) {
    			preds.add((Predicate) bop);
    		}
    	}
    	
    	/*
    	 * Gather up all the variables used by predicates in this group
    	 */
    	final Set<IVariable<?>> groupVars = new HashSet<IVariable<?>>();
    	for (IPredicate bop : preds) {
	        for (BOp arg : bop.args()) {
	            if (arg instanceof IVariable<?>) {
	                final IVariable<?> v = (IVariable<?>) arg;
	                groupVars.add(v);
	            }
	        }
    	}

    	/*
    	 * Gather up the constraints, segregating into three categories:
    	 * -constraints: all variables used by predicates in this group
    	 * -pre-conditionals: all variables already bound by parent group(s)
    	 * -post-conditionals: some or all variables bound in subqueries
    	 */
    	final Collection<IConstraint> constraints = 
    		new LinkedList<IConstraint>();
    	/*
    	 * These are constraints that use variables from non-optional parent
    	 * join groups, and thus should be translated into ConditionalRoutingOps
    	 * for maximum efficiency.
    	 */
    	final Collection<IConstraint> preConditionals = 
    		new LinkedList<IConstraint>();

    	for (SOp sop : group) {
    		final BOp bop = sop.getBOp();
    		if (bop instanceof IConstraint) {
    			final IConstraint c = (IConstraint) bop;

    			{ // find the pre-conditionals
    				
	    			final Iterator<IVariable<?>> constraintVars = 
	                	BOpUtility.getSpannedVariables(c);
	
	    			/*
	    			 * This constraint is a pre-conditional if all of its variables
	    			 * appear in non-optional parent join groups
	    			 */
	                boolean preConditional = true;
	                while (constraintVars.hasNext()) {
	                    final IVariable<?> v = constraintVars.next();
	                    preConditional &= knownBound.contains(v);
	                }
	    			if (preConditional) {
	    				preConditionals.add(c);
	    				continue;
	    			}
	    			
    			}
    			
    			{ // find the post-conditionals
    				
	    			final Iterator<IVariable<?>> constraintVars = 
	                	BOpUtility.getSpannedVariables(c);
	
	    			/*
	    			 * This constraint is a post-conditional if not all of its 
	    			 * variables appear in this join group or non-optional parent 
	    			 * groups (bound by subqueries)
	    			 */
	    			boolean postConditional = false;
	                while (constraintVars.hasNext()) {
	                    final IVariable<?> v = constraintVars.next();
	                    if (!knownBound.contains(v) &&
	                    		!groupVars.contains(v)) {
	                    	postConditional = true;
	                    	break;
	                    }
	                }
	                if (postConditional) {
	    				postConditionals.add(c);
	    				continue;
	                }
	    			
    			}

    			/*
    			 * Neither pre nor post conditional, but a constraint on the
    			 * predicates in this group. done this roundabout way for the 
    			 * benefit of the RTO
    			 */
				constraints.add(c);
    		}
    	}
    	
    	/*
    	 * In the case of multiple free text searches, it is often better to
    	 * use a hash join operator to avoid a heinous cross product of the
    	 * results from the free text searches. 
    	 */
    	final Map<IVariable<?>, Collection<Predicate>> hashJoins =
    		new LinkedHashMap<IVariable<?>, Collection<Predicate>>();
    	final Collection<IVariable<?>> boundByHashJoins =
    		new LinkedList<IVariable<?>>();
    	if (true) { // maybe check query hints for this?

    		int numSearches = 0;
    		{ // first count the searches
    			for (IPredicate pred : preds) {
    				if (isFreeTextSearch(pred))
    					numSearches++;
    			}
    		}
    		if (numSearches > 1) { 
    			{ // collect them up
	    			final Iterator<Predicate> it = preds.iterator();
	    			while (it.hasNext()) {
	    				final Predicate pred = it.next();
	    				if (isFreeTextSearch(pred)) {
	    					// we're going to handle these separately
	    					it.remove();
	    					// create a hash group for this variable
	    					final IVariable v = (IVariable) pred.get(0);
	    					if (hashJoins.containsKey(v)) {
	    						throw new IllegalArgumentException(
	    								"multiple free text searches using the same variable!!");
	    					}
	    					final Collection<Predicate> hashGroup =
	    						new LinkedList<Predicate>();
	    					hashGroup.add(pred);
	    					hashJoins.put(v, hashGroup);
	    					// add this search variables to the list of known
	    					// bound variables
	    					boundByHashJoins.add(v);
	    				}
	    			}
    			}
    			{ // collect up other predicates that use the search vars
	    			final Iterator<Predicate> it = preds.iterator();
	    			while (it.hasNext()) {
	    				final Predicate pred = it.next();
	    				// search always binds to a literal, which can only be
	    				// used as the 2nd arg (the object)
	                    final BOp obj = pred.get(2);
                        if (obj instanceof IVariable<?>) {
                            final IVariable<?> v = (IVariable<?>) obj;
                            if (hashJoins.containsKey(v)) {
    	    					// we're going to handle these separately
    	    					it.remove();
    	    					// add this predicate to the hash group
    	    					hashJoins.get(v).add(pred);
    	    					// add any other variables used by this tail to 
    	    					// the list of known bound variables
    	    					for (BOp arg : pred.args()) {
    	    						if (arg instanceof IVariable<?>) {
    	    							boundByHashJoins.add((IVariable<?>) arg);
    	    						}
    	    					}
                            }
                        }
	    			}    				
    			}
    		}
    		
    	}
    	
    	final IVariable<?>[] required = group.getTree().getRequiredVars();
    	
    	if (log.isInfoEnabled()) {
    		log.info("preds: " + Arrays.toString(preds.toArray()));
    		log.info("constraints: " + Arrays.toString(constraints.toArray()));
    		log.info("preConds: " + Arrays.toString(preConditionals.toArray()));
    		log.info("postConds: " + Arrays.toString(postConditionals.toArray()));
    	}
    	
        PipelineOp left = Rule2BOpUtility.applyQueryHints(
        		new StartOp(BOpBase.NOARGS,
			        NV.asMap(new NV[] {//
			              new NV(Predicate.Annotations.BOP_ID, idFactory
			                      .incrementAndGet()),//
			              new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
			                      BOpEvaluationContext.CONTROLLER),//
			        })),queryHints);

		if (preConditionals != null) { // @todo lift into CONDITION on SubqueryOp
			for (IConstraint c : preConditionals) {
				final int condId = idFactory.incrementAndGet();
		      final PipelineOp condOp = Rule2BOpUtility.applyQueryHints(
		      	new ConditionalRoutingOp(new BOp[]{left},
		              NV.asMap(new NV[]{//
		                  new NV(BOp.Annotations.BOP_ID,condId),
		                  new NV(ConditionalRoutingOp.Annotations.CONDITION, c),
		              })), queryHints);
		      left = condOp;
		      if (log.isDebugEnabled()) {
		      	log.debug("adding conditional routing op: " + condOp);
		      }
			}
		}

		if (hashJoins.size() > 0) {
			final Set<IVariable<?>> lastVars = new LinkedHashSet<IVariable<?>>();
			final Set<IVariable<?>> joinVars = new LinkedHashSet<IVariable<?>>();
			int i = 0;
			for (Collection<Predicate> hashGroup : hashJoins.values()) {
				joinVars.clear();
				if (lastVars.size() > 0) {
					for (Predicate pred : hashGroup) {
						for (BOp arg : pred.args()) {
							if (arg instanceof IVariable<?>) {
								final IVariable<?> v = (IVariable<?>) arg;
								if (lastVars.contains(v)) {
									joinVars.add(v);
								}
							}
						}
					}
				}
				lastVars.clear();
				for (Predicate pred : hashGroup) {
					for (BOp arg : pred.args()) {
						if (arg instanceof IVariable<?>) {
							final IVariable<?> v = (IVariable<?>) arg;
							lastVars.add(v);
						}
					}
				}
				
				if (i == 0) {
					left = convert(hashGroup, constraints, left, knownBound, 
							idFactory, db, queryEngine, queryHints);
				} else {
					final PipelineOp subquery = convert(hashGroup, constraints, 
							null/*left*/, knownBound, 
							idFactory, db, queryEngine, queryHints);
					final IVariable<?>[] joinVarsArray = 
							joinVars.toArray(new IVariable[joinVars.size()]);
					
					if (log.isInfoEnabled()) {
						log.info(Arrays.toString(joinVarsArray));
						log.info(subquery);
					}
					
					left = new SubqueryHashJoinOp(new BOp[]{left},
	                	new NV(Predicate.Annotations.BOP_ID, idFactory.incrementAndGet()),//
	                	new NV(SubqueryHashJoinOp.Annotations.PIPELINED, false),//
	                	new NV(SubqueryHashJoinOp.Annotations.SUBQUERY, subquery),//
	                	new NV(SubqueryHashJoinOp.Annotations.JOIN_VARS, joinVarsArray));
	                	
				}
				i++;
			}
		}
		
		if (preds.size() > 0) {
			
			final IRule rule = new Rule(
					"dummy rule",
					null, // head
					preds.toArray(new IPredicate[preds.size()]),
					QueryOptions.NONE,
					// constraints on the rule.
					constraints.size() > 0 ? 
							constraints.toArray(
									new IConstraint[constraints.size()]) : null,
					null/* constants */, null/* taskFactory */, 
					required);    	
	    	
			left = Rule2BOpUtility.convert(
					rule, left, knownBound, 
					idFactory, db, queryEngine, queryHints);
			
		}

		return left;
		
    }
    
    protected static final boolean isFreeTextSearch(final IPredicate pred) {
    	return pred.getAccessPathExpander() 
			instanceof FreeTextSearchExpander;
    }
    
    protected static final PipelineOp convert(
    		final Collection<Predicate> preds,
    		final Collection<IConstraint> constraints,
    		final PipelineOp pipelineOp,
    		final Set<IVariable<?>> knownBound,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

    	PipelineOp left = pipelineOp;
    	if (left == null) {
    		left = Rule2BOpUtility.applyQueryHints(
            		new StartOp(BOpBase.NOARGS,
        			        NV.asMap(new NV[] {//
        			              new NV(Predicate.Annotations.BOP_ID, idFactory
        			                      .incrementAndGet()),//
        			              new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
        			                      BOpEvaluationContext.CONTROLLER),//
        			        })),queryHints);
    	}
    	
        /*
         * Analyze the predicates and constraints to decide which constraints
         * will run with which predicates.  @todo does not handle optionals
         * correctly, but we do not pass optionals in to Rule2BOpUtility
         * from SOp2BOpUtility anymore so ok for now
         */
        final IConstraint[][] assignedConstraints;
		{
			final int nknownBound = knownBound.size();
			
			// figure out which constraints are attached to which
			// predicates.
			assignedConstraints = PartitionedJoinGroup.getJoinGraphConstraints(
					preds.toArray(new Predicate[preds.size()]), 
					constraints.toArray(new IConstraint[constraints.size()]),
					nknownBound == 0 ? IVariable.EMPTY : knownBound
							.toArray(new IVariable<?>[nknownBound]), 
					false// pathIsComplete
					);
		}

		final BOpContextBase context = new BOpContextBase(queryEngine);
		
		/*
         * 
         */
		int i = 0;
        for (Predicate<?> pred : preds) {
            left = Rule2BOpUtility.join(queryEngine, left, pred,//
                    Arrays.asList(assignedConstraints[i++]), //
                    context, idFactory, queryHints);

        }
        
        return left;
    	
    }

    
}
