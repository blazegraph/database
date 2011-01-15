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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.solutions.SliceOp;
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
    	
    	return !(isUnion(sopGroup) || isOptional(sopGroup));
    	
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
    	 * These are constraints that use variables from non-optional parent
    	 * join groups, and thus should be translated into ConditionalRoutingOps
    	 * for maximum efficiency.
    	 */
    	final Collection<IConstraint> conditionals = 
    		new LinkedList<IConstraint>();
    	
    	final IRule rule = rule(join, conditionals);
    	
    	PipelineOp left = Rule2BOpUtility.convert(
    			rule, conditionals, idFactory, db, queryEngine, queryHints);
    	
    	/*
    	 * Start with left=<this join group> and add a SubqueryOp for each
    	 * sub group.
    	 */
    	
    	final SOpGroups children = join.getChildren();
    	if (children != null) {
	    	for (SOpGroup child : join.getChildren()) {
	    		if (isSingleOptional(child)) {
	    			// handled by the rule() conversion above
	    			continue;
	    		}
	    		final PipelineOp subquery = convert(
	    				child, idFactory, db, queryEngine, queryHints);
	    		final boolean optional = isOptional(child);
	    		final int subqueryId = idFactory.incrementAndGet();
	    		left = new SubqueryOp(new BOp[]{left}, 
	                    new NV(Predicate.Annotations.BOP_ID, subqueryId),//
	                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),//
	                    new NV(SubqueryOp.Annotations.OPTIONAL,optional)//
	            );
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
                            BOpEvaluationContext.CONTROLLER)));
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
        anns.add(new NV(Union.Annotations.BOP_ID, thisId));
        anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
                BOpEvaluationContext.CONTROLLER));
        anns.add(new NV(Union.Annotations.CONTROLLER, true));
        
        final Union thisOp = new Union(args, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        
        return thisOp;

    }

    protected static IRule rule(final SOpGroup group,
    		final Collection<IConstraint> conditionals) {
    	
    	final Collection<IPredicate> preds = new LinkedList<IPredicate>();
    	final Collection<IConstraint> constraints = new LinkedList<IConstraint>();
    	
    	/*
    	 * Gather up all the variables used by non-optional parent join groups
    	 */
    	final Set<IVariable<?>> variables = new HashSet<IVariable<?>>();
    	SOpGroup parent = group;
    	while ((parent = parent.getParent()) != null) {
    		if (isNonOptionalJoinGroup(parent))
    			collectPredicateVariables(variables, parent);
    	}
    	
    	for (SOp sop : group) {
    		final BOp bop = sop.getBOp();
    		if (bop instanceof IPredicate) {
    			preds.add((IPredicate) bop);
    		} else if (bop instanceof IConstraint) {
    			final IConstraint c = (IConstraint) bop;
    			/*
    			 * This constraint is a conditional if all of its variables
    			 * appear in non-optional parent join groups
    			 */
                final Iterator<IVariable<?>> vars = 
                	BOpUtility.getSpannedVariables(c);
                boolean conditional = true;
                while (vars.hasNext()) {
                    final IVariable<?> v = vars.next();
                    conditional &= variables.contains(v);
                }
    			if (conditional)
    				conditionals.add(c);
    			else
    				constraints.add(c);
    		} else {
    			throw new IllegalArgumentException("illegal operator: " + sop);
    		}
    	}
    	
    	/*
    	 * The way that the Sesame operator tree is parsed, optional tails
    	 * become single-operator (predicate) join groups without any children
    	 * of their own.
    	 */
    	final SOpGroups children = group.getChildren();
    	if (children != null) {
	    	for (SOpGroup child : group.getChildren()) {
	    		if (isSingleOptional(child)) {
	    			final SOp sop = child.getSingletonSOp();
	    			final BOp bop = sop.getBOp();
					final IPredicate pred = (IPredicate) bop.setProperty(
							IPredicate.Annotations.OPTIONAL, Boolean.TRUE);
					preds.add(pred);
	    		}
	    	}
    	}
    	
    	final IVariable<?>[] required = group.getTree().getRequiredVars();
    	
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
    	
		return rule;
		
    }
    
}
