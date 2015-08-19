/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.xalan.xsltc.compiler.util.FilterGenerator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.PathNode.PathElt;
import com.bigdata.rdf.sparql.ast.PathNode.PathSequence;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Pruning rules for unknown IVs in statement patterns:
 * 
 * If an optional join is known to fail, then remove the optional group in which
 * it appears from the group (which could be an optional group, a join group, or
 * a union).
 * 
 * If a statement pattern contains an unknown term, a with this statement
 * pattern will certainly fail. Thus the group in which the statement pattern
 * appears (the parent) will also fail. Continue recursively up the parent
 * hierarchy until we hit a UNION or an OPTIONAL parent. If we reach the root 
 * of the where clause for a subquery, then continue up the groups in which the 
 * subquery appears.
 * 
 * If the parent is a UNION, then remove the child from the UNION.
 * 
 * If a UNION has one child, then replace the UNION with the child.
 * 
 * If a UNION is empty, then fail the group in which it fails (unions are not
 * optional).
 * 
 * These rules should be triggered if a join is known to fail, which includes
 * the case of an unknown IV in a statement pattern as well
 * <code>GRAPH uri {}</code> where uri is not a named graph.
 * 
 * <pre>
 * 
 * TODO From BigdataEvaluationStrategyImpl3#945
 * 
 * Prunes the sop tree of optional join groups containing values
 * not in the lexicon.
 * 
 *         sopTree = stb.pruneGroups(sopTree, groupsToPrune);
 * 
 * 
 * If after pruning groups with unrecognized values we end up with a
 * UNION with no subqueries, we can safely just return an empty
 * iteration.
 * 
 *         if (SOp2BOpUtility.isEmptyUnion(sopTree.getRoot())) {
 *             return new EmptyIteration<BindingSet, QueryEvaluationException>();
 *         }
 * </pre>
 * 
 * and also if we encounter a value not in the lexicon, we can still continue
 * with the query if the value is in either an optional tail or an optional join
 * group (i.e. if it appears on the right side of a LeftJoin). We can also
 * continue if the value is in a UNION. Otherwise we can stop evaluating right
 * now.
 * 
 * <pre>
 *                 } catch (UnrecognizedValueException ex) {
 *                     if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
 *                         throw new UnrecognizedValueException(ex);
 *                     } else {
 *                         groupsToPrune.add(sopTree.getGroup(sop.getGroup()));
 *                     }
 *                 }
 * </pre>
 * 
 * 
 * ASTPruneUnknownTerms : If an unknown terms appears in a StatementPatternNode
 * then we get to either fail the query or prune that part of the query. If it
 * appears in an optional, then prune the optional. if it appears in union, the
 * prune that part of the union. if it appears at the top-level then there are
 * no solutions for that query. This is part of what
 * BigdataEvaluationStrategyImpl3#toPredicate(final StatementPattern
 * stmtPattern) is doing. Note that toVE() as called from that method will throw
 * an UnknownValueException if the term is not known to the database.
 * 
 * FIXME Isolate pruning logic since we need to use it in more than one place.
 */
public class ASTUnresolvedTermsOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTUnresolvedTermsOptimizer.class);

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // Main WHERE clause
        {

            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                resolveGroupsWithUnknownTerms(context, queryRoot, whereClause);
                
            }

        }

        // Named subqueries
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (int i = 0; i < namedSubqueries.size(); i++) {

                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    resolveGroupsWithUnknownTerms(context, queryRoot, whereClause);
                    
                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * If the group has an unknown term, resolve it.
     * @param context 
     * 
     * @param op
     */
    private static void resolveGroupsWithUnknownTerms(AST2BOpContext context, final QueryRoot queryRoot,
            final GroupNodeBase<IGroupMemberNode> op) {

    	/*
    	 * Check the statement patterns.
    	 */
    	for (int i = 0; i < op.arity(); i++) {
    		
    		final BOp sp = op.get(i);
    		
    		for (int j = 0; j < sp.arity(); j++) {
    			
    			final BOp bop = sp.get(j);
    			
   				fillInIV(context, bop);
    			
    		}
    		
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GroupNodeBase<?>) {

                @SuppressWarnings("unchecked")
                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) child;

                resolveGroupsWithUnknownTerms(context, queryRoot, childGroup);

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) subquery
                        .getWhereClause();

                resolveGroupsWithUnknownTerms(context, queryRoot, childGroup);

            }

        }

    }

    private static void fillInIV(AST2BOpContext context, final BOp bop) {
        if (bop instanceof ConstantNode) {
            
            final BigdataValue value = ((ConstantNode) bop).getValue();
            final IV iv = value.getIV();
            
            if (iv == null || iv.isNullIV()) {
                
                BigdataValue resolved = context.getAbstractTripleStore().getValueFactory().asValue(value);
                IV resolvedIV;
                if (resolved.getIV()==null) {
                    resolvedIV = context.getAbstractTripleStore().getIV(resolved);
                    if (resolvedIV!=null) {
                        resolved.setIV(resolvedIV);
                        resolvedIV.setValue(resolved);
                        ((ConstantNode) bop).setArg(0, new Constant(resolvedIV));
                    }
                }
            }
        }else if (bop instanceof PathNode) {
            PathAlternative path = ((PathNode) bop).getPathAlternative();
            for (int k = 0; k < path.arity(); k++) {
                BOp pathBop = path.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof PathSequence || bop instanceof PathElt) {
            for (int k = 0; k < bop.arity(); k++) {
                BOp pathBop = bop.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof FilterNode) {
            IValueExpression<? extends IV> ve = ((FilterNode)bop).getValueExpression();
            for (int k = 0; k < ve.arity(); k++) {
                BOp pathBop = ve.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof FunctionNode) {
            IValueExpression<? extends IV> ve = ((FunctionNode)bop).getValueExpression();
            if (ve instanceof IVValueExpression) {
                for (int k = 0; k < ve.arity(); k++) {
                    BOp pathBop = ve.get(k);
                    if (pathBop instanceof Constant && ((Constant)pathBop).get() instanceof TermId) {
                        BigdataValue v = ((TermId) ((Constant)pathBop).get()).getValue();
                        BigdataValue resolved = context.getAbstractTripleStore().getValueFactory().asValue(v);
                        IV resolvedIV;
                        if (resolved.getIV()==null) {
                            resolvedIV = context.getAbstractTripleStore().getIV(resolved);
                            if (resolvedIV!=null) {
                                resolved.setIV(resolvedIV);
                                resolvedIV.setValue(resolved);
                                Constant newConstant = new Constant(resolvedIV);
                                ve = (IValueExpression<? extends IV>) ((IVValueExpression)ve).setArg(k, newConstant);
                                ((FunctionNode) bop).setArg(k, new ConstantNode(newConstant));
                            }
                        } else {
                            fillInIV(context,pathBop); 
                        }
                    }
                }
                ((FunctionNode)bop).setValueExpression(ve);
            } else if (ve instanceof Constant) {
                fillInIV(context,ve); 
            }
        }
    }

}
