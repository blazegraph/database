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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Enumeration;
import java.util.Properties;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Eliminate semantically empty join group nodes which are the sole child of
 * another join groups. Such nodes either do not specify a context or they
 * specify the same context as the parent.
 * 
 * <pre>
 * { { ... } } => { ... }
 * </pre>
 * 
 * and for non-graph groups:
 * 
 * <pre>
 * { ... {} } => { ... }
 * </pre>
 * 
 * This is also done when the child is a UNION.
 * 
 * <pre>
 * { UNION {...} } => UNION {...}
 * </pre>
 * 
 * Or
 * 
 * <pre>
 * { GRAPH ?g {...} } => GRAPH ?g {...}
 * </pre>
 * 
 * Note: An empty <code>{}</code> matches a single empty solution. Since we
 * always push in an empty solution and the join of anything with an empty
 * solution is that source solution, this is the same as not running the group,
 * so we just eliminate the empty group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTEmptyGroupOptimizer.java 5177 2011-09-12 17:49:44Z
 *          thompsonbry $
 */
public class ASTEmptyGroupOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ASTEmptyGroupOptimizer.class);

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

            @SuppressWarnings("unchecked")
            final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                eliminateEmptyGroups(queryRoot, whereClause);
                
//                removeEmptyChildGroups((GraphPatternGroup<?>) whereClause);
                
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

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    eliminateEmptyGroups(namedSubquery, whereClause);
                    
//                    removeEmptyChildGroups((GraphPatternGroup<?>) whereClause);

                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    private static void eliminateEmptyGroups(final QueryBase queryBase, 
            final GraphPatternGroup<?> op) {

        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                eliminateEmptyGroups(queryBase, childGroup);
                
                /*
                 * If we pruned the child, then we need to decrement the index
                 * so that we don't skip one.
                 */
                if (i < op.arity() && op.get(i) != child) {
                	
                	i--;
                	
                }

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                eliminateEmptyGroups(subquery, childGroup);

                /*
                 * If we pruned the child, then we need to decrement the index
                 * so that we don't skip one.
                 */
                if (i < op.arity() && op.get(i) != child) {
                	
                	i--;
                	
                }

            }
            
        }

        final int arity = op.arity();

        if (arity == 0 && 
        		op.getContext() == null &&
        		op.getParent() != null &&
        		!isUnion(op.getParent())) {
        	
            /*
             * If this is an empty graph pattern group then we can just prune it
             * out entirely, unless it is the where clause.
             * 
             * Also, do not prune GRAPH ?g {} or GRAPH uri {}. Those
             * constructions have special semantics.
             * 
             * Note: Do not eliminate an empty join group which is the child of
             * a UNION. See http://sourceforge.net/apps/trac/bigdata/ticket/504
             * (UNION with Empty Group Pattern)
             */
    		
    		final IGroupNode<IGroupMemberNode> parent = 
    			op.getParent();
    		
    		parent.removeChild(op);
    		
        	op.setParent(null);
        	
        } else if (arity == 1 && 
        		op instanceof JoinGroupNode && 
    			op.get(0) instanceof JoinGroupNode) {

            final JoinGroupNode parent = (JoinGroupNode) op;

            final JoinGroupNode child = (JoinGroupNode) op.get(0);

            if ( (!parent.isMinus() && !child.isMinus()) 
            //  fix for trac 712
            	&& (parent.isOptional() || !child.isOptional()) 
            	) {

            	/*
                 * We can always merge two JoinGroupNodes into one, but we have
                 * to make sure we get the optionality right.
                 * 
                 * Note: This is not true for MINUS. MINUS can only be combined
                 * with a child join group which is using a normal join.
                 */

            	/*
1. JoinGroup1 [optional=false] { JoinGroup2 [optional=false] { ... } } -> JoinGroup2 [optional=false] { ... }
2. JoinGroup1 [optional=true]  { JoinGroup2 [optional=true]  { ... } } -> JoinGroup2 [optional=true]  { ... }
3. JoinGroup1 [optional=true]  { JoinGroup2 [optional=false] { ... } } -> JoinGroup2 [optional=true]  { ... }

This case 4 appears to be misconceived: Jeremy Carroll.
4. JoinGroup1 [optional=false] { JoinGroup2 [optional=true]  { ... } } -> JoinGroup2 [optional=true]  { ... }
            	 */

                if (parent.isOptional() && !child.isOptional()) {

                    child.setOptional(true);

                }

                swap(queryBase, parent, child);
        	
            }
            	
        } else if (arity == 1 && 
        		op instanceof UnionNode && 
    			op.get(0) instanceof JoinGroupNode) {
            
        	/*
        	 * We can always replace a UnionNode that has a single JoinGroupNode
        	 * with the JoinGroupNode itself.
        	 */
        	
            final UnionNode parent = (UnionNode) op;

            final JoinGroupNode child = (JoinGroupNode) op.get(0);
            
            swap(queryBase, parent, child);

        } else if (arity == 1 && 
        		op instanceof JoinGroupNode &&
        		!op.isOptional() && !op.isMinus() &&
    			op.get(0) instanceof UnionNode) {
            
        	/*
        	 * If a JoinGroupNode contains a single UnionNode, we can lift the
        	 * UnionNode unless the JoinGroupNode is optional or minus. 
        	 * The MINUS case was added as part of BLZG-852.
        	 */
            final JoinGroupNode parent = (JoinGroupNode) op;

            final UnionNode child = (UnionNode) op.get(0);
            
            swap(queryBase, parent, child);

        } else if (arity == 1 && //
                op.get(0) instanceof IBindingProducerNode && //
                op.getParent() != null && //
                !op.isOptional() && //
                !op.isMinus() && //
                !isUnion(op.getParent()) && //
//                !(((IGroupNode<?>) op.getParent()) instanceof UnionNode) &&
                !isMinus(op.getParent()) &&
//                !(((IGroupNode<?>) op.getParent()) instanceof JoinGroupNode && !(((JoinGroupNode) op
//                        .getParent()).isMinus())) &&
                op.getContext() == op.getParent().getContext()) {

            /*
             * The child is something which produces bindings (hence,
             * not a FILTER) and is neither a JoinGroupNode nor a
             * UnionNode and the operator is neither the top level of
             * the WHERE clause nor a UnionNode or MINUS.
             * 
             * Just replace the parent JoinGroupNode (op) with the
             * child.
             */
           
            // inherit query hints
            copyQueryHints(op.get(0),op.getQueryHints());
            
            ((GroupNodeBase<?>) op.getParent())
                    .replaceWith(op, (BOp) op.get(0));

        }

//        if (op instanceof GraphPatternGroup<?>) {
//
//            removeEmptyChildGroups((GraphPatternGroup<?>) op);
//            
//        }

    }

    /**
     * Return true if the operator is a UNION.
     * 
     * @param op
     *            The operator.
     */
    static private boolean isUnion(final IGroupNode<?> op) {
        if (op instanceof UnionNode)
            return true;
        return false;
    }

    /**
     * Return true if the operator is a MINUS node.
     * 
     * @param op
     *            The operator.
     */
    static private boolean isMinus(final IGroupNode<?> op) {
        if (op instanceof IJoinNode) {
            final IJoinNode g = (IJoinNode) op;
            if (g.isMinus())
                return true;
        }
        return false;
    }
    
//    /**
//     * Return true if the operator is an OPTIONAL node.
//     * 
//     * @param op
//     *            The operator.
//     */
//    private boolean isOptional(final IGroupNode<?> op) {
//        if (op instanceof IJoinNode) {
//            final IJoinNode g = (IJoinNode) op;
//            if (g.isOptional())
//                return true;
//        }
//        return false;
//    }
    
    /**
     * Swap the parent with the child inside the grandparent.  If there is no
     * grandparent, assume the parent is the where clause in the query base,
     * and replace it with the child.
     */
    static private void swap(final QueryBase queryBase, 
    		final GraphPatternGroup<?> parent, 
    		final GraphPatternGroup<?> child) {
    	
    	if (parent.getParent() == null) {
    		
    		/*
    		 * If the parent has no parent, the parent must
    		 * currently be the where clause.  Set the child to
    		 * be the new where clause instead.
    		 */
    		
    		queryBase.setWhereClause(child);
    		
    	} else {
    		
    		/*
    		 * If the parent has a parent, then remove the parent
    		 * from the grandparent and replace it with the child.
    		 */
    		
         final GroupNodeBase<?> grandparent = (GroupNodeBase<?>) parent
                    .getParent();

         // inherit query hints
         copyQueryHints(child,parent.getQueryHints());

    		grandparent.replaceWith(parent, child);
    		
    	}
    	
    	parent.setParent(null);
    	
    }

    
    /**
     * Copies the query hints to the given node, if not specified there. 
     * If the given node already has the query hint, it is left unmodified.
     * 
     * @param node
     * @param queryHints
     */
    private static void copyQueryHints(BOp node, final Properties queryHints) {

       if (queryHints == null)
          return;

       if (!(node instanceof QueryNodeBase))
           return;

       final QueryNodeBase nodeAsQN = (QueryNodeBase)node;
       final Enumeration<?> pnames = queryHints.propertyNames();

       while (pnames.hasMoreElements()) {

          final String name = (String) pnames.nextElement();

          final String value = queryHints.getProperty(name);

          // copy over query hint, ignoring conflicting ones
          final String curHint = nodeAsQN.getQueryHint(name);
          if (curHint==null) {
             nodeAsQN.setQueryHint(name, value);
          }

       }
    }
    
}
