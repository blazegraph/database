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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTBasicGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTBind;
import com.bigdata.rdf.sail.sparql.ast.ASTBindingSet;
import com.bigdata.rdf.sail.sparql.ast.ASTConstraint;
import com.bigdata.rdf.sail.sparql.ast.ASTConstruct;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphPatternGroup;
import com.bigdata.rdf.sail.sparql.ast.ASTHavingClause;
import com.bigdata.rdf.sail.sparql.ast.ASTInlineData;
import com.bigdata.rdf.sail.sparql.ast.ASTLet;
import com.bigdata.rdf.sail.sparql.ast.ASTMinusGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTNamedSubqueryInclude;
import com.bigdata.rdf.sail.sparql.ast.ASTOptionalGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTServiceGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTTRefPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTUnionGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTVar;
import com.bigdata.rdf.sail.sparql.ast.ASTWhereClause;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.QuadsOperationInTriplesModeException;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Visitor handles the <code>GroupGraphPattern</code> production (aka the
 * "WHERE" clause). This includes <code>SubSelect</code> and
 * <code>GraphPatternNotTriples</code>. ASTWhereClause has GroupGraphPattern
 * child which is a (SelectQuery (aka subquery)), GraphPattern
 * (BasicGraphPattern aka JoinGroup or GraphPatternNotTriples). The
 * <code>TriplesBlock</code> is handled by the {@link TriplePatternExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: GroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 * @openrdf
 */
public class GroupGraphPatternBuilder extends TriplePatternExprBuilder {

    private static final Logger log = Logger.getLogger(GroupGraphPatternBuilder.class);
    
    public GroupGraphPatternBuilder(final BigdataASTContext context) {

        super(context);

        this.graphPattern = new GroupGraphPattern();

    }

    /**
     * CONSTRUCT (handled as a TriplesBlock).
     */
    @Override
    final public ConstructNode visit(final ASTConstruct node, final Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;
        
        graphPattern = new GroupGraphPattern();//parentGP); Note: No parent.

        // visit the children of the node (default behavior).
        super.visit(node, null);

        final ConstructNode group = graphPattern.buildGroup(new ConstructNode());
        
        graphPattern = parentGP;

        return group;
        
    }

    /**
     * <code>( SelectQuery | GraphPattern )</code> - this is the common path for
     * SubSelect and graph patterns.
     * <p>
     * Note: (NOT) EXISTS uses a temporary graph pattern in order to avoid the
     * side-effect on the parent's graph pattern. Other value functions with
     * inner graph patterns should do this as well.
     * 
     * @return The {@link GroupNodeBase}. This return value is used by the
     *         visitor method for the {@link ASTWhereClause} and also in SPARQL
     *         UPDATE.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/562">
     *      Sub-select in INSERT cause NPE in UpdateExprBuilder </a>
     */
    @Override
    final public GroupNodeBase<?> visit(final ASTGraphPatternGroup node,
            Object data) throws VisitorException {

        if (log.isInfoEnabled()) {
            log.info("\ndepth=" + depth(node) + ", parentGP(in)="
                    + graphPattern + "\n" + node.dump(indent(node)));
        }

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);

        /* Visit the children of the node (default behavior).  Note: [ret] will be null in many cases with a side effect on [graphPattern]. */
        final Object ret = super.visit(node, null);
        final GroupNodeBase<?> ret2; 

        if (ret instanceof SubqueryRoot) {

            final SubqueryRoot subqueryRoot = (SubqueryRoot) ret;

            if (node.jjtGetParent() instanceof ASTWhereClause) {
                
                /**
                 * SubSelect as the WHERE clause. In this case the outer graph
                 * pattern group is a JoinGroupNode the SubSelect is embedded
                 * within an inner JoinGroupNode.
                 * 
                 * <pre>
                 * SELECT * { SELECT * { ?s ?p ?o } }
                 * </pre>
                 */
                
                graphPattern = new GroupGraphPattern(parentGP);
                
                graphPattern.add(new JoinGroupNode(subqueryRoot));
                
                @SuppressWarnings("rawtypes")
                final GroupNodeBase group = graphPattern
                        .buildGroup(new JoinGroupNode());

                ret2 = group;

            } else {

                /**
                 * SubSelect embedded under the WHERE clause within its own
                 * graph pattern group by the { SELECT ... } syntax. For example
                 * 
                 * <pre>
                 * SELECT ?s { ?s ?x ?o . {SELECT ?x where {?x ?p ?x}}}
                 * </pre>
                 * 
                 * Note: This no longer returns [null] due to the problem
                 * cited in the ticket below.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/562">
                 *      Sub-select in INSERT cause NPE in UpdateExprBuilder </a>
                 */
                
                @SuppressWarnings("rawtypes")
                final GroupNodeBase group = new JoinGroupNode(subqueryRoot);

                parentGP.add(group);
                
                ret2 = group;
                
            }

        } else {

//            if (node.jjtGetParent() instanceof ASTWhereClause
//                    && graphPattern.isSimpleJoinGroup()) {
//
//                /*
//                 * If the sole child is a non-optional join group without a
//                 * graph context, then it is elimated. This handles cases like a
//                 * top-level UNION which would otherwise be nested as
//                 * JoinGroupNode(JoinGroupNode(UnionNode(...))).
//                 */
//
//                final JoinGroupNode group = (JoinGroupNode) graphPattern.get(0);
//
//                parentGP.add(group);
//
//                ret2 = group;
//                
//            } else {

                final JoinGroupNode joinGroup = new JoinGroupNode();

                /*
                 * We look up to the first ASTGraphGraphPattern ancestor,
                 * which defines the context for the given node
                 */
                ASTGraphGraphPattern scopePattern = 
                   firstASTGraphGraphAncestor(node.jjtGetParent());
                if (scopePattern!=null) {
                   Node child = scopePattern.jjtGetChild(0);
                   if (child!=null) {
                      final TermNode s = 
                         (TermNode) scopePattern.jjtGetChild(0).jjtAccept(this, data);
                      
                      if (s!=null) 
                         joinGroup.setContext(s);
                   }
                }

                @SuppressWarnings("rawtypes")
                final GroupNodeBase group = graphPattern.buildGroup(joinGroup);

                parentGP.add(group);

                ret2 = group;

//            }
            
        }

        if (log.isInfoEnabled())
            log.info("\ndepth=" + depth(node) + ", graphPattern(out)="
                    + graphPattern);

        graphPattern = parentGP;

        return ret2;

    }

    /**
     * Note: while openrdf lifts the filters out of the optional, we do not need
     * to do this per MikeP.
     */
    @Override
    final public Void visit(final ASTOptionalGraphPattern node, Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;
        
        graphPattern = new GroupGraphPattern(parentGP);

        // visit the children.
        final Object tmp = super.visit(node, null);

        final JoinGroupNode joinGroup = new JoinGroupNode();

        joinGroup.setOptional(true);

        if (tmp instanceof SubqueryRoot) {
            
            /**
             * Sub-Select
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/806>
             * Incorrect computation of shared variables when lifting out named
             * subqueries </a>
             */
            joinGroup.addChild((SubqueryRoot) tmp);

        } else {

            // GraphPattern
            
            @SuppressWarnings("rawtypes")
            final GroupNodeBase group = graphPattern.buildGroup(joinGroup);
            
            assert group == joinGroup;// should be the same reference.

        }

        parentGP.add(joinGroup);

        graphPattern = parentGP;

        return null;

    }

    /**
     * MINUS is modeled very much like OPTIONAL in the bigdata AST. It is a
     * {@link JoinGroupNode} which is annotated to indicate the negation
     * semantics.
     * <p>
     * Note: The grammar file treats OPTIONAL somewhat differently than MINUS
     * due to the scope of the filters. For OPTIONAL, the child group can see
     * the variable bindings in the parent group. That is not true for MINUS.
     * Therefore the code in this method only sets the MINUS attribute on the
     * group and does not need to create a new group.
     */
    @Override
    public Object visit(final ASTMinusGraphPattern node, Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);

        // visit the children.
        super.visit(node, null);

//        final JoinGroupNode joinGroup = new JoinGroupNode();
//
//        joinGroup.setMinus(true);
//        
//        @SuppressWarnings("rawtypes")
//        final GroupNodeBase group = graphPattern.buildGroup(joinGroup);
        
        /*
         * The child is already wrapped up as a JoinGroupNode and we need to set
         * the MINUS annotation on that child. This takes a lazy approach and,
         * rather than reaching into the graphPattern data structure, it builds
         * a group which will contain just the desired child join group and then
         * pulls it out.
         */

        final JoinGroupNode group = (JoinGroupNode) graphPattern.buildGroup(
                new JoinGroupNode()).get(0);

        // Annotate this group to give it negation semantics.
        group.setMinus(true);
        
        parentGP.add(group);

        graphPattern = parentGP;

        return null;
    }

    @Override
    final public Void visit(final ASTGraphGraphPattern node, Object data)
            throws VisitorException {
        
        final TermNode oldContext = graphPattern.getContext();
        final Scope oldScope = graphPattern.getStatementPatternScope();

        final TermNode newContext = (TermNode) node.jjtGetChild(0).jjtAccept(
                this, null);

        // @see https://jira.blazegraph.com/browse/BLZG-1176
        // moved to ASTDeferredIVResolution.fillInIV(AST2BOpContext, BOp)
//        if (!context.tripleStore.isQuads()) {
//           if (newContext!=null) {
//               throw new QuadsOperationInTriplesModeException(
//                   "Use of GRAPH construct in query body is not supported "
//                   + "in triples mode.");
//           }
//        }

        graphPattern.setContextVar(newContext);
        graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

        node.jjtGetChild(1).jjtAccept(this, null);

        graphPattern.setContextVar(oldContext);
        graphPattern.setStatementPatternScope(oldScope);

        return null;
    }

    /**
     * Given
     * 
     * <pre>
     * select ?s where { { ?s ?p1 ?o } UNION  { ?s ?p2 ?o } UNION  { ?s ?p3 ?o } }
     * </pre>
     * 
     * The parse tree looks like:
     * 
     * <pre>
     * QueryContainer
     *  SelectQuery
     *   Select
     *    ProjectionElem
     *     Var (s)
     *   WhereClause
     *    GraphPatternGroup
     *     UnionGraphPattern
     *      GraphPatternGroup
     *       BasicGraphPattern
     *        TriplesSameSubjectPath
     *         Var (s)
     *         PropertyListPath
     *          Var (p1)
     *          ObjectList
     *           Var (o)
     *      UnionGraphPattern
     *       GraphPatternGroup
     *        BasicGraphPattern
     *         TriplesSameSubjectPath
     *          Var (s)
     *          PropertyListPath
     *           Var (p2)
     *           ObjectList
     *            Var (o)
     *       GraphPatternGroup
     *        BasicGraphPattern
     *         TriplesSameSubjectPath
     *          Var (s)
     *          PropertyListPath
     *           Var (p3)
     *           ObjectList
     *            Var (o)
     * </pre>
     * 
     * That is, UNION in a binary operator in the parse tree. If the right hand
     * argument is also a UNION, then you are looking at a sequence of UNIONs at
     * the same level. Such sequences should be turned into a single
     * {@link UnionNode} in the bigdata AST.
     * 
     * In contrast, this is a UNION of two groups with another UNION embedded in
     * the 2nd group.
     * 
     * <pre>
     * select ?s where { { ?s ?p1 ?o } UNION  { { ?s ?p2 ?o } UNION  { ?s ?p3 ?o } }  }
     * </pre>
     * 
     * The 2nd child of the first union is a GraphPatternGroup, not a
     * UnionGraphPattern.
     * 
     * <pre>
     * QueryContainer
     *  SelectQuery
     *   Select
     *    ProjectionElem
     *     Var (s)
     *   WhereClause
     *    GraphPatternGroup
     *     UnionGraphPattern
     *      GraphPatternGroup
     *       BasicGraphPattern
     *        TriplesSameSubjectPath
     *         Var (s)
     *         PropertyListPath
     *          Var (p1)
     *          ObjectList
     *           Var (o)
     *      GraphPatternGroup
     *       UnionGraphPattern
     *        GraphPatternGroup
     *         BasicGraphPattern
     *          TriplesSameSubjectPath
     *           Var (s)
     *           PropertyListPath
     *            Var (p2)
     *            ObjectList
     *             Var (o)
     *        GraphPatternGroup
     *         BasicGraphPattern
     *          TriplesSameSubjectPath
     *           Var (s)
     *           PropertyListPath
     *            Var (p3)
     *            ObjectList
     *             Var (o)
     * </pre>
     */
    @Override
    final public Void visit(final ASTUnionGraphPattern node, Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);

        // left arg is a some kind of group.
        node.jjtGetChild(0).jjtAccept(this, null);
        
        /*
         * The right arg is also a kind of group. If it is a ASTUnion, then this
         * is really a chain of UNIONs and they can all be flattened out.
         */
        node.jjtGetChild(1).jjtAccept(this, null);
        
        // Build a union of those patterns.
        final UnionNode union = graphPattern.buildGroup(new UnionNode());
        
        parentGP.add(union);
        
        graphPattern = parentGP;

        return null;
        
    }

    /**
	 * A BIND (or FILTER) can appear in an {@link ASTBasicGraphPattern}.
	 * 
	 * @return The {@link AssignmentNode} for the BIND -or- the
	 *         <code>null</code> if this is <code>BIND(tripleRef AS var)<code>.
	 */
    @Override
    final public Object visit(final ASTBind node, final Object data)
            throws VisitorException {

        if (node.jjtGetNumChildren() != 2)
            throw new AssertionError("Expecting two children, not "
                    + node.jjtGetNumChildren() + ", node=" + node.dump(">>>"));

		if (node.jjtGetChild(0) instanceof ASTTRefPattern) {

			/*
			 * Note: This case is handled by visit(ASTTRefPattern,Object) when
			 * we invoke jjtAccept() for the expression node. That method has
			 * already added the appropriate SP to the group. We do not want to
			 * do anything more here.
			 * 
			 * Note: The caller does not actually do anything with the return
			 * value from this method, so returning a [null] here is Ok.
			 * 
			 * @see <a
			 * href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
			 * Reification Done Right</a>
			 */
			
			node.jjtGetChild(0).jjtAccept(this, data);

			return null;
			
		}

		final Object expr = node.jjtGetChild(0).jjtAccept(this, data);

		final ValueExpressionNode ve = (ValueExpressionNode) expr;

        final Node aliasNode = node.jjtGetChild(1);

        final String alias = ((ASTVar) aliasNode).getName();

		final AssignmentNode bind = new AssignmentNode(new VarNode(alias), ve);

		graphPattern.add(bind);

		return bind;
        
    }
    
    /**
     * @return An object which encapsulates both the ordered set of variables
     *         for which bindings exist and the set of binding sets. The
     *         bindings are {@link BigdataValue}s. They must be translated into
     *         {@link IV} through a batch resolution process before they can be
     *         passed into the query engine.
     */
    @Override
    final public BindingsClause visit(final ASTInlineData node, Object data)
            throws VisitorException
    {

        // The ordered list of variable bindings.
        final List<ASTVar> varNodes = node.jjtGetChildren(ASTVar.class); 
        final int nvars = varNodes.size(); 
        final LinkedHashSet<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>(nvars);

        {
            for (ASTVar varNode : varNodes) {
                final VarNode var = (VarNode) varNode.jjtAccept(this, data);
                final IVariable<?> v = var.getValueExpression();
                if (!vars.add(v)) {
                    throw new VisitorException("duplicate variable in BINDINGS");
                }
            }
        }

        /*
         * Collect binding sets.
         */
        {

            final List<ASTBindingSet> bindingNodes = node.jjtGetChildren(ASTBindingSet.class);

            final List<IBindingSet> bindingSets = new LinkedList<IBindingSet>();

            final IVariable<?>[] declaredVars = vars
                    .toArray(new IVariable[nvars]);

            for (ASTBindingSet bindingNode : bindingNodes) {

                final IBindingSet bindingSet = (IBindingSet) bindingNode
                        .jjtAccept(this, declaredVars);

                bindingSets.add(bindingSet);

            }

            final BindingsClause bind = new BindingsClause(vars, bindingSets);

            graphPattern.add(bind);
            
            return bind;

        }
        
    }

    /**
     * A LET is just an alternative syntax for BIND
     * 
     * @return The {@link AssignmentNode}.
     */
    @Override
    final public AssignmentNode visit(final ASTLet node, final Object data)
            throws VisitorException {

        if (node.jjtGetNumChildren() != 2)
            throw new AssertionError("Expecting two children, not "
                    + node.jjtGetNumChildren() + ", node=" + node.dump(">>>"));

        final ValueExpressionNode ve = (ValueExpressionNode) node
                .jjtGetChild(1).jjtAccept(this, data);

        final Node aliasNode = node.jjtGetChild(0);

        final String alias = ((ASTVar) aliasNode).getName();

        final AssignmentNode bind = new AssignmentNode(new VarNode(alias), ve);

        graphPattern.add(bind);

        return bind;

    }
    
    /**
     * A FILTER. The filter is attached to the {@link #graphPattern}. However,
     * it is also returned from this method. The {@link ASTHavingClause} uses
     * the return value.
     * 
     * @return The constraint.
     */
    @Override
    final public ValueExpressionNode visit(final ASTConstraint node, final Object data)
            throws VisitorException {

        final ValueExpressionNode valueExpr = (ValueExpressionNode) node
                .jjtGetChild(0).jjtAccept(this, null);

        graphPattern.addConstraint(valueExpr);

        return valueExpr;

    }

    /**
     * INCLUDE for a named subquery result set.
     */
    @Override
    final public Void visit(final ASTNamedSubqueryInclude node, final Object data)
            throws VisitorException {

        final NamedSubqueryInclude includeNode = new NamedSubqueryInclude(
                node.getName());

//        final int nargs = node.jjtGetNumChildren();
//
//        if (nargs > 0 || node.isQueryHint()) {
//
//            /*
//             * Query hint for the join variables. This query hint may be used if
//             * static analysis of the context in which the INCLUDE appears fails
//             * to predict the correct join variables. (In the worst case it will
//             * fail to identify ANY join variables, which will cause the join to
//             * consider N x M solutions, rather than selectively probing a hash
//             * index to find just those solutions which could match.)
//             * 
//             * Note: We should recognize the syntax () as explicitly requesting
//             * a join without join variables.  This would have to be done as an
//             * action in [sparql.jjt].
//             */
//            
//            final VarNode[] joinvars = new VarNode[nargs];
//
//            for (int i = 0; i < nargs; i++) {
//
//                final Node argNode = node.jjtGetChild(i);
//
//                joinvars[i] = (VarNode) argNode.jjtAccept(this, null);
//
//            }
//
//            includeNode.setJoinVars(joinvars);
//
//        }

        graphPattern.add(includeNode);

        return null;
        
    }

//    public Object visit(ASTServiceGraphPattern node, Object data)
//        throws VisitorException
//    {
//        GraphPattern parentGP = graphPattern;
//
//        ValueExpr serviceRef = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
//
//        graphPattern = new GraphPattern(parentGP);
//        node.jjtGetChild(1).jjtAccept(this, null);
//        TupleExpr serviceExpr = graphPattern.buildTupleExpr();
//        
//        if (serviceExpr instanceof SingletonSet)
//            return null;    // do not add an empty service block
//        
//        String serviceExpressionString = node.getPatternString();
//
//        parentGP.addRequiredTE(new Service(valueExpr2Var(serviceRef), serviceExpr, serviceExpressionString,
//                node.getPrefixDeclarations(), node.getBaseURI(), node.isSilent()));
//
//        graphPattern = parentGP;
//
//        return null;
//    }

    /**
     * SPARQL 1.1 SERVICE.
     * <p> 
     * Note: The prefix declarations are attached to the
     * {@link ASTServiceGraphPattern} by the {@link PrefixDeclProcessor}.
     * <p>
     * TODO Do we need to pass through the baseURI? Can this be used to
     * abbreviate serialized IRIs? (I would think that we would use the prefix
     * declarations for that.)
     */
    @Override
    final public Void visit(final ASTServiceGraphPattern node, final Object data)
            throws VisitorException {

        // left arg is the service reference (a value expression).
        final TermNode serviceRef = (TermNode) node.jjtGetChild(0).jjtAccept(
                this, null);

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);

        // right arg is a some kind of group.
        node.jjtGetChild(1).jjtAccept(this, null);

        // Extract the service's join group.
//        final JoinGroupNode graphNode = graphPattern.buildGroup(new JoinGroupNode());
        final JoinGroupNode graphNode = graphPattern.getSingletonGroup();

        graphPattern = parentGP;

        final ServiceNode serviceNode = new ServiceNode(serviceRef, graphNode);

        /*
         * Note: This "image" of the original group graph pattern is what gets
         * sent to a remote SPARQL end point when we evaluate the SERVICE node.
         * Because the original "image" of the graph pattern is being used, we
         * also need to have the prefix declarations so we can generate a valid
         * SPARQL request.
         */
        serviceNode.setExprImage(node.getPatternString());
        
        final Map<String, String> prefixDecls = node.getPrefixDeclarations();
        
        if (prefixDecls != null && !prefixDecls.isEmpty()) {

            /*
             * Set the prefix declarations which go with that expression image.
             */

            serviceNode.setPrefixDecls(prefixDecls);
            
        }

        if (node.isSilent()) {
            
            serviceNode.setSilent(true);
            
        }
        
        graphPattern.add(serviceNode);
        
        return (Void) null;

    }

}
