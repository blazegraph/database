/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 20, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTBaseDecl;
import org.openrdf.query.parser.sparql.ast.ASTConstruct;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTGroupClause;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTNamedSubquery;
import org.openrdf.query.parser.sparql.ast.ASTOffset;
import org.openrdf.query.parser.sparql.ast.ASTOrderClause;
import org.openrdf.query.parser.sparql.ast.ASTOrderCondition;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Top-level expression builder for SPARQL.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataExprBuilder extends GroupGraphPatternBuilder {

    private static final Logger log = Logger.getLogger(BigdataExprBuilder.class);

    public BigdataExprBuilder(final BigdataASTContext context) {

        super(context);

    }

    /**
     * This is the top-level entry point for a SPARQL query.
     * <p>
     * Note: {@link ASTBaseDecl} and {@link ASTPrefixDecl}s are available from
     * the {@link ASTQueryContainer} node. They are being ignored here as they
     * should have been processed before running the {@link BigdataExprBuilder}.
     * <p>
     * Note: {@link ASTQuery} is an abstract type. The concrete classes are:
     * {@link ASTAskQuery}, {@link ASTConstructQuery}, {@link ASTDescribeQuery},
     * and {@link ASTSelectQuery}. This method will wind up delegating to the
     * visitor method for the appropriate concrete {@link ASTQuery} instance.
     */
    @Override
    public QueryRoot visit(final ASTQueryContainer node, final Object data)
            throws VisitorException {

        if (log.isInfoEnabled())
            log.info("\n" + node.dump(">"));

        return (QueryRoot) node.getQuery().jjtAccept(this, null);

    }

    //
    // ASTQuery visitor methods for SELECT, ASK, DESCRIBE and CONSTRUCT.
    //
    
    /**
     * This is the entry point for both a top-level SELECT and a SubSelect.
     * 
     * @return The method returns either a {@link QueryRoot} or a
     *         {@link SubqueryRoot} depending on whether or not the
     *         {@link ASTSelectQuery} appears as a top-level query or a
     *         subquery.
     */
    @Override
    public QueryBase visit(final ASTSelectQuery astQuery, Object data)
            throws VisitorException {

        final QueryBase queryRoot = getQueryBase(astQuery, data,
                QueryType.SELECT);

        // Accept optional NamedSubquery(s).
        handleNamedSubqueryClause(astQuery, queryRoot);
                
        handleSelect(astQuery, queryRoot);

        handleWhereClause(astQuery, queryRoot);
        
        handleGroupBy(astQuery, queryRoot);

        handleHaving(astQuery, queryRoot);

        handleOrderBy(astQuery, queryRoot);
        
        handleSlice(astQuery, queryRoot);
        
        return queryRoot;

    }

    /**
     * ASK query.
     */
    @Override
    public QueryBase visit(final ASTAskQuery node, Object data)
            throws VisitorException {

        final QueryBase queryRoot = getQueryBase(node, data, QueryType.ASK);

        // Accept optional NamedSubquery(s).
        handleNamedSubqueryClause(node, queryRoot);

        /*
         * Note: Nothing is projected.
         */
        
        handleWhereClause(node, queryRoot);

        /*
         * Note: GROUP BY and HAVING are not currently permitted by the SPARQL
         * 1.1 grammar. This means that you can not place a constraint on
         * aggregates in an ASK query.
         */
        
//      handleGroupBy(node, queryRoot);
//
//      handleHaving(node, queryRoot);

        /*
         * Note: At most one solution.
         */
        
        final SliceNode slice = new SliceNode(0L/* offset */, 1L/* limit */);

        queryRoot.setSlice(slice);
        
        return queryRoot;
        
    }

    /**
     * DESCRIBE query.
     * <p>
     * Note: The openrdf parser has a "Describe" production which is not in the
     * SPARQL 1.1 grammar (it is an equivalent grammar in that it accepts the
     * same inputs). This means that the "projection" part of the DESCRIBE query
     * is visited on the {@link ASTDescribe} node.
     */
    @Override
    public QueryBase visit(final ASTDescribeQuery node, Object data)
            throws VisitorException {

        final QueryBase queryRoot = getQueryBase(node, data, QueryType.DESCRIBE);

        // Accept optional NamedSubquery(s).
        handleNamedSubqueryClause(node, queryRoot);
        
        // Process describe clause
        node.getDescribe().jjtAccept(this, queryRoot);

        handleWhereClause(node, queryRoot);
        
        handleGroupBy(node, queryRoot);

        handleHaving(node, queryRoot);

        handleOrderBy(node, queryRoot);
        
        handleSlice(node, queryRoot);

        return queryRoot;
        
    }

    /**
     * This is the "projection" part of the DESCRIBE query. This code marks the
     * query as a "DESCRIBE" and generates and attaches a {@link ProjectionNode}
     * to the {@link QueryBase}. The {@link ProjectionNode} uses an assignment
     * node to bind an anonymous variable for a IRI. Variables are projected
     * normally. <code>*</code> is projected as the variable named "*".
     * <p>
     * Note: This {@link ProjectionNode} models the "DESCRIBE" but does not
     * provide a query plan. The {@link QueryBase} MUST be further rewritten in
     * order to supply an appropriate query plan. This should be done using the
     * appropriate {@link IASTOptimizer}.
     * 
     * @param data
     *            The {@link QueryBase}.
     */
    @Override
    public Void visit(final ASTDescribe node, Object data)
            throws VisitorException {

        final QueryBase queryRoot = (QueryBase) data;

        final ProjectionNode projection = new ProjectionNode();
        queryRoot.setProjection(projection);

        if (node.isWildcard()) {

            projection.addProjectionVar(new VarNode("*"));
            
        } else {

            final int nchildren = node.jjtGetNumChildren();

            for (int i = 0; i < nchildren; i++) {

                /*
                 * Note: Delegates to the ValueExprBuilder. Can visit VarNode or
                 * ConstantNode(IV<URI,_>).
                 */

                final TermNode resource = (TermNode) node.jjtGetChild(i)
                        .jjtAccept(this, null);

                if (resource instanceof VarNode) {

                    projection.addProjectionVar((VarNode) resource);

                } else {

                    final VarNode anonVar = context.createAnonVar("-iri-");

                    projection.addExpr(new AssignmentNode(anonVar, resource));

                }

            }

        }
        
        return null;
        
    }

    /**
     * Handle a CONSTRUCT query.
     * <p>
     * This builds a {@link ConstructNode} which is a model of the statement
     * patterns in the construct template and attaches the where clause and
     * solution modifiers. However, this is NOT a complete description of the
     * semantics of the query. An {@link IASTOptimizer} will intercept the
     * CONSTRUCT query and rewrite it before it is executed.
     * 
     * FIXME There is an alternative form of a CONSTRUCT query which this is not
     * covering. (CONSTRUCT WHERE { TriplesTemplate? } SolutionModifier. openrdf
     * does not support this form yet.
     * 
     * FIXME Add support for quads (Anzo extension).
     */
    @Override
    public QueryBase visit(final ASTConstructQuery node, Object data)
            throws VisitorException {

        final QueryBase queryRoot = getQueryBase(node, data,
                QueryType.CONSTRUCT);

        // Accept optional NamedSubquery(s).
        handleNamedSubqueryClause(node, queryRoot);

        /*
         * Process construct clause.
         * 
         * Note: The children of the ASTConstruct is a TriplesBlock in
         * sparql.jjt. This delegates to the GroupGraphPatternBuilder, which
         * handles the TriplesBlock.
         */

        final ASTConstruct constructNode = node.getConstruct();

        final ConstructNode tmp = (ConstructNode) constructNode.jjtAccept(
                this, null/* data */);

        queryRoot.setConstruct(tmp);

        handleWhereClause(node, queryRoot);

        handleGroupBy(node, queryRoot);

        handleHaving(node, queryRoot);

        handleOrderBy(node, queryRoot);

        handleSlice(node, queryRoot);

        return queryRoot;

    }

    //
    // Grammar constructions below the ASTQuery node.
    //
    
    @Override
    final public GroupByNode visit(final ASTGroupClause node, Object data)
            throws VisitorException {
        
        final GroupByNode groupBy = new GroupByNode();
        
        final int childCount = node.jjtGetNumChildren();

        for (int i = 0; i < childCount; i++) {

            /*
             * Delegate to the value expression builder. 
             */

            final AssignmentNode expr = (AssignmentNode) node.jjtGetChild(i)
                    .jjtAccept(this, null/* data */);

            groupBy.addExpr((AssignmentNode) expr);
            
        }

        return groupBy;
        
    }

    @Override
    final public List<OrderByExpr> visit(final ASTOrderClause node, Object data)
            throws VisitorException {

        final int childCount = node.jjtGetNumChildren();

        final List<OrderByExpr> elements = new ArrayList<OrderByExpr>(
                childCount);

        for (int i = 0; i < childCount; i++) {

            /*
             * Note: OrderByExpr will delegate to the ValueExprBuilder.
             */

            elements.add((OrderByExpr) node.jjtGetChild(i)
                    .jjtAccept(this, null));

        }

        return elements;
    }

    /**
     * Note: Delegate to the {@link ValueExprBuilder}.
     */
    @Override
    final public OrderByExpr visit(final ASTOrderCondition node, Object data)
        throws VisitorException
    {

        final ValueExpressionNode valueExpr = (ValueExpressionNode) node
                .jjtGetChild(0).jjtAccept(this, null);

        return new OrderByExpr(valueExpr, node.isAscending());
        
    }

    @Override
    final public Long visit(ASTLimit node, Object data) throws VisitorException {
        return node.getValue();
    }

    @Override
    final public Long visit(ASTOffset node, Object data) throws VisitorException {
        return node.getValue();
    }

    //
    // private helper methods.
    //

    /**
     * Handle the optional WITH SelectQuery AS NAME clause(s).  These are a
     * SPARQL 1.1 extension for named temporary solution sets.
     * <P>
     * Note: This delegates the translation to a helper visitor. A SubSelect
     * will wind up delegated back to an instance of this visitor.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    private void handleNamedSubqueryClause(final ASTQuery astQuery,
            final QueryBase queryRoot) throws VisitorException {

        {

            // Check for any instances of this child.
            final ASTNamedSubquery aNamedSubquery = (ASTNamedSubquery) astQuery
                    .jjtGetChild(ASTNamedSubquery.class);

            if (aNamedSubquery == null) {

                // No instances.
                return;
                
            }
            
        }

        if (!(queryRoot instanceof QueryRoot))
            throw new VisitorException(
                    "WITH SubSelect AS %namedSet only allowed for top-level query.");

        /*
         * Setup the AST node for the named subqueries.
         */
        
        final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();

        ((QueryRoot) queryRoot).setNamedSubqueries(namedSubqueries);

        final int nchildren = astQuery.jjtGetNumChildren();

        for (int i = 0; i < nchildren; i++) {

            final Node aChild = astQuery.jjtGetChild(i);

            if (!(aChild instanceof ASTNamedSubquery))
                continue;

            // Found a named subquery.
            final ASTNamedSubquery aNamedSubquery = (ASTNamedSubquery) aChild;
            
            /*
             * Note: This visitor will wind up accepting and returning a
             * SubqueryRoot rather than a NamedSubqueryRoot, so we have to copy
             * the data into an appropriate object.
             */
            
            // Accept the Subquery
            final SubqueryRoot subquery = (SubqueryRoot) aNamedSubquery
                    .jjtAccept(this, queryRoot/* data */);

            // Create instance of the right class.
            final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(
                    subquery.getQueryType(), aNamedSubquery.getName());
            
            // Add to the container.
            namedSubqueries.add(namedSubquery);

            // Copy all args.
            for (BOp arg : subquery.args()) {
                namedSubquery.addArg(arg);
            }

            // Copy all annotations.
            namedSubquery.copyAll(subquery.annotations());

        }

    }
    
    /**
     * Return the appropriate {@link QueryBase} instance. If the parent of the
     * {@link ASTQuery} is an {@link ASTQueryContainer} then this will return
     * {@link QueryRoot}. Otherwise it will return {@link SubqueryRoot}.
     * 
     * @param node
     *            The {@link ASTQuery} node.
     * @param data
     *            The data. This can be bound to some things other than a
     *            {@link QueryBase}. Such bindings are ignored.
     * @param queryType
     *            The type of the {@link ASTQuery}.
     * 
     * @return Some kind of {@link QueryBase} object.
     */
    private QueryBase getQueryBase(final ASTQuery node, final Object data,
            final QueryType queryType) {

        final Node p = node.jjtGetParent();
        
        if (log.isInfoEnabled())
            log.info("parent=" + p + ", data="
                    + (data == null ? "null" : data.getClass().getSimpleName()));

        if (p instanceof ASTQueryContainer) {

            return new QueryRoot(queryType);

        }

        return new SubqueryRoot(queryType);

    }

    /**
     * Handle a SELECT clause. The {@link ProjectionNode} will be attached to
     * the {@link QueryBase}.
     * 
     * @param astQuery
     *            The {@link ASTSelectQuery} node.
     * @param queryRoot
     *            The {@link QueryBase}.
     * 
     * @throws VisitorException
     */
    private void handleSelect(final ASTSelectQuery astQuery,
            final QueryBase queryRoot) throws VisitorException {

        final ASTSelect select = astQuery.getSelect();

        final ProjectionNode projection = new ProjectionNode();
        
        queryRoot.setProjection(projection);
        
        if (select.isDistinct())
            projection.setDistinct(true);
        
        if (select.isReduced())
            projection.setReduced(true);
        
        if (select.isWildcard()) {
            projection.addProjectionVar(new VarNode("*"));
        
        } else {
        
            final Iterator<ASTProjectionElem> itr = select
                    .getProjectionElemList().iterator();
            
            while (itr.hasNext()) {
                
                /*
                 * The last argument of the children is the Var. Anything before
                 * that is an ArgList which must be interpreted in its own
                 * right.
                 * 
                 * (Var | ArgList Var)
                 */
                
                final ASTProjectionElem e = itr.next();
                
                if (!e.hasAlias()) {
                
                    // Var
                    final ASTVar aVar = (ASTVar) e.jjtGetChild(0/* index */);
                    
                    projection.addProjectionVar(new VarNode(aVar.getName()));
            
                } else {
                    
                    // Expression AS Var
                    final SimpleNode expr = (SimpleNode) e.jjtGetChild(0);
                    
                    final IValueExpressionNode ve = (IValueExpressionNode) expr
                            .jjtAccept(this, null/* data */);
                    
                    final String varname = e.getAlias();
                    
                    projection.addProjectionExpression(new AssignmentNode(
                            new VarNode(varname), ve));
                
                }
            
            }
            
        }

    }
    
    /**
     * Handle the optional WHERE clause. (For example, DESCRIBE may be used
     * without a WHERE clause.)
     * <P>
     * Note: This delegates the translation to a helper visitor. A SubSelect
     * will wind up delegated back to an instance of this visitor.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    @SuppressWarnings("unchecked")
    private void handleWhereClause(final ASTQuery astQuery,
            final QueryBase queryRoot) throws VisitorException {

        final ASTWhereClause whereClause = astQuery.getWhereClause();

        if (whereClause != null) {

            final ASTGraphPatternGroup graphPatternGroup = whereClause
                    .getGraphPatternGroup();

            graphPattern = new GroupGraphPattern();
            
            final IGroupNode<IGroupMemberNode> ret = (IGroupNode<IGroupMemberNode>) graphPatternGroup
                    .jjtAccept(this, null/* data */);

            queryRoot.setWhereClause(ret);

        }

    }

    /**
     * Handle an optional GROUP BY clause.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    private void handleGroupBy(final ASTQuery astQuery, final QueryBase queryRoot)
            throws VisitorException {
        
        final ASTGroupClause groupNode = astQuery.getGroupClause();
        
        if (groupNode != null) {

            final GroupByNode groupBy = (GroupByNode) groupNode.jjtAccept(
                    this, null);

            queryRoot.setGroupBy(groupBy);

        }

    }
    
    /**
     * Handle an optional HAVING clause.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    private void handleHaving(final ASTQuery astQuery, final QueryBase queryRoot)
            throws VisitorException {

        final ASTHavingClause havingClause = astQuery.getHavingClause();

        if (havingClause != null) {

            final HavingNode havingNode = new HavingNode();

            final int nchildren = havingClause.jjtGetNumChildren();

            for (int i = 0; i < nchildren; i++) {

                final IValueExpressionNode ve = (IValueExpressionNode) havingClause
                        .jjtGetChild(i).jjtAccept(this, null/* data */);

                havingNode.addExpr(ve);

            }

            queryRoot.setHaving(havingNode);

        }

    }
    
    /**
     * Handle an optional ORDER BY clause.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    private void handleOrderBy(final ASTQuery astQuery,
            final QueryBase queryRoot) throws VisitorException {

        final ASTOrderClause orderNode = astQuery.getOrderClause();

        if (orderNode != null) {

            final OrderByNode orderBy = new OrderByNode();

            @SuppressWarnings("unchecked")
            final List<OrderByExpr> orderElemements = (List<OrderByExpr>) orderNode
                    .jjtAccept(this, null);

            for (OrderByExpr orderByExpr : orderElemements)
                orderBy.addExpr(orderByExpr);

            queryRoot.setOrderBy(orderBy);

        }
    
    }
    
    /**
     * Handle an optional LIMIT/OFFSET.
     * 
     * @param astQuery
     *            The AST query node. This is an abstract base class. There are
     *            concrete instances for SELECT, ASK, DESCRIBE, and CONSTRUCT.
     * @param queryRoot
     *            The bigdata query root.
     */
    private void handleSlice(final ASTQuery astQuery,
            final QueryBase queryRoot) {

        final ASTLimit theLimit = astQuery.getLimit();

        final ASTOffset theOffset = astQuery.getOffset();

        if (theLimit != null || theOffset != null) {

            final SliceNode theSlice = new SliceNode();

            if (theLimit != null)
                theSlice.setLimit(theLimit.getValue());

            if (theOffset != null)
                theSlice.setOffset(theOffset.getValue());

            queryRoot.setSlice(theSlice);

        }

    }

}
