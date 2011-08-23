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
import org.openrdf.query.parser.serql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTBaseDecl;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTGroupClause;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
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
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
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
public class BigdataExprBuilder extends BigdataASTVisitorBase {

    private static final Logger log = Logger.getLogger(BigdataExprBuilder.class);
    
    /**
     * Used to build up {@link IValueExpressionNode}s.
     */
    private final ValueExprBuilder valueExprBuilder;
    
    /**
     * Used to build up {@link IGroupNode}s.
     */
    private final GroupGraphPatternBuilder groupGraphPatternBuilder;

    public BigdataExprBuilder(final BigdataASTContext context) {

        super(context);

        this.groupGraphPatternBuilder = new GroupGraphPatternBuilder(
                context);

        this.valueExprBuilder = new ValueExprBuilder(context);

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
     * 
     * <pre>
     * ASTQueryContainer QueryContainer():
     * {}
     * {
     *     Prolog() Query() <EOF>
     *     { return jjtThis; }
     * }
     * 
     * void Prolog() #void :
     * {}
     * {
     *     [ BaseDecl() ] ( PrefixDecl() )*
     * }
     * 
     * void BaseDecl() :
     * { Token t; }
     * {
     *     <BASE> t = <Q_IRI_REF> {jjtThis.setIRI(_trimString(t.image, 1));}
     * }
     * 
     * void PrefixDecl() :
     * { Token prefix; }
     * {
     *     <PREFIX> prefix = <PNAME_NS> IRI()
     * }
     * void Query() #void :
     * {}
     * {
     *     SelectQuery()
     * |   ConstructQuery()
     * |   DescribeQuery()
     * |   AskQuery()
     * }
     * </pre>
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
     * This is the entry point for both a top-level SELECT and a SubSelect. The
     * two contexts are differentiated based on the <i>data</i>, which is non-
     * <code>null</code> if this is a SubSelect. The method returns either a
     * {@link QueryRoot} or a {@link SubqueryRoot} depending on whether or not
     * the {@link ASTSelectQuery} appears as a top-level query or a subquery.
     */
    @Override
    public QueryBase visit(final ASTSelectQuery astQuery, Object data)
            throws VisitorException {

        final QueryBase queryRoot = data == null ? new QueryRoot(
                QueryType.SELECT) : new SubqueryRoot();

        /*
         * Handle SELECT expressions.
         */
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
                 * (Var | Expression AS Var)
                 */
                final ASTProjectionElem e = itr.next();
                if (!e.hasAlias()) {
                    // Var
                    final ASTVar aVar = (ASTVar) e
                            .jjtGetChild(0/* index */);
                    projection.addProjectionVar(new VarNode(aVar
                            .getName()));
                } else {
                    // Expression AS Var
                    final SimpleNode expr = (SimpleNode) e.jjtGetChild(0);
                    final IValueExpressionNode ve = (IValueExpressionNode) expr
                            .jjtAccept(valueExprBuilder, null/* data */);
                    final ASTVar aVar = (ASTVar) e
                            .jjtGetChild(1/* index */);
                    projection
                            .addProjectionExpression(new AssignmentNode(
                                    new VarNode(aVar.getName()), ve));
                }
            }
        }

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

        final QueryBase queryRoot = data == null ? new QueryRoot(QueryType.ASK)
                : new SubqueryRoot();

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

        final QueryBase queryRoot = data == null ? new QueryRoot(
                QueryType.DESCRIBE) : new SubqueryRoot();

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
                        .jjtAccept(valueExprBuilder, null);

                if (resource instanceof VarNode) {

                    projection.addProjectionVar((VarNode) resource);

                } else {

                    final VarNode anonVar = context.createAnonVar("-iri-"
                            + context.constantVarID);

                    projection.addExpr(new AssignmentNode(anonVar, resource));

                }

            }

        }
        
//        /*
//         * Create a graph query that produces the statements that have the
//         * requests resources as subject or object.
//         */
//        
//        final VarNode subjVar = context.createAnonVar("-descr-subj");
//        final VarNode predVar = context.createAnonVar("-descr-pred");
//        final VarNode objVar = context.createAnonVar("-descr-obj");
//        final StatementPatternNode sp = new StatementPatternNode(subjVar,
//                predVar, objVar);
//
//        /*
//         * Add the statement pattern to the where clause so it is joined with
//         * whatever was already specified in the where clause. If there is no
//         * where clause, then we will create an empty join group and add the
//         * statement pattern to that.
//         */
//        IGroupNode whereClause = queryRoot.getWhereClause();
//        if (whereClause == null) {
//            whereClause = new JoinGroupNode();
//            queryRoot.setWhereClause(whereClause);
//        }
//        whereClause.addChild(sp);
//
//        /*
//         * Setup SameTerm constraints for each variable or IRI in the "DESCRIBE"
//         * clause.
//         */
//        {
//
//            final int nchildren = node.jjtGetNumChildren();
//
//            final FunctionNode sameTerms[] = new FunctionNode[2 * nchildren];
//
//            for (int i = 0, j = 0; i < nchildren; i++) {
//
//                /*
//                 * Note: Delegates to the ValueExprBuilder. Can visit VarNode or
//                 * ConstantNode(IV<URI,_>).
//                 */
//
//                final ValueExpressionNode resource = (ValueExpressionNode) node
//                        .jjtGetChild(i).jjtAccept(valueExprBuilder, null);
//
//                sameTerms[j++] = new FunctionNode(context.lex,
//                        FunctionRegistry.SAME_TERM, null/* scalarValues */,
//                        new ValueExpressionNode[] { subjVar, resource });
//
//                sameTerms[j++] = new FunctionNode(context.lex,
//                        FunctionRegistry.SAME_TERM, null/* scalarValues */,
//                        new ValueExpressionNode[] { objVar, resource });
//
//            }
//
//            final ValueExpressionNode constraint = new FunctionNode(
//                    context.lex, FunctionRegistry.IN, null/* scalarValues */,
//                    sameTerms);
//
//            whereClause.addChild(new FilterNode(constraint));
//
//        }
//
//        final ProjectionNode projection = new ProjectionNode();
//        projection.setReduced(true);
//        projection.addProjectionVar(subjVar);
//        projection.addProjectionVar(predVar);
//        projection.addProjectionVar(objVar);
//        queryRoot.setProjection(projection);

        return null;
        
    }

        
//  @Override
//  public TupleExpr visit(ASTConstructQuery node, Object data)
//      throws VisitorException
//  {
//      // Start with building the graph pattern
//      graphPattern = new GraphPattern();
//      node.getWhereClause().jjtAccept(this, null);
//      TupleExpr tupleExpr = graphPattern.buildTupleExpr();
//
//      // Apply result ordering
//      ASTOrderClause orderNode = node.getOrderClause();
//      if (orderNode != null) {
//          List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
//          tupleExpr = new Order(tupleExpr, orderElemements);
//      }
//
//      // Process construct clause
//      ASTConstruct constructNode = node.getConstruct();
//      if (!constructNode.isWildcard()) {
//          tupleExpr = (TupleExpr)constructNode.jjtAccept(this, tupleExpr);
//      }
//      else {
//          // create construct clause from graph pattern.
//          ConstructorBuilder cb = new ConstructorBuilder();
//
//          // SPARQL does not allow distinct or reduced right now. Leaving
//          // functionality in construct builder for
//          // possible future use.
//          tupleExpr = cb.buildConstructor(tupleExpr, false, false);
//      }
//
//      // process limit and offset clauses
//      ASTLimit limitNode = node.getLimit();
//      long limit = -1L;
//      if (limitNode != null) {
//          limit = (Long)limitNode.jjtAccept(this, null);
//      }
//
//      ASTOffset offsetNode = node.getOffset();
//      long offset = -1;
//      if (offsetNode != null) {
//          offset = (Long)offsetNode.jjtAccept(this, null);
//      }
//
//      if (offset >= 1 || limit >= 0) {
//          tupleExpr = new Slice(tupleExpr, offset, limit);
//      }
//
//      return tupleExpr;
//  }
//
//  @Override
//  public TupleExpr visit(ASTConstruct node, Object data)
//      throws VisitorException
//  {
//      TupleExpr result = (TupleExpr)data;
//
//      // Collect construct triples
//      graphPattern = new GraphPattern();
//      super.visit(node, null);
//      TupleExpr constructExpr = graphPattern.buildTupleExpr();
//
//      // Retrieve all StatementPattern's from the construct expression
//      List<StatementPattern> statementPatterns = StatementPatternCollector.process(constructExpr);
//
//      Set<Var> constructVars = getConstructVars(statementPatterns);
//
//      // Create BNodeGenerator's for all anonymous variables
//      Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//      for (Var var : constructVars) {
//          if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//              ValueExpr valueExpr;
//
//              if (var.hasValue()) {
//                  valueExpr = new ValueConstant(var.getValue());
//              }
//              else {
//                  valueExpr = new BNodeGenerator();
//              }
//
//              extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//          }
//      }
//
//      if (!extElemMap.isEmpty()) {
//          result = new Extension(result, extElemMap.values());
//      }
//
//      // Create a Projection for each StatementPattern in the constructor
//      List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//      for (StatementPattern sp : statementPatterns) {
//          ProjectionElemList projElemList = new ProjectionElemList();
//
//          projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//          projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//          projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//          projList.add(projElemList);
//      }
//
//      if (projList.size() == 1) {
//          result = new Projection(result, projList.get(0));
//      }
//      else if (projList.size() > 1) {
//          result = new MultiProjection(result, projList);
//      }
//      else {
//          // Empty constructor
//          result = new EmptySet();
//      }
//
//      return new Reduced(result);
//  }
//
//  /**
//   * Gets the set of variables that are relevant for the constructor. This
//   * method accumulates all subject, predicate and object variables from the
//   * supplied statement patterns, but ignores any context variables.
//   */
//  private Set<Var> getConstructVars(Collection<StatementPattern> statementPatterns) {
//      Set<Var> vars = new LinkedHashSet<Var>(statementPatterns.size() * 2);
//
//      for (StatementPattern sp : statementPatterns) {
//          vars.add(sp.getSubjectVar());
//          vars.add(sp.getPredicateVar());
//          vars.add(sp.getObjectVar());
//      }
//
//      return vars;
//  }

    //
    // Grammar constructions below the ASTQuery node.
    //
    
    @Override
    public GroupByNode visit(final ASTGroupClause node, Object data)
            throws VisitorException {
        
        final GroupByNode groupBy = new GroupByNode();
        
        final int childCount = node.jjtGetNumChildren();

        for (int i = 0; i < childCount; i++) {

            /*
             * Delegate to the value expression builder. 
             */

            final AssignmentNode expr = (AssignmentNode) node.jjtGetChild(i)
                    .jjtAccept(valueExprBuilder, null/* data */);

            groupBy.addExpr((AssignmentNode) expr);
            
        }

        return groupBy;
        
    }

    @Override
    public List<OrderByExpr> visit(final ASTOrderClause node, Object data)
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
    public OrderByExpr visit(final ASTOrderCondition node, Object data)
        throws VisitorException
    {

        final ValueExpressionNode valueExpr = (ValueExpressionNode) node
                .jjtGetChild(0).jjtAccept(valueExprBuilder, null);

        return new OrderByExpr(valueExpr, node.isAscending());
        
    }

    @Override
    public Long visit(ASTLimit node, Object data) throws VisitorException {
        return node.getValue();
    }

    @Override
    public Long visit(ASTOffset node, Object data) throws VisitorException {
        return node.getValue();
    }

    //
    // private helper methods.
    //
    
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
    private void handleWhereClause(final ASTQuery astQuery,
            final QueryBase queryRoot) throws VisitorException {

        final ASTWhereClause whereClause = astQuery.getWhereClause();

        if (whereClause != null) {

            final ASTGraphPatternGroup graphPatternGroup = whereClause
                    .getGraphPatternGroup();

            queryRoot.setWhereClause((IGroupNode) graphPatternGroup.jjtAccept(
                    groupGraphPatternBuilder, null/* data */));

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

                /*
                 * Delegate children to the GroupGraphPatternBuilder (it handles
                 * ASTConstraint, which is also used in FILTER). It will
                 * delegate to the ValueExprBuilder to handle the inner value
                 * expression.
                 */
                final IValueExpressionNode ve = (IValueExpressionNode) havingClause
                        .jjtGetChild(i).jjtAccept(groupGraphPatternBuilder,
                                null/* data */);

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
