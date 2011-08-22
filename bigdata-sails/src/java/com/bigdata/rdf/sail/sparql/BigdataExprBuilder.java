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

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTOffset;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SliceNode;
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

    /*---------*
     * Methods *
     *---------*/

//    @Override
//    public TupleExpr visit(ASTQueryContainer node, Object data)
//        throws VisitorException
//    {
//        // Skip the prolog, any information it contains should already have been
//        // processed
//        return (TupleExpr)node.getQuery().jjtAccept(this, null);
//    }

    /**
     * <pre>
     * ASTQueryContainer QueryContainer():
     * {}
     * {
     *     Prolog() Query() <EOF>
     *     { return jjtThis; }
     * }
     * </pre>
     */
    @Override
    public QueryRoot visit(final ASTQueryContainer node, final Object data)
        throws VisitorException
    {

        if (log.isInfoEnabled())
            log.info("\n" + node.dump(">"));

        /**
         * Skip the prolog, any information it contains should already have been
         * processed.
         * 
         * Note: BaseDecl and PrefixDeclList are also on the ASTQueryContainer
         * node, but they are being ignored here.
         * 
         * Note: ASTQuery is an abstract type. The concrete classes are:
         * ASTAskQuery, ASTConstructQuery, ASTDescribeQuery, and ASTSelectQuery.
         * 
         * <pre>
         * void Query() #void :
         * {}
         * {
         *     SelectQuery()
         * |   ConstructQuery()
         * |   DescribeQuery()
         * |   AskQuery()
         * }
         * </pre>
         * 
         * Note: The code delegates to ASTQuery.jjtAccept(this,...). However,
         * that pattern winds up reentering into this class on the visit()
         * method for the appropriate concrete subtype of ASTQuery. This is a
         * bit circular. We could have invoked visit(astNode,null) as easily.
         */

        final ASTQuery astQuery = node.getQuery();

        final QueryRoot queryRoot = new QueryRoot();

        /*
         * Handle different core query types: {ASTSelectQuery, ASTAskQuery,
         * ASTConstructQuery, ASTDescribeQuery}.
         * 
         * TODO Refactor into visit(astQuery,null) with implementation in each
         * visit(ASTSelectQuery), visit(ASTAskQuery), etc. (How do we model ASK,
         * CONSTRUCT, DESCRIBE?)
         */
        {
            if (astQuery instanceof ASTSelectQuery) {
                final ASTSelectQuery selectQuery = (ASTSelectQuery) astQuery;
                /*
                 * PROJECTION.
                 */
                final ASTSelect select = selectQuery.getSelect();
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

            } else {
                /*
                 * FIXME ASTAskQuery
                 * 
                 * FIXME ASTDescribeQuery
                 * 
                 * FIXME ASTConstructQuery
                 */
                throw new UnsupportedOperationException("ASTQuery=" + astQuery);
            }
        }

        /*
         * Handle the "WHERE" clause.
         * 
         * TODO This delegates the translation to a helper visitor, but
         * delegation may be a problem when we recurse through SubSelect.
         */
        {

            final ASTWhereClause whereClause = astQuery.getWhereClause();

            final ASTGraphPatternGroup graphPatternGroup = whereClause
                    .getGraphPatternGroup();

            queryRoot.setWhereClause((IGroupNode) graphPatternGroup.jjtAccept(
                    groupGraphPatternBuilder, null/* data */));

        }

        // FIXME GROUP BY / HAVING
        
//        // FIXME ORDER BY
//        ASTOrderClause orderNode = node.getOrderClause();
//        if (orderNode != null) {
//            List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
//            tupleExpr = new Order(tupleExpr, orderElemements);
//        }

        // Handle SLICE
        {
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
        
        return queryRoot;
        
//        return (IQueryNode) aChild.jjtAccept(this, null);
        
    }

//    /*
//     * FIXME All the aggregate and order by logic needs to be redone.
//     */
//    @Override
//    public TupleExpr visit(ASTSelectQuery node, Object data)
//        throws VisitorException
//    {
//        GraphPattern parentGP = graphPattern;
//
//        // Start with building the graph pattern
//        graphPattern = new GraphPattern();
//        node.getWhereClause().jjtAccept(this, null);
//        TupleExpr tupleExpr = graphPattern.buildTupleExpr();
//
//        // Apply grouping
//        ASTGroupClause groupNode = node.getGroupClause();
//        if (groupNode != null) {
//
//            tupleExpr = (TupleExpr)groupNode.jjtAccept(this, tupleExpr);
//        }
//
//        // Apply HAVING group filter condition
//        ASTHavingClause havingNode = node.getHavingClause();
//        if (havingNode != null) {
//
//            // add implicit group if necessary
//            if (!(tupleExpr instanceof Group)) {
//                tupleExpr = new Group(tupleExpr);
//            }
//
//            // FIXME is the child of a HAVING node always a compare?
//            Compare condition = (Compare)havingNode.jjtGetChild(0).jjtAccept(this, tupleExpr);
//
//            // retrieve any aggregate operators from the condition.
//            AggregateCollector collector = new AggregateCollector();
//            collector.meet(condition);
//
//            // replace operator occurrences with an anonymous var, and alias it
//            // to the group
//            Extension extension = new Extension();
//            for (AggregateOperator operator : collector.getOperators()) {
//                VarNode var = context.createAnonVar("-const-" + context.constantVarID++);
//
//                // replace occurrence of the operator in the filter condition
//                // with the variable.
//                AggregateOperatorReplacer replacer = new AggregateOperatorReplacer(operator, var);
//                replacer.meet(condition);
//
//                String alias = var.getName();
//
//                // create an extension linking the operator to the variable
//                // name.
//                ExtensionElem pe = new ExtensionElem(operator, alias);
//                extension.addElement(pe);
//
//                // add the aggregate operator to the group.
//                GroupElem ge = new GroupElem(alias, operator);
//
//                // FIXME quite often the aggregate in the HAVING clause will be
//                // a
//                // duplicate of an aggregate in the projection. We could perhaps
//                // optimize for that, to avoid
//                // having to evaluate twice.
//                ((Group)tupleExpr).addGroupElement(ge);
//            }
//
//            extension.setArg(tupleExpr);
//            tupleExpr = new Filter(extension, condition);
//        }
//
//        // Apply result ordering
//        ASTOrderClause orderNode = node.getOrderClause();
//        if (orderNode != null) {
//            List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
//            tupleExpr = new Order(tupleExpr, orderElemements);
//        }
//
//        // Apply projection
//        tupleExpr = (TupleExpr)node.getSelect().jjtAccept(this, tupleExpr);
//
//        // Process limit and offset clauses
//        ASTLimit limitNode = node.getLimit();
//        long limit = -1L;
//        if (limitNode != null) {
//            limit = (Long)limitNode.jjtAccept(this, null);
//        }
//
//        ASTOffset offsetNode = node.getOffset();
//        long offset = -1L;
//        if (offsetNode != null) {
//            offset = (Long)offsetNode.jjtAccept(this, null);
//        }
//
//        if (offset >= 1L || limit >= 0L) {
//            tupleExpr = new Slice(tupleExpr, offset, limit);
//        }
//
//        if (parentGP != null) {
//            parentGP.addRequiredTE(tupleExpr);
//            graphPattern = parentGP;
//        }
//
//        return tupleExpr;
//    }
//
//    @Override
//    public TupleExpr visit(ASTSelect node, Object data)
//        throws VisitorException
//    {
//        TupleExpr result = (TupleExpr)data;
//
//        Extension extension = new Extension();
//
//        ProjectionElemList projElemList = new ProjectionElemList();
//
//        for (ASTProjectionElem projElemNode : node.getProjectionElemList()) {
//
//            Node child = projElemNode.jjtGetChild(0);
//
//            String alias = projElemNode.getAlias();
//            if (alias != null) {
//                // aliased projection element
//                ValueExpr valueExpr = (ValueExpr)child.jjtAccept(this, null);
//
//                projElemList.addElement(new ProjectionElem(alias));
//
//                if (valueExpr instanceof AggregateOperator) {
//                    // Apply implicit grouping if necessary
//                    GroupFinder groupFinder = new GroupFinder();
//                    result.visit(groupFinder);
//                    Group group = groupFinder.getGroup();
//
//                    boolean existingGroup = true;
//                    if (group == null) {
//                        group = new Group(result);
//                        existingGroup = false;
//                    }
//
//                    group.addGroupElement(new GroupElem(alias, (AggregateOperator)valueExpr));
//
//                    extension.setArg(group);
//
//                    if (!existingGroup) {
//                        result = group;
//                    }
//                }
//                extension.addElement(new ExtensionElem(valueExpr, alias));
//            }
//            else if (child instanceof ASTVar) {
//                Var projVar = (Var)child.jjtAccept(this, null);
//                projElemList.addElement(new ProjectionElem(projVar.getName()));
//            }
//            else {
//                throw new IllegalStateException("required alias for non-Var projection elements not found");
//            }
//        }
//
//        if (!extension.getElements().isEmpty()) {
//            extension.setArg(result);
//            result = extension;
//        }
//
//        result = new Projection(result, projElemList);
//
//        if (node.isDistinct()) {
//            result = new Distinct(result);
//        }
//        else if (node.isReduced()) {
//            result = new Reduced(result);
//        }
//
//        return result;
//    }
//
//    private class GroupFinder extends QueryModelVisitorBase<VisitorException> {
//
//        private Group group;
//
//        @Override 
//        public void meet(Projection projection) {
//            // stop tree traversal on finding a projection: we do not wish to find the group in a sub-select.
//        }
//        
//        @Override
//        public void meet(Group group) {
//            this.group = group;
//        }
//
//        public Group getGroup() {
//            return group;
//        }
//    }
//
//    @Override
//    public TupleExpr visit(ASTConstructQuery node, Object data)
//        throws VisitorException
//    {
//        // Start with building the graph pattern
//        graphPattern = new GraphPattern();
//        node.getWhereClause().jjtAccept(this, null);
//        TupleExpr tupleExpr = graphPattern.buildTupleExpr();
//
//        // Apply result ordering
//        ASTOrderClause orderNode = node.getOrderClause();
//        if (orderNode != null) {
//            List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
//            tupleExpr = new Order(tupleExpr, orderElemements);
//        }
//
//        // Process construct clause
//        ASTConstruct constructNode = node.getConstruct();
//        if (!constructNode.isWildcard()) {
//            tupleExpr = (TupleExpr)constructNode.jjtAccept(this, tupleExpr);
//        }
//        else {
//            // create construct clause from graph pattern.
//            ConstructorBuilder cb = new ConstructorBuilder();
//
//            // SPARQL does not allow distinct or reduced right now. Leaving
//            // functionality in construct builder for
//            // possible future use.
//            tupleExpr = cb.buildConstructor(tupleExpr, false, false);
//        }
//
//        // process limit and offset clauses
//        ASTLimit limitNode = node.getLimit();
//        long limit = -1L;
//        if (limitNode != null) {
//            limit = (Long)limitNode.jjtAccept(this, null);
//        }
//
//        ASTOffset offsetNode = node.getOffset();
//        long offset = -1;
//        if (offsetNode != null) {
//            offset = (Long)offsetNode.jjtAccept(this, null);
//        }
//
//        if (offset >= 1 || limit >= 0) {
//            tupleExpr = new Slice(tupleExpr, offset, limit);
//        }
//
//        return tupleExpr;
//    }
//
//    @Override
//    public TupleExpr visit(ASTConstruct node, Object data)
//        throws VisitorException
//    {
//        TupleExpr result = (TupleExpr)data;
//
//        // Collect construct triples
//        graphPattern = new GraphPattern();
//        super.visit(node, null);
//        TupleExpr constructExpr = graphPattern.buildTupleExpr();
//
//        // Retrieve all StatementPattern's from the construct expression
//        List<StatementPattern> statementPatterns = StatementPatternCollector.process(constructExpr);
//
//        Set<Var> constructVars = getConstructVars(statementPatterns);
//
//        // Create BNodeGenerator's for all anonymous variables
//        Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//        for (Var var : constructVars) {
//            if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//                ValueExpr valueExpr;
//
//                if (var.hasValue()) {
//                    valueExpr = new ValueConstant(var.getValue());
//                }
//                else {
//                    valueExpr = new BNodeGenerator();
//                }
//
//                extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//            }
//        }
//
//        if (!extElemMap.isEmpty()) {
//            result = new Extension(result, extElemMap.values());
//        }
//
//        // Create a Projection for each StatementPattern in the constructor
//        List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//        for (StatementPattern sp : statementPatterns) {
//            ProjectionElemList projElemList = new ProjectionElemList();
//
//            projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//            projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//            projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//            projList.add(projElemList);
//        }
//
//        if (projList.size() == 1) {
//            result = new Projection(result, projList.get(0));
//        }
//        else if (projList.size() > 1) {
//            result = new MultiProjection(result, projList);
//        }
//        else {
//            // Empty constructor
//            result = new EmptySet();
//        }
//
//        return new Reduced(result);
//    }
//
//    /**
//     * Gets the set of variables that are relevant for the constructor. This
//     * method accumulates all subject, predicate and object variables from the
//     * supplied statement patterns, but ignores any context variables.
//     */
//    private Set<Var> getConstructVars(Collection<StatementPattern> statementPatterns) {
//        Set<Var> vars = new LinkedHashSet<Var>(statementPatterns.size() * 2);
//
//        for (StatementPattern sp : statementPatterns) {
//            vars.add(sp.getSubjectVar());
//            vars.add(sp.getPredicateVar());
//            vars.add(sp.getObjectVar());
//        }
//
//        return vars;
//    }
//
//    @Override
//    public TupleExpr visit(ASTDescribeQuery node, Object data)
//        throws VisitorException
//    {
//        TupleExpr tupleExpr = null;
//
//        if (node.getWhereClause() != null) {
//            // Start with building the graph pattern
//            graphPattern = new GraphPattern();
//            node.getWhereClause().jjtAccept(this, null);
//            tupleExpr = graphPattern.buildTupleExpr();
//
//            // Apply result ordering
//            ASTOrderClause orderNode = node.getOrderClause();
//            if (orderNode != null) {
//                List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
//                tupleExpr = new Order(tupleExpr, orderElemements);
//            }
//
//            // Process limit and offset clauses
//            ASTLimit limitNode = node.getLimit();
//            long limit = -1;
//            if (limitNode != null) {
//                limit = (Long)limitNode.jjtAccept(this, null);
//            }
//
//            ASTOffset offsetNode = node.getOffset();
//            long offset = -1;
//            if (offsetNode != null) {
//                offset = (Long)offsetNode.jjtAccept(this, null);
//            }
//
//            if (offset >= 1 || limit >= 0) {
//                tupleExpr = new Slice(tupleExpr, offset, limit);
//            }
//        }
//
//        // Process describe clause last
//        return (TupleExpr)node.getDescribe().jjtAccept(this, tupleExpr);
//    }
//
//    @Override
//    public TupleExpr visit(ASTDescribe node, Object data)
//        throws VisitorException
//    {
//        TupleExpr result = (TupleExpr)data;
//
//        // Create a graph query that produces the statements that have the
//        // requests resources as subject or object
//        Var subjVar = createAnonVar("-descr-subj");
//        Var predVar = createAnonVar("-descr-pred");
//        Var objVar = createAnonVar("-descr-obj");
//        StatementPattern sp = new StatementPattern(subjVar, predVar, objVar);
//
//        if (result == null) {
//            result = sp;
//        }
//        else {
//            result = new Join(result, sp);
//        }
//
//        List<SameTerm> sameTerms = new ArrayList<SameTerm>(2 * node.jjtGetNumChildren());
//
//        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//            ValueExpr resource = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, null);
//
//            sameTerms.add(new SameTerm(subjVar.clone(), resource));
//            sameTerms.add(new SameTerm(objVar.clone(), resource));
//        }
//
//        ValueExpr constraint = sameTerms.get(0);
//        for (int i = 1; i < sameTerms.size(); i++) {
//            constraint = new Or(constraint, sameTerms.get(i));
//        }
//
//        result = new Filter(result, constraint);
//
//        ProjectionElemList projElemList = new ProjectionElemList();
//        projElemList.addElement(new ProjectionElem(subjVar.getName(), "subject"));
//        projElemList.addElement(new ProjectionElem(predVar.getName(), "predicate"));
//        projElemList.addElement(new ProjectionElem(objVar.getName(), "object"));
//        result = new Projection(result, projElemList);
//
//        return new Reduced(result);
//    }
//
//    @Override
//    public TupleExpr visit(ASTAskQuery node, Object data)
//        throws VisitorException
//    {
//        graphPattern = new GraphPattern();
//
//        super.visit(node, null);
//
//        TupleExpr tupleExpr = graphPattern.buildTupleExpr();
//        tupleExpr = new Slice(tupleExpr, 0, 1);
//
//        return tupleExpr;
//    }
//
//    @Override
//    public Group visit(ASTGroupClause node, Object data)
//        throws VisitorException
//    {
//        TupleExpr tupleExpr = (TupleExpr)data;
//        Group g = new Group(tupleExpr);
//        int childCount = node.jjtGetNumChildren();
//
//        List<String> groupBindingNames = new ArrayList<String>();
//        for (int i = 0; i < childCount; i++) {
//            String name = (String)node.jjtGetChild(i).jjtAccept(this, g);
//            groupBindingNames.add(name);
//        }
//
//        g.setGroupBindingNames(groupBindingNames);
//
//        return g;
//    }
//
//    @Override
//    public String visit(ASTGroupCondition node, Object data)
//        throws VisitorException
//    {
//        Group group = (Group)data;
//        TupleExpr arg = group.getArg();
//
//        Extension extension = null;
//        if (arg instanceof Extension) {
//            extension = (Extension)arg;
//        }
//        else {
//            extension = new Extension();
//        }
//
//        String name = null;
//        ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);
//        if (ve instanceof Var) {
//            name = ((Var)ve).getName();
//        }
//        else {
//            if (node.jjtGetNumChildren() > 1) {
//                Var v = (Var)node.jjtGetChild(1).jjtAccept(this, data);
//                name = v.getName();
//            }
//            else {
//                // create an alias on the spot
//                name = createConstVar(null).getName();
//            }
//
//            ExtensionElem elem = new ExtensionElem(ve, name);
//            extension.addElement(elem);
//        }
//
//        if (extension.getElements().size() > 0 && !(arg instanceof Extension)) {
//            extension.setArg(arg);
//            group.setArg(extension);
//        }
//
//        return name;
//    }
//
//    @Override
//    public List<OrderElem> visit(ASTOrderClause node, Object data)
//        throws VisitorException
//    {
//        int childCount = node.jjtGetNumChildren();
//        List<OrderElem> elements = new ArrayList<OrderElem>(childCount);
//
//        for (int i = 0; i < childCount; i++) {
//            elements.add((OrderElem)node.jjtGetChild(i).jjtAccept(this, null));
//        }
//
//        return elements;
//    }
//
//    @Override
//    public OrderElem visit(ASTOrderCondition node, Object data)
//        throws VisitorException
//    {
//        ValueExpr valueExpr = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
//        return new OrderElem(valueExpr, node.isAscending());
//    }

    @Override
    public Long visit(ASTLimit node, Object data)
        throws VisitorException
    {
        return node.getValue();
    }

    @Override
    public Long visit(ASTOffset node, Object data)
        throws VisitorException
    {
        return node.getValue();
    }

}
