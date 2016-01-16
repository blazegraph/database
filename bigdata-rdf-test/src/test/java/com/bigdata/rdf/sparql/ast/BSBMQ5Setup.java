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
 * Created on Oct 21, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Problem setup for BSBM Q5.
 * <p>
 * <h2>Analysis of BSBM Q5</h2>
 * <p>
 * The following predicates all join on {@link #product}:
 * <ul>
 * <li>{@link BSBMQ5Setup#p0}</li>
 * <li>{@link BSBMQ5Setup#p2}</li>
 * <li>{@link BSBMQ5Setup#p4}</li>
 * <li>{@link BSBMQ5Setup#p5}</li>
 * </ul>
 * The predicates ({@link BSBMQ5Setup#p3} and {@link BSBMQ5Setup#p5}) do not
 * directly join with any of the other predicates (they do not directly
 * share any variables). In general, a join without shared variables means
 * the cross product of the sources will be materialized and such joins
 * should be run last.
 * <p>
 * However, in this case there are two SPARQL FILTERs (
 * {@link BSBMQ5Setup#c1} and {@link BSBMQ5Setup#c2}) which (a) use those
 * variables ({@link BSBMQ5Setup#origProperty1} and
 * {@link BSBMQ5Setup#origProperty2}); and (b) can constrain the query. This
 * means that running the predicates without shared variables and applying
 * the constraints before the tail of the plan can in fact lead to a more
 * efficient join path.
 */
public class BSBMQ5Setup {
    
    /**
     * Note: First id assigned is zero. This matches the naming of the
     * statement pattern nodes.
     */
    final AtomicInteger nextId = new AtomicInteger(-1); 

    public final BigdataURI rdfsLabel; 
    public final BigdataURI productFeature;
    public final BigdataURI productPropertyNumeric1;
    public final BigdataURI productPropertyNumeric2;
    public final BigdataURI product53999;
    public final BigdataLiteral _120;
    public final BigdataLiteral _170;

    public final QueryRoot queryRoot;
    public final ProjectionNode projection;
    
    /** ?product rdfs:label ?productLabel . */
    public final StatementPatternNode p0;
    /** productInstance bsbm:productFeature ?prodFeature . */
    public final StatementPatternNode p1;
    /** ?product bsbm:productFeature ?prodFeature . */
    public final StatementPatternNode p2;
    /** productInstance bsbm:productPropertyNumeric1 ?origProperty1 . */
    public final StatementPatternNode p3;
    /** ?product bsbm:productPropertyNumeric1 ?simProperty1 . */
    public final StatementPatternNode p4;
    /** productInstance bsbm:productPropertyNumeric2 ?origProperty2 . */
    public final StatementPatternNode p5;
    /** ?product bsbm:productPropertyNumeric2 ?simProperty2 . */
    public final StatementPatternNode p6;

    /**
     * FILTER (productInstance != ?product)
     */
    public final FilterNode c0;

    /**
     * FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 >
     * (?origProperty1 - 120))
     * <p>
     * Note: The AND in the compound filters is typically optimized out such
     * that each of these is represented as its own {@link FilterNode}, but
     * I have combined them for the purposes of these unit tests.
     */
    public final FilterNode c1;

    /**
     * FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 >
     * (?origProperty2 - 170))
     * <p>
     * Note: The AND in the compound filters is typically optimized out such
     * that each of these is represented as its own {@link FilterNode}, but
     * I have combined them for the purposes of these unit tests.
     */
    public final FilterNode c2;

    /** The constraints on the join graph. */
    public final FilterNode[] constraints;

    /** no constraints. */
    public final Set<FilterNode> NA = Collections.emptySet();

    /** {@link #c0} attaches when any of [p0,p2,p4,p6] runs for the 1st time. */
    public final Set<FilterNode> C0;

    /** {@link #c1} attaches when 2nd of (p3,p4) runs (in any order). */
    public final Set<FilterNode> C1;

    /** {@link #c2} attaches when 2nd of (p5,p6) runs (in any order). */
    public final Set<FilterNode> C2;

    public BSBMQ5Setup(final AbstractTripleStore store) {
        
        /*
         * Resolve terms against the lexicon.
         */
        final BigdataValueFactory valueFactory = store.getLexiconRelation()
                .getValueFactory();

        final String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
        // final String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        final String bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

        final String productInstance = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22";

        rdfsLabel = valueFactory.createURI(rdfs + "label");

        productFeature = valueFactory.createURI(bsbm + "productFeature");

        productPropertyNumeric1 = valueFactory.createURI(bsbm
                + "productPropertyNumeric1");

        productPropertyNumeric2 = valueFactory.createURI(bsbm
                + "productPropertyNumeric2");

        product53999 = valueFactory.createURI(productInstance);

        _120 = valueFactory.createLiteral("120", XSD.INTEGER);

        _170 = valueFactory.createLiteral("170", XSD.INTEGER);

        final BigdataValue[] terms = new BigdataValue[] { rdfsLabel,
                productFeature, productPropertyNumeric1,
                productPropertyNumeric2, product53999, _120, _170 };

        // resolve terms.
        store.getLexiconRelation()
                .addTerms(terms, terms.length, false/* readOnly */);

        for (BigdataValue bv : terms) {
            // Cache the Value on the IV.
            bv.getIV().setValue(bv);
        }

        // BSBM Q5
        queryRoot = new QueryRoot(QueryType.SELECT);

        projection = new ProjectionNode();
        queryRoot.setProjection(projection);
        projection.addProjectionVar(new VarNode("product"));
        projection.addProjectionVar(new VarNode("productLabel"));

        final JoinGroupNode whereClause = new JoinGroupNode();
        queryRoot.setWhereClause(whereClause);

        // ?product rdfs:label ?productLabel .
        p0 = new StatementPatternNode(new VarNode("product"),
                new ConstantNode(rdfsLabel.getIV()), new VarNode(
                        "productLabel"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p0.setId(nextId.incrementAndGet());

        // productInstance bsbm:productFeature ?prodFeature .
        p1 = new StatementPatternNode(
                new ConstantNode(product53999.getIV()), new ConstantNode(
                        productFeature.getIV()),
                new VarNode("prodFeature"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p1.setId(nextId.incrementAndGet());
        
        // ?product bsbm:productFeature ?prodFeature .
        p2 = new StatementPatternNode(new VarNode("product"),
                new ConstantNode(productFeature.getIV()), new VarNode(
                        "prodFeature"), null/* c */, Scope.DEFAULT_CONTEXTS);
        p2.setId(nextId.incrementAndGet());
        
        // productInstance bsbm:productPropertyNumeric1 ?origProperty1 .
        p3 = new StatementPatternNode(
                new ConstantNode(product53999.getIV()), new ConstantNode(
                        productPropertyNumeric1.getIV()), new VarNode(
                        "origProperty1"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p3.setId(nextId.incrementAndGet());
        
        // ?product bsbm:productPropertyNumeric1 ?simProperty1 .
        p4 = new StatementPatternNode(new VarNode("product"),
                new ConstantNode(productPropertyNumeric1.getIV()),
                new VarNode("simProperty1"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p4.setId(nextId.incrementAndGet());
        
        // productInstance bsbm:productPropertyNumeric2 ?origProperty2 .
        p5 = new StatementPatternNode(
                new ConstantNode(product53999.getIV()), new ConstantNode(
                        productPropertyNumeric2.getIV()), new VarNode(
                        "origProperty2"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p5.setId(nextId.incrementAndGet());
        
        // ?product bsbm:productPropertyNumeric2 ?simProperty2 .
        p6 = new StatementPatternNode(new VarNode("product"),
                new ConstantNode(productPropertyNumeric2.getIV()),
                new VarNode("simProperty2"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
        p6.setId(nextId.incrementAndGet());
        
        whereClause.addChild(p0);
        whereClause.addChild(p1);
        whereClause.addChild(p2);
        whereClause.addChild(p3);
        whereClause.addChild(p4);
        whereClause.addChild(p5);
        whereClause.addChild(p6);

        // FILTER (productInstance != ?product)
        c0 = new FilterNode(FunctionNode.NE(
                new ConstantNode(product53999.getIV()), new VarNode(
                        "product")));

        whereClause.addChild(c0);

        // FILTER (?simProperty1 < (?origProperty1 + 120) &&
        // ?simProperty1 > (?origProperty1 - 120))
        {
            final ValueExpressionNode left = new FunctionNode(
                    FunctionRegistry.LT, null/* scalarArgs */,
                    new ValueExpressionNode[] {//
                    new VarNode("simProperty1"),//
                            new FunctionNode(FunctionRegistry.ADD,
                                    null/* scalarArgs */,
                                    new ValueExpressionNode[] {//
                                    new VarNode("origProperty1"),//
                                            new ConstantNode(_120.getIV()) }) //
                    });

            final ValueExpressionNode right = new FunctionNode(
                    FunctionRegistry.GT, null/* scalarArgs */,
                    new ValueExpressionNode[] {//
                    new VarNode("simProperty1"),//
                            new FunctionNode(FunctionRegistry.SUBTRACT,
                                    null/* scalarArgs */,
                                    new ValueExpressionNode[] {//
                                    new VarNode("origProperty1"),//
                                            new ConstantNode(_120.getIV()) }) //
                    });

            final ValueExpressionNode expr = new FunctionNode(
                    FunctionRegistry.AND, null/* scalarValues */,
                    new ValueExpressionNode[] { left, right });

            c1 = new FilterNode(expr);

            whereClause.addChild(c1);
        }

        // FILTER (?simProperty2 < (?origProperty2 + 170) &&
        // ?simProperty2 > (?origProperty2 - 170))
        {
            final ValueExpressionNode left = new FunctionNode(
                    FunctionRegistry.LT, null/* scalarArgs */,
                    new ValueExpressionNode[] {//
                    new VarNode("simProperty2"),//
                            new FunctionNode(FunctionRegistry.ADD,
                                    null/* scalarArgs */,
                                    new ValueExpressionNode[] {//
                                    new VarNode("origProperty2"),//
                                            new ConstantNode(_170.getIV()) }) //
                    });

            final ValueExpressionNode right = new FunctionNode(
                    FunctionRegistry.GT, null/* scalarArgs */,
                    new ValueExpressionNode[] {//
                    new VarNode("simProperty2"),//
                            new FunctionNode(FunctionRegistry.SUBTRACT,
                                    null/* scalarArgs */,
                                    new ValueExpressionNode[] {//
                                    new VarNode("origProperty2"),//
                                            new ConstantNode(_170.getIV()) }) //
                    });

            final ValueExpressionNode expr = new FunctionNode(
                    FunctionRegistry.AND, null/* scalarValues */,
                    new ValueExpressionNode[] { left, right });

            c2 = new FilterNode(expr);

            whereClause.addChild(c2);

        }

        constraints = new FilterNode[] { c0, c1, c2 };

        {
            final OrderByNode orderByNode = new OrderByNode();
            queryRoot.setOrderBy(orderByNode);
            orderByNode.addExpr(new OrderByExpr(
                    new VarNode("productLabel"), true/* ascending */));
        }

        queryRoot.setSlice(new SliceNode(0L/* offset */, 5L/* limit */));

        C0 = TestStaticAnalysis_CanJoinUsingConstraints.asSet(new FilterNode[] { c0 });
        C1 = TestStaticAnalysis_CanJoinUsingConstraints.asSet(new FilterNode[] { c1 });
        C2 = TestStaticAnalysis_CanJoinUsingConstraints.asSet(new FilterNode[] { c2 });

        
        // Cache the value expressions for both ASTs.
        {
            
            final IBindingSet[] bsets = new IBindingSet[] {};

            final AST2BOpContext context = new AST2BOpContext(
                    new ASTContainer(queryRoot), store);

            new ASTSetValueExpressionsOptimizer().optimize(context,
                  new QueryNodeWithBindingSet(queryRoot, bsets));
        }

    } // ctor

} // Setup
