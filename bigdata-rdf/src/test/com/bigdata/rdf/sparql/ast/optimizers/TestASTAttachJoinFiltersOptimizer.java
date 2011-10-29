/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.BSBMQ5Setup;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTAttachJoinFiltersOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTAttachJoinFiltersOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTAttachJoinFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTAttachJoinFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * TODO Write unit test. Note that the core logic for deciding join filter
     * attachment is tested elsewhere.
     * 
     * TODO Write unit test for re-attachment. This differs because the filters
     * need to be collected from the required statement pattern nodes and then
     * reattached for a new join ordering. (RTO use case).
     */
    public void test_attachFilters() {

//        final BSBMQ5Setup s = new BSBMQ5Setup(store);
//
//        final StaticAnalysis sa = new StaticAnalysis(s.queryRoot);
//
//        final IJoinNode[] path = { s.p3, s.p4, s.p5, s.p6, s.p1, s.p2, s.p0 };
//
//        final FilterNode[][] actual = sa
//                .getJoinGraphConstraints(path, s.constraints,
//                        null/* knownBoundVars */, true/* pathIsComplete */);
//
//        @SuppressWarnings("unchecked")
//        final Set<FilterNode>[] expected = new Set[] { //
//                s.NA, // p3
//                asSet(new FilterNode[]{s.c0,s.c1}), // p4
//                s.NA, // p5
//                s.C2, // p6
//                s.NA, // p1
//                s.NA, // p2
//                s.NA, // p0
//        };
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        @SuppressWarnings("rawtypes")
//        final IV a = makeIV(RDF.TYPE);
//        @SuppressWarnings("rawtypes")
//        final IV polititian = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
//        @SuppressWarnings("rawtypes")
//        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
//        @SuppressWarnings("rawtypes")
//        final IV sponsor = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/sponsor"));
//        @SuppressWarnings("rawtypes")
//        final IV title = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/title"));
//        @SuppressWarnings("rawtypes")
//        final IV N = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#N"));
//        @SuppressWarnings("rawtypes")
//        final IV family = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#Family"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            {
//                final ProjectionNode projection = new ProjectionNode();
//                given.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//            }
//
//            final String set1 = "--nsr-1";
//            final String set2 = "--nsr-2";
//            final String set3 = "--nsr-3";
//
//            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
//            given.setNamedSubqueries(namedSubqueries);
//
//            // Required joins and simple optionals lifted into a named subquery.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set1);
//                namedSubqueries.add(nsr);
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
//                whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
//                        new ConstantNode(a), new ConstantNode(polititian),
//                        null/* c */, Scope.DEFAULT_CONTEXTS));
//
//                // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
//                {
//                    final StatementPatternNode sp = new StatementPatternNode(
//                            new VarNode("var1"), new ConstantNode(name),
//                            new VarNode("var6"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS);
//                    sp.setOptional(true);
//                    whereClause.addChild(sp);
//                }
//                
//            }
//
//            // First complex optional group.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set2);
//                namedSubqueries.add(nsr);
//
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var12"));
//                projection.addProjectionVar(new VarNode("var4"));
//                
//                whereClause.addChild(new NamedSubqueryInclude(set1));
//
//                {
//                    
//                    final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
//                    whereClause.addChild(optionalGroup1);
//
//                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
//                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
//                            "var12"), new ConstantNode(sponsor), new VarNode(
//                            "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
//                    
//                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
//                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
//                            "var12"), new ConstantNode(title), new VarNode(
//                            "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
//                    
//                }
//
//            }
//
//            // Second complex optional group.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set3);
//                namedSubqueries.add(nsr);
//
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var13"));
//                projection.addProjectionVar(new VarNode("var10"));
//                
//                whereClause.addChild(new NamedSubqueryInclude(set1));
//
//                {
//
//                    final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
//                    whereClause.addChild(optionalGroup2);
//
//                    // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
//                    optionalGroup2.addChild(new StatementPatternNode(
//                            new VarNode("var1"), new ConstantNode(N),
//                            new VarNode("var13"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS));
//
//                    // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family>
//                    // ?_var10
//                    optionalGroup2.addChild(new StatementPatternNode(
//                            new VarNode("var13"), new ConstantNode(family),
//                            new VarNode("var10"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS));
//
//                }
//            }
//            
//            // WHERE
//            {
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                given.setWhereClause(whereClause);
//                whereClause.addChild(new NamedSubqueryInclude(set2));
//                whereClause.addChild(new NamedSubqueryInclude(set3));
//            }
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        final JoinGroupNode complexOpt1;
//        final JoinGroupNode complexOpt2;
//        {
//
//            {
//                final ProjectionNode projection = new ProjectionNode();
//                expected.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//            }
//
//            final String set1 = "--nsr-1";
//            final String set2 = "--nsr-2";
//            final String set3 = "--nsr-3";
//
//            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
//            expected.setNamedSubqueries(namedSubqueries);
//
//            // Required joins and simple optionals lifted into a named subquery.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set1);
//                namedSubqueries.add(nsr);
//                nsr.setJoinVars(new VarNode[]{/*none*/});
//                nsr.setDependsOn(new String[]{});
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
//                whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
//                        new ConstantNode(a), new ConstantNode(polititian),
//                        null/* c */, Scope.DEFAULT_CONTEXTS));
//
//                // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
//                {
//                    final StatementPatternNode sp = new StatementPatternNode(
//                            new VarNode("var1"), new ConstantNode(name),
//                            new VarNode("var6"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS);
//                    sp.setOptional(true);
//                    whereClause.addChild(sp);
//                }
//                
//            }
//
//            // First complex optional group.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set2);
//                namedSubqueries.add(nsr);
//                nsr.setJoinVars(new VarNode[]{/*none*/});
//                nsr.setDependsOn(new String[]{set1});
//
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var12"));
//                projection.addProjectionVar(new VarNode("var4"));
//                
//                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
//                nsi1.setJoinVars(new VarNode[]{/*none*/});
//                whereClause.addChild(nsi1);
//
//                {
//                    
//                    final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
//                    whereClause.addChild(optionalGroup1);
//
//                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
//                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
//                            "var12"), new ConstantNode(sponsor), new VarNode(
//                            "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
//                    
//                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
//                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
//                            "var12"), new ConstantNode(title), new VarNode(
//                            "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
//                    
//                    complexOpt1 = optionalGroup1;
//                }
//
//            }
//
//            // Second complex optional group.
//            {
//                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
//                        QueryType.SELECT, set3);
//                namedSubqueries.add(nsr);
//                nsr.setJoinVars(new VarNode[]{new VarNode("var1")});
//                nsr.setDependsOn(new String[]{set1});
//
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                nsr.setWhereClause(whereClause);
//
//                final ProjectionNode projection = new ProjectionNode();
//                nsr.setProjection(projection);
//                projection.addProjectionVar(new VarNode("var1"));
//                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var13"));
//                projection.addProjectionVar(new VarNode("var10"));
//                
//                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
//                nsi1.setJoinVars(new VarNode[]{/*none*/});
//                whereClause.addChild(nsi1);
//
//                {
//
//                    final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
//                    whereClause.addChild(optionalGroup2);
//
//                    // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
//                    optionalGroup2.addChild(new StatementPatternNode(
//                            new VarNode("var1"), new ConstantNode(N),
//                            new VarNode("var13"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS));
//
//                    // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family>
//                    // ?_var10
//                    optionalGroup2.addChild(new StatementPatternNode(
//                            new VarNode("var13"), new ConstantNode(family),
//                            new VarNode("var10"), null/* c */,
//                            Scope.DEFAULT_CONTEXTS));
//
//                    complexOpt2 = optionalGroup2;
//                }
//            }
//            
//            // WHERE
//            {
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                expected.setWhereClause(whereClause);
//                
//                final NamedSubqueryInclude nsi2 = new NamedSubqueryInclude(set2);
//                nsi2.setJoinVars(new VarNode[]{/*none*/});
//                whereClause.addChild(nsi2);
//                
//                final NamedSubqueryInclude nsi3 = new NamedSubqueryInclude(set3);
//                nsi3.setJoinVars(new VarNode[]{new VarNode("var1")});
//                whereClause.addChild(nsi3);
//            }
//
//        }
//
//        final IASTOptimizer rewriter = new ASTAttachJoinFiltersOptimizer();
//        
//        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
//                given), store);
//
//        final IQueryNode actual = rewriter.optimize(context,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//        final StaticAnalysis sa = new StaticAnalysis(expected);
//
//        // Test "incoming" bindings.
//        assertEquals(asSet(new Var[] { Var.var("var1") }),
//                sa.getDefinitelyIncomingBindings(complexOpt1,
//                        new LinkedHashSet<IVariable<?>>()));
//
//        // Test "incoming" bindings.
//        assertEquals(asSet(new Var[] { Var.var("var1") }),
//                sa.getDefinitelyIncomingBindings(complexOpt2,
//                        new LinkedHashSet<IVariable<?>>()));

        fail("write test");
        
    }

}
