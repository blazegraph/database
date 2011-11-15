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

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.Annotations;

/**
 * Test suite for {@link ASTStaticJoinOptimizer}.
 */
public class TestASTRunFirstRunLastOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTRunFirstRunLastOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTRunFirstRunLastOptimizer(String name) {
        super(name);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_runFirst() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));
        
        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));

        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));

        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));

        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));

        @SuppressWarnings("rawtypes")
        final IV g = makeIV(new URIImpl("http://example/g"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            // pre-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("x"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));
            
            where.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(a), new ConstantNode(a)));
            
            where.addChild(new StatementPatternNode(new VarNode("b"),
                    new ConstantNode(b), new ConstantNode(b)));

            final StatementPatternNode cSPN = new StatementPatternNode(new VarNode("c"),
                    new ConstantNode(c), new ConstantNode(c));
            
            cSPN.setQueryHint(QueryHints.RUN_LAST, "true");
            
            where.addChild(cSPN);

            where.addChild(new StatementPatternNode(new VarNode("d"),
                    new ConstantNode(d), new ConstantNode(d)));

            final StatementPatternNode eSPN = new StatementPatternNode(new VarNode("e"),
                    new ConstantNode(e), new ConstantNode(e));
            
            eSPN.setQueryHint(QueryHints.RUN_FIRST, "true");
            
            where.addChild(eSPN);

            where.addChild(new StatementPatternNode(new VarNode("f"),
                    new ConstantNode(f), new ConstantNode(f)));

            where.addChild(new StatementPatternNode(new VarNode("g"),
                    new ConstantNode(g), new ConstantNode(g)));

            // post-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("y"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));
            
            // post-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("z"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            // pre-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("x"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));
            
            final StatementPatternNode eSPN = new StatementPatternNode(new VarNode("e"),
                    new ConstantNode(e), new ConstantNode(e));
            
            eSPN.setQueryHint(QueryHints.RUN_FIRST, "true");
            
            where.addChild(eSPN);

            where.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(a), new ConstantNode(a)));
            
            where.addChild(new StatementPatternNode(new VarNode("b"),
                    new ConstantNode(b), new ConstantNode(b)));

            where.addChild(new StatementPatternNode(new VarNode("d"),
                    new ConstantNode(d), new ConstantNode(d)));

            where.addChild(new StatementPatternNode(new VarNode("f"),
                    new ConstantNode(f), new ConstantNode(f)));

            where.addChild(new StatementPatternNode(new VarNode("g"),
                    new ConstantNode(g), new ConstantNode(g)));

            final StatementPatternNode cSPN = new StatementPatternNode(new VarNode("c"),
                    new ConstantNode(c), new ConstantNode(c));
            
            cSPN.setQueryHint(QueryHints.RUN_LAST, "true");
            
            where.addChild(cSPN);

            // post-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("y"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));
            
            // post-filter
            where.addChild(new FilterNode(
            		new FunctionNode(FunctionRegistry.GT, null,
            				new ValueExpressionNode[] {
            					new VarNode("z"),
            					new ConstantNode(makeIV(new LiteralImpl("0", XSD.INTEGER)))
            		})));

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }

        final IASTOptimizer rewriter = new ASTRunFirstRunLastOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    private StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality) {
        
        return newStatementPatternNode(s, p, o, cardinality, false);
        
    }
        
    private StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality, final boolean optional) {
        
        final StatementPatternNode sp = new StatementPatternNode(s, p, o);
        
        sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
        
        if (optional) {
            
            sp.setOptional(true);
            
        }
        
        return sp;
        
    }

}
