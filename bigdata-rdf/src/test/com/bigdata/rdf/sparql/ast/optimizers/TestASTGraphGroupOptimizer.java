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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.TestNamedGraphs;

/**
 * Test suite for {@link ASTGraphGroupOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see TestNamedGraphs
 */
public class TestASTGraphGroupOptimizer extends
        AbstractASTEvaluationTestCase {

    public TestASTGraphGroupOptimizer() {
        super();
    }

    public TestASTGraphGroupOptimizer(final String name) {
        super(name);
    }

    /**
     * Unit test where nested GRAPH patterns share the same variable.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s
     * WHERE {
     *   GRAPH ?g {
     *     ?s :p :o .
     *     OPTIONAL { ?g :p2 ?s }
     *   }
     * }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_graphGroupOptimizer_01() throws MalformedQueryException {
        
        final String queryStr = ""//
                + "PREFIX : <http://example.org/>\n"//
                + "SELECT ?s\n"//
                + "WHERE {\n"//
                + "  GRAPH ?g {\n"//
                + "    ?s :p :o .\n"//
                + "    OPTIONAL { ?g :p2 ?s }\n"//
                + "  }\n"//
                + "}";

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI p = f.createURI("http://example.org/p");
        final BigdataURI p2= f.createURI("http://example.org/p2");
        final BigdataURI o = f.createURI("http://example.org/o");
        final BigdataValue[] values = new BigdataValue[] { p, p2, o };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = BOpUtility.deepCopy(queryRoot);
        
        queryRoot = (QueryRoot) new ASTGraphGroupOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        /*
         * Create the expected AST.
         */
        final JoinGroupNode expectedClause = new JoinGroupNode();
        {

            final VarNode g = new VarNode("g");
            final VarNode s = new VarNode("s");
            
            final JoinGroupNode graphGroup = new JoinGroupNode();
            graphGroup.setContext(g);
            expectedClause.addChild(graphGroup);

            // ?s :p :o
            graphGroup.addChild(new StatementPatternNode(//
                    s,// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new ConstantNode(new Constant(o.getIV())),// o
                    g,// c
                    Scope.NAMED_CONTEXTS//
                    ));
        
            final JoinGroupNode optionalGroup = new JoinGroupNode(true/*optional*/);
            graphGroup.addChild(optionalGroup);
            
            // ?g :p2 ?s
            optionalGroup.addChild(new StatementPatternNode(//
                    g,// s
                    new ConstantNode(new Constant(p2.getIV())),// p
                    s,// o
                    g,// c
                    Scope.NAMED_CONTEXTS//
                    ));

        }

        assertSameAST(expectedClause, queryRoot.getWhereClause());

    }

    /** 
     * Unit test where nested GRAPH patterns do not share the same variable.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s
     * WHERE {
     *   GRAPH ?g {
     *     ?s :p :o .
     *     GRAPH ?g1 { ?g :p2 ?s }
     *   }
     * }
     * </pre>
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   public void test_graphGroupOptimizer_02() throws MalformedQueryException {
       
       final String queryStr = ""//
               + "PREFIX : <http://example.org/>\n"//
               + "SELECT ?s\n"//
               + "WHERE {\n"//
               + "  GRAPH ?g {\n"//
               + "    ?s :p :o .\n"//
               + "    GRAPH ?g1 { ?g :p2 ?s }\n"//
               + "  }\n"//
               + "}";

       /*
        * Add the Values used in the query to the lexicon. This makes it
        * possible for us to explicitly construct the expected AST and
        * the verify it using equals().
        */
       final BigdataValueFactory f = store.getValueFactory();
       final BigdataURI p = f.createURI("http://example.org/p");
       final BigdataURI p2= f.createURI("http://example.org/p2");
       final BigdataURI o = f.createURI("http://example.org/o");
       final BigdataValue[] values = new BigdataValue[] { p, p2, o };
       store.getLexiconRelation()
               .addTerms(values, values.length, false/* readOnly */);

       final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
               .parseQuery2(queryStr, baseURI);

       final AST2BOpContext context = new AST2BOpContext(astContainer, store);

       QueryRoot queryRoot = astContainer.getOriginalAST();
       
       queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
               context, queryRoot, null/* bindingSets */);

       queryRoot = BOpUtility.deepCopy(queryRoot);
       
       queryRoot = (QueryRoot) new ASTGraphGroupOptimizer().optimize(
               context, queryRoot, null/* bindingSets */);

       /*
        * Create the expected AST.
        */
       final JoinGroupNode expectedClause = new JoinGroupNode();
       {

           final VarNode g = new VarNode("g");
           final VarNode g1 = new VarNode("g1");
           final VarNode s = new VarNode("s");
           
           final JoinGroupNode graphGroup = new JoinGroupNode();
           expectedClause.addChild(graphGroup);
           graphGroup.setContext(g);

           // ?s :p :o
           graphGroup.addChild(new StatementPatternNode(//
                   s,// s
                   new ConstantNode(new Constant(p.getIV())),// p
                   new ConstantNode(new Constant(o.getIV())),// o
                   g,// c
                   Scope.NAMED_CONTEXTS//
                   ));
       
           final JoinGroupNode innerGraphGroup = new JoinGroupNode();
           graphGroup.addChild(innerGraphGroup);
           innerGraphGroup.setContext(g1);
           
           // ?g :p2 ?s
           innerGraphGroup.addChild(new StatementPatternNode(//
                   g,// s
                   new ConstantNode(new Constant(p2.getIV())),// p
                   s,// o
                   g1,// c
                   Scope.NAMED_CONTEXTS//
                   ));
           
            final FilterNode filterNode = new FilterNode(FunctionNode.sameTerm(
                    g, g1));

            AST2BOpUtility.toVE(context.getLexiconNamespace(),
                    filterNode.getValueExpressionNode());
            
            innerGraphGroup.addChild(filterNode);

        }

        assertSameAST(expectedClause, queryRoot.getWhereClause());

    }

}
