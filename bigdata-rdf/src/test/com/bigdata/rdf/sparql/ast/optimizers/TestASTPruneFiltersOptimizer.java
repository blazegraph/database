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

import com.bigdata.bop.Constant;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTLiftPreFiltersOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTPruneFiltersOptimizer extends
        AbstractASTEvaluationTestCase {
    
    public TestASTPruneFiltersOptimizer() {
        super();
    }

    public TestASTPruneFiltersOptimizer(final String name) {
        super(name);
    }

    /**
     * This test is be based on <code>Filter-nested - 2</code> (Filter on
     * variable ?v which is not in scope).
     * 
     * <pre>
     * PREFIX : <http://example/> 
     * 
     * SELECT ?v
     * { :x :p ?v . { FILTER(?v = 1) } }
     * </pre>
     * 
     * This is one of the DAWG "bottom-up" evaluation semantics tests.
     * <code>?v</code> is not bound in the FILTER because it is evaluated with
     * bottom up semantics and therefore the bindings from the parent group are
     * not visible.
     */
    public void test_pruneFiltersOptimizer_filter_nested_2()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . { FILTER(?v = 1) } }";
        
        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x = f.createURI("http://example/x");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataValue[] values = new BigdataValue[] { x, p };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = (QueryRoot) new ASTPruneFiltersOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        /*
         * Create the expected AST for the WHERE clause.
         */
        final JoinGroupNode expectedWhereClause = new JoinGroupNode();
        {
            // :x :q ?w
            expectedWhereClause.addChild(new StatementPatternNode(//
                    new ConstantNode(new Constant(x.getIV())),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));

        }

        assertEquals("modifiedClause", expectedWhereClause,
                queryRoot.getWhereClause());

    }
    
}
