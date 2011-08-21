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
/*
 * Created on Aug 20, 2011
 */
package com.bigdata.rdf.sail.sparql;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;

import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ValueExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBigdataExprBuilder.java 5063 2011-08-21 16:13:03Z
 *          thompsonbry $
 * 
 *          FIXME Test handling of URIs, bnodes, and literals all of which get
 *          turned into ConstantNode instances.
 * 
 *          FIXME Explicit test of COUNT(DISTINCT *) and COUNT(*).
 */
public class TestValueExprBuilder extends AbstractBigdataExprBuilderTestCase {

    private static final Logger log = Logger
            .getLogger(TestValueExprBuilder.class);
    
    public TestValueExprBuilder() {
    }

    public TestValueExprBuilder(String name) {
        super(name);
    }

    /**
     * Simple variable rename in select expression.
     * 
     * <pre>
     * SELECT (?s as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_bind() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"),//
                            new VarNode("s")//
                    ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * SELECT using math expression.
     * 
     * <pre>
     * SELECT (?s + ?o as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_math_expr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s + ?o as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode(lex, //
                                    FunctionRegistry.ADD,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * Select using comparison expression.
     * 
     * <pre>
     * SELECT (?s < ?o as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_compare_expr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s < ?o as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode(lex, //
                                    FunctionRegistry.LT,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);
        
        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }
    
}
