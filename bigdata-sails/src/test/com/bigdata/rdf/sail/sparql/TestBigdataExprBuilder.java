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

import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link BigdataExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO Test each query type (SELECT, ASK, DESCRIBE, CONSTRUCT).
 * 
 * TODO Test SLICE.
 * 
 * TODO Test conversion of value expressions.
 * 
 * TODO Test SUBQUERY.
 * 
 * TODO Modify grammar and test named subquery (WITH AS INCLUDE).
 * 
 * TODO Test SELECT DISTINCT, SELECT REDUCED, SELECT *.
 */
public class TestBigdataExprBuilder extends AbstractBigdataExprBuilderTestCase {

    private static final Logger log = Logger
            .getLogger(TestBigdataExprBuilder.class);
    
    public TestBigdataExprBuilder() {
    }

    public TestBigdataExprBuilder(String name) {
        super(name);
    }

    /**
     * Unit test for simple SELECT query
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_s_where_s_p_o() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);
//        final QueryRoot actual = new QueryRoot();
//        {
//
//            final ProjectionNode projection2 = new ProjectionNode();
//            projection2.addProjectionVar(new VarNode("s"));
//            actual.setProjection(projection2);
//
//            final JoinGroupNode whereClause2 = new JoinGroupNode();
//            whereClause2.addChild(new StatementPatternNode(new VarNode("s"),
//                    new VarNode("p"), new VarNode("o")));
//            actual.setRoot(whereClause2);
//        }

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT DISTINCT
     * 
     * <pre>
     * SELECT DISTINCT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_distinct() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select DISTINCT ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setDistinct(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT REDUCED
     * 
     * <pre>
     * SELECT REDUCED ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_reduced() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select REDUCED ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setReduced(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT query with a wildcard (<code>*</code>).
     * 
     * <pre>
     * SELECT * where {?s ?p ?o}
     * </pre>
     */
    public void test_select_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select * where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SLICE in SELECT query.
     * <pre>
     * SELECT ?s where {?s ?p ?o} LIMIT 10 OFFSET 5
     * </pre>
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_slice() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o} limit 10 offset 5";

        final QueryRoot expected = new QueryRoot();
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
            expected.setWhereClause(whereClause);
            
            final SliceNode slice = new SliceNode();
            expected.setSlice(slice);
            slice.setLimit(10);
            slice.setOffset(5);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
}
