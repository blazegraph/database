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
 * Created on Aug 20, 2011
 */
package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.ClearGraph;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;

/**
 * Test suite for bigdata specific extensions in {@link UpdateExprBuilder}.
 * 
 * @see https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestUpdateExprBuilder2 extends AbstractBigdataExprBuilderTestCase {

    public TestUpdateExprBuilder2() {
    }

    public TestUpdateExprBuilder2(String name) {
        super(name);
    }

    /*
     * Bigdata specific extensions for named solution sets.
     */
    
    /**
     * INSERT INTO named solution set (bigdata extension).
     * 
     * <pre>
     * PREFIX dc:  <http://purl.org/dc/elements/1.1/>
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * 
     * INSERT INTO %cached_solution_set 
     * WHERE
     *   { GRAPH  <http://example/bookStore>
     *        { ?book dc:date ?date .
     *          FILTER ( ?date > "1970-01-01T00:00:00-02:00"^^xsd:dateTime )
     *          ?book ?p ?v
     *   } }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/524 (SPARQL Cache)
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert__insertInto_01() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = //
                  "PREFIX dc:  <http://purl.org/dc/elements/1.1/>\n"//
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"//
                + "INSERT INTO %cached_solution_set\n"//
                + "SELECT ?book ?date\n"//
                + "WHERE\n"//
                + "   { GRAPH  <http://example/bookStore>\n"//
                + "        { ?book dc:date ?date .\n"//
                + "          FILTER ( ?date > \"1970-01-01T00:00:00-02:00\"^^xsd:dateTime )\n"//
                + "          ?book ?p ?v\n"//
                + "} }";
        
        final IV dcDate = makeIV(valueFactory.createURI("http://purl.org/dc/elements/1.1/date"));
        final IV dateTime = makeIV(valueFactory.createLiteral("1970-01-01T00:00:00-02:00",XSD.DATETIME));
        final IV bookstore = makeIV(valueFactory.createURI("http://example/bookStore"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadsDataOrNamedSolutionSet insertClause = new QuadsDataOrNamedSolutionSet(
                        "%cached_solution_set");
                
                assertTrue(insertClause.isSolutions());
                assertFalse(insertClause.isQuads());
                
                final ProjectionNode projection = new ProjectionNode();
                insertClause.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("book"));
                projection.addProjectionVar(new VarNode("date"));
                
                op.setInsertClause(insertClause);

            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();

                final JoinGroupNode graphGroup = new JoinGroupNode();

                whereClause.addChild(graphGroup);

                graphGroup.setContext(new ConstantNode(bookstore));

                graphGroup.addChild(new StatementPatternNode(
                        new VarNode("book"), new ConstantNode(dcDate),
                        new VarNode("date"), new ConstantNode(bookstore),
                        Scope.NAMED_CONTEXTS));

                graphGroup.addChild(new FilterNode(FunctionNode.GT(new VarNode(
                        "date"), new ConstantNode(dateTime))));

                graphGroup.addChild(new StatementPatternNode(
                        new VarNode("book"), new VarNode("p"),
                        new VarNode("v"), new ConstantNode(bookstore),
                        Scope.NAMED_CONTEXTS));

                op.setWhereClause(whereClause);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * DELETE FROM named solution set (bigdata extension).
     * 
     * <pre>
     * PREFIX dc:  <http://purl.org/dc/elements/1.1/>
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * 
     * DELETE FROM %cached_solution_set 
     * WHERE
     *   { GRAPH  <http://example/bookStore>
     *        { ?book dc:date ?date .
     *          FILTER ( ?date > "1970-01-01T00:00:00-02:00"^^xsd:dateTime )
     *          ?book ?p ?v
     *   } }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/524 (SPARQL Cache)
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert__deleteFrom_01() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = //
                  "PREFIX dc:  <http://purl.org/dc/elements/1.1/>\n"//
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"//
                + "DELETE FROM %cached_solution_set\n"//
                + "SELECT ?book ?date\n"//
                + "WHERE\n"//
                + "   { GRAPH  <http://example/bookStore>\n"//
                + "        { ?book dc:date ?date .\n"//
                + "          FILTER ( ?date > \"1970-01-01T00:00:00-02:00\"^^xsd:dateTime )\n"//
                + "          ?book ?p ?v\n"//
                + "} }";
        
        final IV dcDate = makeIV(valueFactory.createURI("http://purl.org/dc/elements/1.1/date"));
        final IV dateTime = makeIV(valueFactory.createLiteral("1970-01-01T00:00:00-02:00",XSD.DATETIME));
        final IV bookstore = makeIV(valueFactory.createURI("http://example/bookStore"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadsDataOrNamedSolutionSet deleteClause = new QuadsDataOrNamedSolutionSet(
                        "%cached_solution_set");
                
                final ProjectionNode projection = new ProjectionNode();
                deleteClause.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("book"));
                projection.addProjectionVar(new VarNode("date"));
                
                op.setDeleteClause(deleteClause);

            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();

                final JoinGroupNode graphGroup = new JoinGroupNode();

                whereClause.addChild(graphGroup);

                graphGroup.setContext(new ConstantNode(bookstore));

                graphGroup.addChild(new StatementPatternNode(
                        new VarNode("book"), new ConstantNode(dcDate),
                        new VarNode("date"), new ConstantNode(bookstore),
                        Scope.NAMED_CONTEXTS));

                graphGroup.addChild(new FilterNode(FunctionNode.GT(new VarNode(
                        "date"), new ConstantNode(dateTime))));

                graphGroup.addChild(new StatementPatternNode(
                        new VarNode("book"), new VarNode("p"),
                        new VarNode("v"), new ConstantNode(bookstore),
                        Scope.NAMED_CONTEXTS));

                op.setWhereClause(whereClause);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * create solutions %solutionSet
     * </pre>
     */
    public void test_create_solutions() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CREATE ( SILENT )? SOLUTIONS SolutionSetName
        final String sparql = "create solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CreateGraph op = new CreateGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");
            
            assertTrue(op.isTargetSolutionSet());

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * create silent solutions %solutionSet
     * </pre>
     */
    public void test_create_solutions_silent() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CREATE ( SILENT )? SOLUTIONS SolutionSetName
        final String sparql = "create silent solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CreateGraph op = new CreateGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

            op.setSilent(true);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Example of a CREATE using a triples block to provision the named solution
     * set.
     * 
     * <pre>
     * create silent solutions %solutionSet {
     *    hint:Query hint:engine "cache" .
     *    hint:Query hint:expireAge "100000"^^<http://www.w3.org/2001/XMLSchema#long> .
     * }
     * </pre>
     * 
     * TODO Test to verify that we reject variables in the triples template
     * (ground triples only).
     * 
     * TODO Maybe specify the implementation class for the persistence?
     */
//    @SuppressWarnings("rawtypes")
    public void test_create_solutions_silent_params()
            throws MalformedQueryException, TokenMgrError, ParseException {

        //CREATE ( SILENT )? SOLUTIONS SolutionSetName { TriplesTemplate }
        final String sparql = "create silent solutions %solutionSet {\n"//
                +"  hint:Query hint:engine \"cache\" .\n"
                +"  hint:Query hint:expireAge \"100000\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        		+"}"
        		;

        final UpdateRoot expected = new UpdateRoot();
        {

            final CreateGraph op = new CreateGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

            op.setSilent(true);
            
//          final IV hintQuery = makeIV(valueFactory.createURI(QueryHints.NAMESPACE+QueryHintScope.Query));
//          final IV hintEngine = makeIV(valueFactory.createURI(QueryHints.NAMESPACE+"engine"));
//          final IV hintExpireAge = makeIV(valueFactory.createURI(QueryHints.NAMESPACE+"expireAge"));
//          final IV cache = makeIV(valueFactory.createLiteral("cache"));
//          final IV millis = makeIV(valueFactory.createLiteral("100000",XSD.LONG));

//            final QuadData params = new QuadData();
//            
//            params.addChild(new StatementPatternNode(
//                    new ConstantNode(hintQuery), new ConstantNode(hintEngine),
//                    new ConstantNode(cache)));
//
//            params.addChild(new StatementPatternNode(
//                    new ConstantNode(hintQuery), new ConstantNode(hintExpireAge),
//                    new ConstantNode(millis)));

          final BigdataURI hintQuery = valueFactory.createURI(QueryHints.NAMESPACE+QueryHintScope.Query);
          final BigdataURI hintEngine = valueFactory.createURI(QueryHints.NAMESPACE+"engine");
          final BigdataURI hintExpireAge = valueFactory.createURI(QueryHints.NAMESPACE+"expireAge");
          final BigdataLiteral cache = valueFactory.createLiteral("cache");
          final BigdataLiteral millis = valueFactory.createLiteral("100000",XSD.LONG);

            final BigdataStatement[] params = new BigdataStatement[] {
                    valueFactory.createStatement(//
                            (BigdataResource)hintQuery, //
                            (BigdataURI)hintEngine,//
                            (BigdataValue)cache, //
                            null, // c
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)hintQuery,//
                            (BigdataURI)hintExpireAge,//
                            (BigdataValue)millis,//
                            null,// c 
                            StatementEnum.Explicit),//
            };
            
            op.setParams(params);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * drop solutions %solutionSet
     * </pre>
     */
    public void test_drop_solutions_namedSet() throws MalformedQueryException,
            TokenMgrError, ParseException {

        // DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "drop solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * drop silent solutions %solutionSet
     * </pre>
     */
    public void test_drop_solutions_namedSet_silent() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "drop silent solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

            op.setSilent(true);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * drop solutions
     * </pre>
     */
    public void test_drop_solutions() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "drop solutions";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            op.setAllSolutionSets(true);

            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop graphs
     * </pre>
     */
    public void test_drop_graphs() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "drop graphs";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            op.setAllGraphs(true);
            
            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * clear solutions %solutionSet
     * </pre>
     */
    public void test_clear_solutions_namedSet() throws MalformedQueryException,
            TokenMgrError, ParseException {

        // DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "clear solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * clear silent solutions %solutionSet
     * </pre>
     */
    public void test_clear_solutions_namedSet_silent() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "clear silent solutions %solutionSet";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setTargetSolutionSet("%solutionSet");

            op.setSilent(true);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * clear solutions
     * </pre>
     */
    public void test_clear_solutions() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME))
        final String sparql = "clear solutions";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            op.setAllSolutionSets(true);
            
            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    

    /**
     * <pre>
     * clear graphs
     * </pre>
     */
    public void test_clear_graphs() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
        final String sparql = "clear graphs";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            op.setAllGraphs(true);
            
            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
}
