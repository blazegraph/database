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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.rio.RDFParser.DatatypeHandling;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.AddGraph;
import com.bigdata.rdf.sparql.ast.ClearGraph;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CopyGraph;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteData;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.InsertData;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.MoveGraph;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QuadData;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link UpdateExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestUpdateExprBuilder extends AbstractBigdataExprBuilderTestCase {

//    private static final Logger log = Logger
//            .getLogger(TestBigdataExprBuilder.class);
    
    public TestUpdateExprBuilder() {
    }

    public TestUpdateExprBuilder(String name) {
        super(name);
    }

    /**
     * <pre>
     * load <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);
            
            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the SILENT keyword.
     * 
     * <pre>
     * load silent <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_silent_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load silent <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            op.setSilent(true);
            
            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the SILENT keyword.
     * 
     * <pre>
     * load silent <http://www.bigdata.com/data> into graph <http://www.bigdata.com/graph1>
     * </pre>
     */
    public void test_load_silent_graph_into_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load silent <http://www.bigdata.com/data> into graph <http://www.bigdata.com/graph1>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            op.setSilent(true);
            
            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for a sequence of simple LOAD operations
     */
    public void test_load_graphs() throws MalformedQueryException,
            TokenMgrError, ParseException {

        // LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load <http://www.bigdata.com/data>;\n" +
        		"load <http://www.bigdata.com/data1>;" +
        		"load <http://www.bigdata.com/data2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));

        }
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data1"))));

        }
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            final RDFParserOptions options = new RDFParserOptions();
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data2"))));

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * 
     * <pre>
     * load verifyData=true <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_verifyData_true() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load verifyData=true <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setVerifyData(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * 
     * <pre>
     * load verifyData=false <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_verifyData_false() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load verifyData=false <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setVerifyData(false);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load stopAtFirstError=true <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_stopAtFirstError_true() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load stopAtFirstError=true <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setStopAtFirstError(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load stopAtFirstError=false <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_stopAtFirstError_false() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load stopAtFirstError=false <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setStopAtFirstError(false);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load preserveBNodeIDs=true <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_preserveBNodeIDs_true() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load preserveBNodeIDs=true <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setPreserveBNodeIDs(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load preserveBNodeIDs=false <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_preserveBNodeIDs_false() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load preserveBNodeIDs=false <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setPreserveBNodeIDs(false);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load datatypeHandling=IGNORE <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_datatypeHandling_ignore() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load datatypeHandling=ignore <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setDatatypeHandling(DatatypeHandling.IGNORE);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load datatypeHandling=VERIFY <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_datatypeHandling_verify() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load datatypeHandling=verify <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setDatatypeHandling(DatatypeHandling.VERIFY);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load datatypeHandling=NORMALIZE <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_datatypeHandling_normalize() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load datatypeHandling=normalize <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

//            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();
            
            options.setDatatypeHandling(DatatypeHandling.VERIFY);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple LOAD operation with the non-standard extension
     * <pre>
     * load verifyData=true silent datatypeHandling=NORMALIZE <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_load_nonStandard_multipleOptions()
            throws MalformedQueryException, TokenMgrError, ParseException {

        // LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
        final String sparql = "load verifyData=true silent datatypeHandling=normalize <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            op.setSilent(true);

            final RDFParserOptions options = new RDFParserOptions();

            options.setVerifyData(true);
            
            options.setDatatypeHandling(DatatypeHandling.VERIFY);

            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple CLEAR operation.
     * 
     * <pre>
     * clear graph <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_clear_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "clear graph <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * clear default
     * </pre>
     */
    public void test_clear_default() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "clear default";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setScope(Scope.DEFAULT_CONTEXTS);
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * clear named
     * </pre>
     */
    public void test_clear_named() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "clear named";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setScope(Scope.NAMED_CONTEXTS);
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * clear all
     * </pre>
     */
    public void test_clear_all() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "clear all";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            op.setAllGraphs(true);
            
            op.setAllSolutionSets(true);
            
            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * clear silent all
     * </pre>
     */
    public void test_clear_silent_all() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //clear  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "clear silent all";

        final UpdateRoot expected = new UpdateRoot();
        {

            final ClearGraph op = new ClearGraph();

            expected.addChild(op);
            
            op.setAllGraphs(true);

            op.setAllSolutionSets(true);
            
            op.setSilent(true);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop graph <http://www.bigdata.com/data>
     * </pre>
     */
    public void test_drop_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //drop  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "drop graph <http://www.bigdata.com/data>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop default
     * </pre>
     */
    public void test_drop_default() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //drop  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "drop default";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setScope(Scope.DEFAULT_CONTEXTS);
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop named
     * </pre>
     */
    public void test_drop_named() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //drop  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "drop named";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setScope(Scope.NAMED_CONTEXTS);
           
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop all
     * </pre>
     */
    public void test_drop_all() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //drop  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "drop all";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            op.setAllGraphs(true);

            op.setAllSolutionSets(true);
            
            expected.addChild(op);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * drop silent all
     * </pre>
     */
    public void test_drop_silent_all() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //drop  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
        final String sparql = "drop silent all";

        final UpdateRoot expected = new UpdateRoot();
        {

            final DropGraph op = new DropGraph();

            expected.addChild(op);
            
            op.setAllGraphs(true);

            op.setAllSolutionSets(true);
            
            op.setSilent(true);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * create graph <http://www.bigdata.com/graph1>
     * </pre>
     */
    public void test_create() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CREATE ( SILENT )? GRAPH IRIref
        final String sparql = "create graph <http://www.bigdata.com/graph1>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CreateGraph op = new CreateGraph();

            expected.addChild(op);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * create silent graph <http://www.bigdata.com/graph1>
     * </pre>
     */
    public void test_create_silent() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //CREATE ( SILENT )? GRAPH IRIref
        final String sparql = "create silent graph <http://www.bigdata.com/graph1>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CreateGraph op = new CreateGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * copy <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_copy_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "copy <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CopyGraph op = new CopyGraph();

            expected.addChild(op);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * copy silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_copy_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "copy silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CopyGraph op = new CopyGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * copy silent <http://www.bigdata.com/graph1> to default 
     * </pre>
     */
    public void test_copy_graph_to_default() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "copy silent <http://www.bigdata.com/graph1> to default";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CopyGraph op = new CopyGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * copy silent default to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_copy_default_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "copy silent default to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final CopyGraph op = new CopyGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    //
    //
    //
    
    /**
     * <pre>
     * move <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_move_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "move <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final MoveGraph op = new MoveGraph();

            expected.addChild(op);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * move silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_move_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "move silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final MoveGraph op = new MoveGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * move silent <http://www.bigdata.com/graph1> to default 
     * </pre>
     */
    public void test_move_graph_to_default() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "move silent <http://www.bigdata.com/graph1> to default";

        final UpdateRoot expected = new UpdateRoot();
        {

            final MoveGraph op = new MoveGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * move silent default to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_move_default_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "move silent default to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final MoveGraph op = new MoveGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    //
    //
    //
    
    /**
     * <pre>
     * add <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_add_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "add <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final AddGraph op = new AddGraph();

            expected.addChild(op);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * add silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_add_graph_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "add silent <http://www.bigdata.com/graph1> to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final AddGraph op = new AddGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * add silent <http://www.bigdata.com/graph1> to default 
     * </pre>
     */
    public void test_add_graph_to_default() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "add silent <http://www.bigdata.com/graph1> to default";

        final UpdateRoot expected = new UpdateRoot();
        {

            final AddGraph op = new AddGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph1"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * add silent default to <http://www.bigdata.com/graph2> 
     * </pre>
     */
    public void test_silent_add_default_to_graph() throws MalformedQueryException,
            TokenMgrError, ParseException {

        //COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
        final String sparql = "add silent default to <http://www.bigdata.com/graph2>";

        final UpdateRoot expected = new UpdateRoot();
        {

            final AddGraph op = new AddGraph();

            expected.addChild(op);
            
            op.setSilent(true);
            
            op.setTargetGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/graph2"))));

        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    //
    //
    //
    
    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * INSERT DATA
     * { 
     *   <http://example/book1> dc:title "A new book" ;
     *                          dc:creator "A.N.Other" .
     * }
     * </pre>
     */
//    @SuppressWarnings("rawtypes")
    public void test_insert_data() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "INSERT DATA\n" + //
                "{\n" + //
                "  <http://example/book1> dc:title \"A new book\" ;\n" + //
                "                         dc:creator \"A.N.Other\" .\n" + //
                "}";

        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/elements/1.1/creator");
            final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
            final BigdataLiteral label1 = valueFactory.createLiteral("A new book");
            final BigdataLiteral label2 = valueFactory.createLiteral("A.N.Other");

            final BigdataStatement[] data = new BigdataStatement[] { //
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcTitle,//
                            (BigdataValue)label1, //
                            null,
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)book1, //
                            (BigdataURI)dcCreator, //
                            (BigdataValue) label2, //
                            null,//
                            StatementEnum.Explicit),//
//                    new SPO(book1, dcTitle, label1, null,
//                            StatementEnum.Explicit),//
//                    new SPO(book1, dcCreator, label2, null,
//                            StatementEnum.Explicit),//
            };
            op.setData(data);
            
        }
        
        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * PREFIX ns: <http://example.org/ns#>
     * INSERT DATA
     * { GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } }
     * </pre>
     */
//    @SuppressWarnings("rawtypes")
    public void test_insert_data_quads() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{ GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } }";

        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataURI bookstore = valueFactory.createURI("http://example/bookStore");
            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI price = valueFactory.createURI("http://example.org/ns#price");
            final BigdataLiteral i42 = valueFactory.createLiteral("42",XSD.INTEGER);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)price, //
                            (BigdataValue)i42, //
                            (BigdataResource)bookstore, //
                            StatementEnum.Explicit),//
            };

            op.setData(data);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * INSERT DATA { _:bnode a <http://example/Foo> . }
     * </pre>
     * @throws MalformedQueryException 
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/573">
     *      NullPointerException when attempting to INSERT DATA containing a
     *      blank node </a>
     */
    public void test_insert_data_ticket573() throws MalformedQueryException {
        
        final String sparql = "INSERT DATA { _:bnode a <http://example/Foo> . }";
        
        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataBNode bnode = valueFactory.createBNode("-anon-1");
//            final BigdataBNode bnode = valueFactory.createBNode("bnode");
            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.toString());
            final BigdataURI foo = valueFactory.createURI("http://example/Foo");

            final BigdataStatement[] data = new BigdataStatement[] { //
                    valueFactory.createStatement(//
                            bnode,//
                            rdfType,//
                            foo, //
                            null,// c 
                            StatementEnum.Explicit),//
            };

            op.setData(data);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        // no null pointer exception, but Sesame 2.7 Sparql parser will
        // not respect the bnode id, so we cannot assert same AST
//        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * PREFIX ns: <http://example.org/ns#>
     * INSERT DATA
     * { 
     *   <http://example/book1> dc:title "A new book" .
     *   <http://example/book1> dc:creator "A.N.Other" .
     *   GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } 
     * }
     * </pre>
     */
//    @SuppressWarnings("rawtypes")
    public void test_insert_data_triples_then_quads() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{\n"
                + "  <http://example/book1> dc:title \"A new book\" .\n"
                + "  <http://example/book1> dc:creator \"A.N.Other\" .\n" //
                + "  GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 }\n"
                + "}";

        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/elements/1.1/creator");
            final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
            final BigdataLiteral label1 = valueFactory.createLiteral("A new book");
            final BigdataLiteral label2 = valueFactory.createLiteral("A.N.Other");
            final BigdataURI bookstore = valueFactory.createURI("http://example/bookStore");
            final BigdataURI price = valueFactory.createURI("http://example.org/ns#price");
            final BigdataLiteral i42 = valueFactory.createLiteral("42",XSD.INTEGER);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcTitle,//
                            (BigdataValue)label1,//
                            null,//
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcCreator, //
                            (BigdataValue)label2,//
                            null,//
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)book1, //
                            (BigdataURI)price, //
                            (BigdataValue)i42, //
                            (BigdataResource)bookstore,//
                            StatementEnum.Explicit),//
            };
            
            op.setData(data);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    public void test_insert_data_triples_then_quads2() throws MalformedQueryException,
        TokenMgrError, ParseException {
       
        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{\n"
                + "    { <a:s1> <a:p1> <a:o1>\n }"
                + "    GRAPH <a:G> { <a:s> <a:p1> 'o1'; <a:p2> <a:o2> }\n" 
                + "    GRAPH <a:G1> { <a:s> <a:p1> 'o1'; <a:p2> <a:o2> } \n"
                + "    <a:s1> <a:p1> <a:o1>\n"
                + "}";

        parseUpdate(sparql, baseURI);

    }

    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * PREFIX ns: <http://example.org/ns#>
     * INSERT DATA
     * { 
     *   GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } 
     *   <http://example/book1> dc:title "A new book" .
     *   <http://example/book1> dc:creator "A.N.Other" .
     * }
     * </pre>
     */
//    @SuppressWarnings("rawtypes")
    public void test_insert_data_quads_then_triples() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{\n"
                + "  GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 }\n"
                + "  <http://example/book1> dc:title \"A new book\" .\n"
                + "  <http://example/book1> dc:creator \"A.N.Other\" .\n" //
                + "}";

        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/elements/1.1/creator");
            final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
            final BigdataLiteral label1 = valueFactory.createLiteral("A new book");
            final BigdataLiteral label2 = valueFactory.createLiteral("A.N.Other");
            final BigdataURI bookstore = valueFactory.createURI("http://example/bookStore");
            final BigdataURI price = valueFactory.createURI("http://example.org/ns#price");
            final BigdataLiteral i42 = valueFactory.createLiteral("42",XSD.INTEGER);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    valueFactory.createStatement(//
                            (BigdataResource)book1, //
                            (BigdataURI)price, //
                            (BigdataValue)i42, //
                            (BigdataResource)bookstore,//
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcTitle,//
                            (BigdataValue)label1,//
                            null,//
                            StatementEnum.Explicit),//
                    valueFactory.createStatement(//
                            (BigdataResource)book1, //
                            (BigdataURI)dcCreator,//
                            (BigdataValue)label2, //
                            null,//
                            StatementEnum.Explicit),//
            };
            op.setData(data);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * PREFIX ns: <http://example.org/ns#>
     * INSERT DATA
     * { 
     *   <http://example/book1> dc:title "A new book" .
     *   GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } 
     *   <http://example/book1> dc:creator "A.N.Other" .
     * }
     * </pre>
     */
//    @SuppressWarnings("rawtypes")
    public void test_insert_data_triples_quads_triples() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{\n"
                + "  <http://example/book1> dc:title \"A new book\" . "
                + "  GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 }\n"
                + "  <http://example/book1> dc:creator \"A.N.Other\" .\n" //
                + "}";

        final UpdateRoot expected = new UpdateRoot();
        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataURI book1 = valueFactory.createURI("http://example/book1");
            final BigdataURI dcCreator = valueFactory.createURI("http://purl.org/dc/elements/1.1/creator");
            final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
            final BigdataLiteral label1 = valueFactory.createLiteral("A new book");
            final BigdataLiteral label2 = valueFactory.createLiteral("A.N.Other");
            final BigdataURI bookstore = valueFactory.createURI("http://example/bookStore");
            final BigdataURI price = valueFactory.createURI("http://example.org/ns#price");
            final BigdataLiteral i42 = valueFactory.createLiteral("42",XSD.INTEGER);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcTitle,//
                            (BigdataValue)label1, //
                            null, //
                            StatementEnum.Explicit),//
                    
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)price,//
                            (BigdataValue)i42,//
                            (BigdataResource)bookstore,//
                            StatementEnum.Explicit),//
                    
                    valueFactory.createStatement(//
                            (BigdataResource)book1,//
                            (BigdataURI)dcCreator,//
                            (BigdataValue)label2,//
                            null,// 
                            StatementEnum.Explicit),//
            
            };
            op.setData(data);
            
        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * DELETE DATA
     * { GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  "Fundamentals of Compiler Desing" } } ;
     * 
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * INSERT DATA
     * { GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  "Fundamentals of Compiler Design" } }     *
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_data_quads_insert_data_quads()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "DELETE DATA\n"
                + "{ GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  \"Fundamentals of Compiler Desing\" } } ;\n"
                + "\n"
                + "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "INSERT DATA\n"
                + "{ GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  \"Fundamentals of Compiler Design\" } }";

        final IV book1 = makeIV(valueFactory.createURI("http://example/book1"));
        final IV dcTitle = makeIV(valueFactory.createURI("http://purl.org/dc/elements/1.1/title"));
        final IV label1 = makeIV(valueFactory.createLiteral("Fundamentals of Compiler Desing"));
        final IV label2 = makeIV(valueFactory.createLiteral("Fundamentals of Compiler Design"));
        final IV bookstore = makeIV(valueFactory.createURI("http://example/bookStore"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteData op = new DeleteData();

            expected.addChild(op);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    
            valueFactory.createStatement(//
                    (BigdataResource) book1.getValue(),//
                    (BigdataURI) dcTitle.getValue(),//
                    (BigdataValue) label1.getValue(),//
                    (BigdataResource) bookstore.getValue(),//
                    StatementEnum.Explicit//
                    ),//

            };
            op.setData(data);

        }

        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final BigdataStatement[] data = new BigdataStatement[] { //
                    
                    valueFactory.createStatement(//
                            (BigdataResource) book1.getValue(),//
                            (BigdataURI) dcTitle.getValue(),//
                            (BigdataValue) label2.getValue(),//
                            (BigdataResource) bookstore.getValue(),//
                            StatementEnum.Explicit//
                            ),//
//            new SPO(book1, dcTitle, label2, bookstore, StatementEnum.Explicit),//
                                
            };
            op.setData(data);
            
        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * DELETE/INSERT w/o WITH.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH <http://example/addresses> {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: Since WITH is not given, the context position is <code>null</code>
     * and the {@link Scope} is {@link Scope#DEFAULT_CONTEXTS} for any
     * {@link StatementPatternNode} which is outside of a GRAPH group. Within a
     * GRAPH group, the context is, of course, the constant IRI or variable
     * specified by the GRAPH group and the {@link Scope} is
     * {@link Scope#NAMED_CONTEXTS}.
     * 
     * @see StatementPatternNode
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_00() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group.
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,//new ConstantNode(addresses),
                        Scope.DEFAULT_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new ConstantNode(
                        addresses), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * DELETE/INSERT plus WITH to specify the graph.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * WITH <http://example/addresses>
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH <http://example/addresses> {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: The context position on the {@link StatementPatternNode} outside of
     * the GRAPH group is bound to the constant specified by the WITH clause.
     * <code>WITH uri</code> is a syntactic sugar for
     * <code>GRAPH uri {...}</code>, so the {@link StatementPatternNode} is also
     * marked as {@link Scope#NAMED_CONTEXTS}.
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "WITH <http://example/addresses>\n"//
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                /*
                 * Outside the GRAPH group. This is still marked as
                 * NAMED_CONTEXTS because WITH creates an implicit top-level
                 * GRAPH group wrapping the INSERT clause, DELETE clause, and
                 * WHERE clause.
                 */
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new ConstantNode(
                        addresses), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));
                
            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * DELETE/INSERT and USING.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * USING <http://example/addresses2>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH <http://example/addresses> {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: WITH is not used, so there is no implicit GRAPH wrapping the INSERT
     * clause, DELETE clause, and WHERE clause. Therefore, the context will be
     * <code>null</code> for the statement patterns in the DELETE clause and
     * INSERT clause and outside of the GRAPH clause in the WHERE clause. 
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "USING <http://example/addresses2>\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV addresses2 = makeIV(valueFactory.createURI("http://example/addresses2"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {
            
            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group.
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new ConstantNode(
                        addresses), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));
                
            }

            @SuppressWarnings("unchecked")
            final DatasetNode dataset = new DatasetNode(
                    (Set<IV>)Collections.singleton(addresses2),// defaultGraph
                    (Set)Collections.emptySet(),// namedGraphs
                    true // update
                    );
            
            op.setDataset(dataset);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * DELETE/INSERT and USING NAMED.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * USING NAMED <http://example/addresses>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: WITH is not used, so there is no implicit GRAPH wrapping the INSERT
     * clause, DELETE clause, and WHERE clause. Therefore, the context will be
     * <code>null</code> for the statement patterns in the DELETE clause and
     * INSERT clause and outside of the GRAPH clause in the WHERE clause.
     * <p> 
     * Note: For this test, the GRAPH group is also using a variable.
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_03() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "USING NAMED <http://example/addresses>\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH ?graph {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {
            
            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2), null,
                        Scope.DEFAULT_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group.
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new VarNode(
                        "graph"), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new VarNode("graph"),
                        Scope.NAMED_CONTEXTS)));
                
            }

            @SuppressWarnings("unchecked")
            final DatasetNode dataset = new DatasetNode(
                    (Set)Collections.emptySet(),// defaultGraph
                    (Set<IV>)Collections.singleton(addresses),// namedGraphs
                    true // update
                    );
            
            op.setDataset(dataset);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
      
    /**
     * DELETE/INSERT, WITH, USING, and USING NAMED.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * WITH <http://example/addresses>
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * USING NAMED <http://example/addresses>
     * USING NAMED <http://example/addresses2>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Since WITH is used, the DELETE, INSERT, and WHERE clause is each wrapped
     * by an implicit <code>GRAPH uri</code>.
     * <p>
     * Note: For this test, the GRAPH group is also using a variable. However,
     * that variable will be constrained by the WITH clause to a constant.
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_04() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "WITH <http://example/addresses>\n"//
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "USING NAMED <http://example/addresses>\n"//
                + "USING NAMED <http://example/addresses2>\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH ?graph {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV addresses2 = makeIV(valueFactory.createURI("http://example/addresses2"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {
            
            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group.
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new VarNode(
                        "graph"), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new VarNode("graph"),
                        Scope.NAMED_CONTEXTS)));
                
            }

            final Set<IV> namedGraphs = new LinkedHashSet<IV>();
            namedGraphs.add(addresses);
            namedGraphs.add(addresses2);
            
            @SuppressWarnings("unchecked")
            final DatasetNode dataset = new DatasetNode(
                    (Set)Collections.emptySet(),// defaultGraph
                    namedGraphs,//
                    true // update
                    );
            
            op.setDataset(dataset);

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * DELETE/INSERT w/o WITH but using a GRAPH in the DELETE and INSERT
     * clauses.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { 
     *   GRAPH <http://example/addresses> {?person foaf:givenName 'Bill'} .
     *   ?person foaf:givenName 'Bill'
     * }
     * INSERT {
     *   ?person foaf:givenName 'William'
     *   GRAPH <http://example/addresses> {?person foaf:givenName 'William'}
     * }
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH <http://example/addresses> {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: Since WITH is not given, the context position is <code>null</code>
     * and the {@link Scope} is {@link Scope#DEFAULT_CONTEXTS} for any
     * {@link StatementPatternNode} which is outside of a GRAPH group. Within a
     * GRAPH group, the context is, of course, the constant IRI or variable
     * specified by the GRAPH group and the {@link Scope} is
     * {@link Scope#NAMED_CONTEXTS}.
     * 
     * @see StatementPatternNode
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_10() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE {\n"//
                + "  GRAPH <http://example/addresses> { ?person foaf:givenName 'Bill' } .\n" //
                + "  ?person foaf:givenName 'Bill'\n"
                + "}\n"//
                + "INSERT {\n"//
                + "  ?person foaf:givenName 'William'\n"
                + "  GRAPH <http://example/addresses> { ?person foaf:givenName 'William' } .\n" //
                + "}\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
 
                // inside the GRAPH group
                deleteClause.addChild(new QuadData(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));

                // outside the GRAPH group
                deleteClause.addChild(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,
                        Scope.DEFAULT_CONTEXTS));
                        
                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                // outside the GRAPH group
                insertClause.addChild(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label2), null,
                        Scope.DEFAULT_CONTEXTS));

                // inside the GRAPH group
                insertClause.addChild(new QuadData(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label2), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group.
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), null,//new ConstantNode(addresses),
                        Scope.DEFAULT_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new ConstantNode(
                        addresses), new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS)));

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * DELETE/INSERT with WITH and a GRAPH in the DELETE and INSERT clauses.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * WITH <http://example/addresses2>
     * DELETE { 
     *   GRAPH ?graph {?person foaf:givenName 'Bill'} .
     *   ?person foaf:givenName 'Bill'
     * }
     * INSERT {
     *   ?person foaf:givenName 'William'
     *   GRAPH ?graph {?person foaf:givenName 'William'}
     * }
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: Since WITH is given, there is an implicit GRAPH wrapping the
     * INSERT, DELETE, and WHERE clauses.
     * 
     * @see StatementPatternNode
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_11() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                +"WITH <http://example/addresses>\n"
                + "DELETE {\n"//
                + "  GRAPH ?graph { ?person foaf:givenName 'Bill' } .\n" //
                + "  ?person foaf:givenName 'Bill'\n"
                + "}\n"//
                + "INSERT {\n"//
                + "  ?person foaf:givenName 'William'\n"
                + "  GRAPH ?graph { ?person foaf:givenName 'William' } .\n" //
                + "}\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH ?group {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
 
                // inside the GRAPH group
                deleteClause.addChild(new QuadData(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new VarNode("graph"),
                        Scope.NAMED_CONTEXTS)));

                // outside the GRAPH group (bound by WITH).
                deleteClause.addChild(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));
                        
                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {

                final QuadData insertClause = new QuadData();

                // outside the GRAPH group (bound by WITH)
                insertClause.addChild(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label2), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                // inside the GRAPH group
                insertClause.addChild(new QuadData(new StatementPatternNode(
                        new VarNode("person"), new ConstantNode(givenName),
                        new ConstantNode(label2), new VarNode("graph"),
                        Scope.NAMED_CONTEXTS)));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                op.setWhereClause(whereClause);

                // Outside the GRAPH group (bound by with).
                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1), new ConstantNode(addresses),
                        Scope.NAMED_CONTEXTS));

                // The GRAPH group.
                whereClause.addChild(new JoinGroupNode(new VarNode("group"),
                        new StatementPatternNode(new VarNode("person"),
                                new ConstantNode(givenName), new ConstantNode(
                                        label1), new VarNode("group"),
                                Scope.NAMED_CONTEXTS)));

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * DELETE/INSERT with default graph.
     * 
     * <pre>
     * PREFIX dc:  <http://purl.org/dc/elements/1.1/>
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * 
     * DELETE
     *  { ?book ?p ?v }
     * WHERE
     *  { ?book dc:date ?date .
     *    FILTER ( ?date > "1970-01-01T00:00:00-02:00"^^xsd:dateTime )
     *    ?book ?p ?v
     *  }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_20() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = //
        "PREFIX dc:  <http://purl.org/dc/elements/1.1/>\n"//
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"//
                + "DELETE\n"//
                + " { ?book ?p ?v }\n"//
                + "WHERE\n"//
                + " { ?book dc:date ?date .\n"//
                + "  FILTER ( ?date > \"1970-01-01T00:00:00-02:00\"^^xsd:dateTime )\n"//
                + "   ?book ?p ?v\n"//
                + "}";
        
        final IV dcDate = makeIV(valueFactory.createURI("http://purl.org/dc/elements/1.1/date"));
        final IV dateTime = makeIV(valueFactory.createLiteral("1970-01-01T00:00:00-02:00",XSD.DATETIME));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "book"), new VarNode("p"), new VarNode("v")));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();

                whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                        new ConstantNode(dcDate), new VarNode("date")));
                
                whereClause.addChild(new FilterNode(FunctionNode.GT(
                        new VarNode("date"), new ConstantNode(dateTime))));
                
                whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                        new VarNode("p"), new VarNode("v")));
                
                op.setWhereClause(whereClause);
                
            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * DELETE/INSERT plus GRAPH in INSERT and WHERE clauses.
     * 
     * <pre>
     * PREFIX dc:  <http://purl.org/dc/elements/1.1/>
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * 
     * INSERT 
     *   { GRAPH <http://example/bookStore2> { ?book ?p ?v } }
     * WHERE
     *   { GRAPH  <http://example/bookStore>
     *        { ?book dc:date ?date .
     *          FILTER ( ?date > "1970-01-01T00:00:00-02:00"^^xsd:dateTime )
     *          ?book ?p ?v
     *   } }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_21() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = //
        "PREFIX dc:  <http://purl.org/dc/elements/1.1/>\n"//
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"//
                + "INSERT \n"//
                + "   { GRAPH <http://example/bookStore2> { ?book ?p ?v } }\n"//
                + "WHERE\n"//
                + "   { GRAPH  <http://example/bookStore>\n"//
                + "        { ?book dc:date ?date .\n"//
                + "          FILTER ( ?date > \"1970-01-01T00:00:00-02:00\"^^xsd:dateTime )\n"//
                + "          ?book ?p ?v\n"//
                + "} }";
        
        final IV dcDate = makeIV(valueFactory.createURI("http://purl.org/dc/elements/1.1/date"));
        final IV dateTime = makeIV(valueFactory.createLiteral("1970-01-01T00:00:00-02:00",XSD.DATETIME));
        final IV bookstore = makeIV(valueFactory.createURI("http://example/bookStore"));
        final IV bookstore2 = makeIV(valueFactory.createURI("http://example/bookStore2"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData insertClause = new QuadData();

                final QuadData innerGraph = new QuadData();
                
                insertClause.addChild(innerGraph);
                        
                innerGraph.addChild(new StatementPatternNode(new VarNode(
                        "book"), new VarNode("p"), new VarNode("v"),
                        new ConstantNode(bookstore2), Scope.NAMED_CONTEXTS));

                op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

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
     * A unit test for the DELETE WHERE "shortcut" form.
     * <pre>
     * DELETE WHERE {?x foaf:name ?y }
     * </pre>
     */
    public void test_delete_where_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "DELETE WHERE {?x <http://xmlns.com/foaf/0.1/name> ?y }";

        @SuppressWarnings("rawtypes")
        final IV foafName = makeIV(valueFactory
                .createURI("http://xmlns.com/foaf/0.1/name"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(foafName), new VarNode("y")));

                op.setWhereClause(whereClause);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A unit test for the DELETE WHERE "shortcut" form.
     * <pre>
     * DELETE WHERE { GRAPH ?g { ?x foaf:name ?y } }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/568 (DELETE WHERE
     *      fails with Java AssertionError)
     */
    public void test_delete_where_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "DELETE WHERE { GRAPH ?g { ?x <http://xmlns.com/foaf/0.1/name> ?y } }";

        @SuppressWarnings("rawtypes")
        final IV foafName = makeIV(valueFactory
                .createURI("http://xmlns.com/foaf/0.1/name"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                final JoinGroupNode graphClause = new JoinGroupNode();
                
                graphClause.setContext(new VarNode("g"));

                whereClause.addChild(graphClause);
                
                graphClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(foafName), new VarNode("y"),
                        new VarNode("g"), Scope.NAMED_CONTEXTS));

                op.setWhereClause(whereClause);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
    * DELETE/INSERT with blank node in the DELETE template.
     * <p>
     * Note: blank nodes are not allowed in the DELETE clause template (nor in
     * DELETE DATA). This is because the blank nodes in the DELETE clause are
     * distinct for each solution plugged into that template. Thus they can not
     * match anything in the database.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
     *      DELETE/INSERT WHERE handling of blank nodes </a>
     */
    public void test_delete_insert_blankNodes01() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { [] foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";

        try {
            parseUpdate(sparql, baseURI);
            fail("Expecting exception: blank nodes not allowed in DELETE template");
        } catch (MalformedQueryException ex) {
            // Ignore expected exception.
//            ex.printStackTrace();
        }

    }

    /**
     * DELETE/INSERT with blank node in the DELETE template.
     * <p>
     * Note: blank nodes are not allowed in the DELETE clause template (nor in
     * DELETE DATA). This is because the blank nodes in the DELETE clause are
     * distinct for each solution plugged into that template. Thus they can not
     * match anything in the database.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
     *      DELETE/INSERT WHERE handling of blank nodes </a>
     */
    public void test_delete_insert_blankNodes02()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { ?person foaf:givenName [] }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH <http://example/addresses> {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";

        try {
            parseUpdate(sparql, baseURI);
            fail("Expecting exception: blank nodes not allowed in DELETE template");
        } catch (MalformedQueryException ex) {
            // Ignore expected exception.
//            ex.printStackTrace();
        }

    }

    /**
     * A unit test for the DELETE WHERE form without the shortcut, but
     * there the template and the where clause are the same.
     * <pre>
     * DELETE {?x foaf:name ?y } WHERE {?x foaf:name ?y }
     * </pre>
     */
    public void test_delete_where_without_shortcut_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "DELETE {?x <http://xmlns.com/foaf/0.1/name> ?y}\n"
                + " WHERE {?x <http://xmlns.com/foaf/0.1/name> ?y }";

        @SuppressWarnings("rawtypes")
        final IV foafName = makeIV(valueFactory
                .createURI("http://xmlns.com/foaf/0.1/name"));

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(foafName), new VarNode("y")));

                op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

            }
            {

                final JoinGroupNode whereClause = new JoinGroupNode();

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(foafName), new VarNode("y")));

                op.setWhereClause(whereClause);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

  /**
   * A sequences of DELETE/INSERT operations with different data sets and no
   * WITH clause.
   * 
   * <pre>
   * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
   * 
   * DELETE { ?person foaf:givenName 'Bill' }
   * WHERE {
   *   ?person foaf:givenName 'Bill' .
   *   GRAPH ?graph {
   *     ?person foaf:givenName 'Bill'
   *     }
   * };
   * INSERT { ?person foaf:givenName 'William' }
   * USING NAMED <http://example/addresses>
   * WHERE {
   *   ?person foaf:givenName 'Bill' .
   *   GRAPH ?graph {
   *     ?person foaf:givenName 'Bill'
   *     }
   * }
   * </pre>
   */
  @SuppressWarnings("rawtypes")
  public void test_delete_insert_30() throws MalformedQueryException,
          TokenMgrError, ParseException {

      final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
              + "DELETE { ?person foaf:givenName 'Bill' }\n"//
              + "WHERE {\n"//
              + "  ?person foaf:givenName 'Bill'. \n"//
              + "  GRAPH ?graph {\n"//
              + "    ?person foaf:givenName 'Bill'\n"//
              + "    }\n"//
              + "};"//
              + "INSERT { ?person foaf:givenName 'William' }\n"//
              + "USING NAMED <http://example/addresses>\n"//
              + "WHERE {\n"//
              + "  ?person foaf:givenName 'Bill'. \n"//
              + "  GRAPH ?graph {\n"//
              + "    ?person foaf:givenName 'Bill'\n"//
              + "    }\n"//
              + "}";
      
      final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
      final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
      final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
      final IV label2 = makeIV(valueFactory.createLiteral("William"));

      final UpdateRoot expected = new UpdateRoot();
      {
          
            {

                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData deleteClause = new QuadData();

                    deleteClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

            }

            {
                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData insertClause = new QuadData();

                    insertClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label2), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

                final Set<IV> namedGraphs = new LinkedHashSet<IV>();
                namedGraphs.add(addresses);

                @SuppressWarnings("unchecked")
                final DatasetNode dataset = new DatasetNode(
                        (Set) Collections.emptySet(),// defaultGraph
                        namedGraphs,//
                        true // update
                );

                op.setDataset(dataset);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A sequences of DELETE/INSERT operations with different data sets and no
     * WITH clause.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { ?person foaf:givenName 'Bill' }
     * USING NAMED <http://example/addresses>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * };
     * INSERT { ?person foaf:givenName 'William' }
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     * 
     * Note: This is the same operations, but the first one has the data set
     * while the second one does not.
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_31() throws MalformedQueryException,
            TokenMgrError, ParseException {
        
        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "USING NAMED <http://example/addresses>\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH ?graph {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "};"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE {\n"//
                + "  ?person foaf:givenName 'Bill'. \n"//
                + "  GRAPH ?graph {\n"//
                + "    ?person foaf:givenName 'Bill'\n"//
                + "    }\n"//
                + "}";

        final IV addresses = makeIV(valueFactory
                .createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory
                .createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));

        final UpdateRoot expected = new UpdateRoot();
        {

            {

                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData deleteClause = new QuadData();

                    deleteClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

                final Set<IV> namedGraphs = new LinkedHashSet<IV>();
                namedGraphs.add(addresses);

                @SuppressWarnings("unchecked")
                final DatasetNode dataset = new DatasetNode(
                        (Set) Collections.emptySet(),// defaultGraph
                        namedGraphs,//
                        true // update
                );

                op.setDataset(dataset);

            }

            {
                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData insertClause = new QuadData();

                    insertClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label2), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A sequences of DELETE/INSERT operations with different data sets and no
     * WITH clause. Test ensures, that named graph from dataset definition
     * of the first clause does not get leaked into the second clause dataset definition.
     * 
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * DELETE { ?person foaf:givenName 'Bill' }
     * USING NAMED <http://example/addresses>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * };
     * INSERT { ?person foaf:givenName 'William' }
     * USING NAMED <http://example/addresses2>
     * WHERE {
     *   ?person foaf:givenName 'Bill' .
     *   GRAPH ?graph {
     *     ?person foaf:givenName 'Bill'
     *     }
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_datasets_isolation() throws MalformedQueryException,
          TokenMgrError, ParseException {

      final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
              + "DELETE { ?person foaf:givenName 'Bill' }\n"//
              + "USING NAMED <http://example/addresses>\n"//
              + "WHERE {\n"//
              + "  ?person foaf:givenName 'Bill'. \n"//
              + "  GRAPH ?graph {\n"//
              + "    ?person foaf:givenName 'Bill'\n"//
              + "    }\n"//
              + "};"//
              + "INSERT { ?person foaf:givenName 'William' }\n"//
              + "USING NAMED <http://example/addresses2>\n"//
              + "WHERE {\n"//
              + "  ?person foaf:givenName 'Bill'. \n"//
              + "  GRAPH ?graph {\n"//
              + "    ?person foaf:givenName 'Bill'\n"//
              + "    }\n"//
              + "}";
      
      final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
      final IV addresses2 = makeIV(valueFactory.createURI("http://example/addresses2"));
      final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
      final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
      final IV label2 = makeIV(valueFactory.createLiteral("William"));

      final UpdateRoot expected = new UpdateRoot();
      {
          
            {

                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData deleteClause = new QuadData();

                    deleteClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setDeleteClause(new QuadsDataOrNamedSolutionSet(deleteClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

                final Set<IV> namedGraphs = new LinkedHashSet<IV>();
                namedGraphs.add(addresses);

                @SuppressWarnings("unchecked")
                final DatasetNode dataset = new DatasetNode(
                        (Set) Collections.emptySet(),// defaultGraph
                        namedGraphs,//
                        true // update
                );

                op.setDataset(dataset);
            }

            {
                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData insertClause = new QuadData();

                    insertClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label2), null,
                            Scope.DEFAULT_CONTEXTS));

                    op.setInsertClause(new QuadsDataOrNamedSolutionSet(insertClause));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();

                    op.setWhereClause(whereClause);

                    // Outside the GRAPH group.
                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "person"), new ConstantNode(givenName),
                            new ConstantNode(label1), null,
                            Scope.DEFAULT_CONTEXTS));

                    // The GRAPH group.
                    whereClause.addChild(new JoinGroupNode(
                            new VarNode("graph"), new StatementPatternNode(
                                    new VarNode("person"), new ConstantNode(
                                            givenName),
                                    new ConstantNode(label1), new VarNode(
                                            "graph"), Scope.NAMED_CONTEXTS)));

                }

                final Set<IV> namedGraphs = new LinkedHashSet<IV>();
                namedGraphs.add(addresses2);

                @SuppressWarnings("unchecked")
                final DatasetNode dataset = new DatasetNode(
                        (Set) Collections.emptySet(),// defaultGraph
                        namedGraphs,//
                        true // update
                );

                op.setDataset(dataset);
            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

   /**
     * <pre>
     * INSERT {
     * 
     *     GRAPH <http://example/out> { ?s ?p ?v . }
     * 
     * }
     * WHERE {
     * 
     *     SELECT ?s ?p ?v
     *     WHERE {
     * 
     *         GRAPH <http://example/in> { ?s ?p ?v . }
     * 
     *     }
     * 
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/562" >
     *      Sub-select in INSERT cause NPE in UpdateExprBuilder </a>
     */
    public void test_ticket_562() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "INSERT {\n" + //
                "  GRAPH <http://example/out> { ?s ?p ?v . }\n" + //
                "}\n" + //
                "WHERE {\n" + //
                "  SELECT ?s ?p ?v\n" + //
                "  WHERE {\n" + //
                "    GRAPH <http://example/in> { ?s ?p ?v . }\n" + //
                "    }\n" + //
                "}";

        @SuppressWarnings("rawtypes")
        final IV out = makeIV(valueFactory.createURI("http://example/out"));

        @SuppressWarnings("rawtypes")
        final IV in = makeIV(valueFactory.createURI("http://example/in"));

        final UpdateRoot expected = new UpdateRoot();
        {

            {

                final DeleteInsertGraph op = new DeleteInsertGraph();

                expected.addChild(op);

                {

                    final QuadData insertClause = new QuadData();

                    op.setInsertClause(new QuadsDataOrNamedSolutionSet(
                            insertClause));

                    final QuadData innerGroup = new QuadData();
                    insertClause.addChild(innerGroup);
                    
                    innerGroup.addChild(new StatementPatternNode(new VarNode(
                            "s"), new VarNode("p"), new VarNode("v"),
                            new ConstantNode(out), Scope.NAMED_CONTEXTS));

                }

                final SubqueryRoot subquery;
                
                // Top-level WHERE clause.
                {

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    op.setWhereClause(whereClause);

                    subquery = new SubqueryRoot(QueryType.SELECT);
                    whereClause.addChild(subquery);

                }
                
                // Subquery
                {
                    
                    final ProjectionNode projection = new ProjectionNode();
                    projection.addProjectionVar(new VarNode("s"));
                    projection.addProjectionVar(new VarNode("p"));
                    projection.addProjectionVar(new VarNode("v"));
                    subquery.setProjection(projection);
                    
                    final JoinGroupNode whereClause = new JoinGroupNode();
                    subquery.setWhereClause(whereClause);

                    final JoinGroupNode graphClause = new JoinGroupNode();
                    graphClause.setContext(new ConstantNode(in));
                    whereClause.addChild(graphClause);
                    
                    graphClause.addChild(new StatementPatternNode(new VarNode(
                            "s"), new VarNode("p"), new VarNode("v"),
                            new ConstantNode(in), Scope.NAMED_CONTEXTS));

                }

//                final Set<IV> namedGraphs = new LinkedHashSet<IV>();
//                namedGraphs.add(addresses);
//
//                @SuppressWarnings({ "unchecked", "rawtypes" })
//                final DatasetNode dataset = new DatasetNode(
//                        (Set) Collections.emptySet(),// defaultGraph
//                        (Set) Collections.emptySet(),// namedGraphs
//                        true // update
//                );
//
//                op.setDataset(dataset);

            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
