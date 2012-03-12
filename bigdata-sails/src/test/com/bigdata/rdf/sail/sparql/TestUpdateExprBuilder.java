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

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
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
import com.bigdata.rdf.sparql.ast.QuadData;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;

/**
 * Test suite for {@link UpdateExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBigdataExprBuilder.java 5073 2011-08-23 00:33:54Z
 *          thompsonbry $
 * 
 *          TODO Are we going to attach prefix decls to the {@link Update}
 *          operations or the {@link UpdateRoot}? Or at all?
 * 
 *          TODO What about {@link DatasetNode} attachments?
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

            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data"))));

        }
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data1"))));

        }
        {

            final LoadGraph op = new LoadGraph();

            expected.addChild(op);

            op.setSourceGraph(new ConstantNode(makeIV(valueFactory
                    .createURI("http://www.bigdata.com/data2"))));

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

            final ISPO[] data = new ISPO[] { //
                    valueFactory.createStatement(
                    book1, dcTitle, label1, null, StatementEnum.Explicit),//
                    valueFactory.createStatement(
                    book1, dcCreator, label2, null, StatementEnum.Explicit),//
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

            final ISPO[] data = new ISPO[] { //
                    valueFactory.createStatement(book1, price, i42, bookstore,
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
     *   <http://example/book1> dc:creator "A.N.Other" .
     *   GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } 
     * }
     * </pre>
     */
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

            final ISPO[] data = new ISPO[] { //
                    valueFactory.createStatement(
                    book1, dcTitle, label1, null, StatementEnum.Explicit),//
                    valueFactory.createStatement(
                    book1, dcCreator, label2, null, StatementEnum.Explicit),//
                    valueFactory.createStatement(book1, price, i42, bookstore,
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
     *   GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } 
     *   <http://example/book1> dc:title "A new book" .
     *   <http://example/book1> dc:creator "A.N.Other" .
     * }
     * </pre>
     */
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

            final ISPO[] data = new ISPO[] { //
                    valueFactory.createStatement(book1, price, i42, bookstore,
                    StatementEnum.Explicit),//
                    valueFactory.createStatement(
                    book1, dcTitle, label1, null, StatementEnum.Explicit),//
                    valueFactory.createStatement(
                    book1, dcCreator, label2, null, StatementEnum.Explicit),//
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
    public void test_insert_data_triples_quads_triples() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA\n"
                + "{\n"
                + "  <http://example/book1> dc:title \"A new book\" .\n"
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

            final ISPO[] data = new ISPO[] { //
                    
                    valueFactory.createStatement(
                    book1, dcTitle, label1, null, StatementEnum.Explicit),//
                    
                    valueFactory.createStatement(book1, price, i42, bookstore,
                    StatementEnum.Explicit),//
                    
                    valueFactory.createStatement(
                    book1, dcCreator, label2, null, StatementEnum.Explicit),//
            
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
    public void test_delete_data_quads_insert_data_quads()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "DELETE DATA\n"
                + "{ GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  \"Fundamentals of Compiler Desing\" } } ;\n"
                + "\n"
                + "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"
                + "INSERT DATA\n"
                + "{ GRAPH <http://example/bookStore> { <http://example/book1>  dc:title  \"Fundamentals of Compiler Design\" } }";

        final BigdataURI book1 = valueFactory.createURI("http://example/book1");
        final BigdataURI dcTitle = valueFactory.createURI("http://purl.org/dc/elements/1.1/title");
        final BigdataLiteral label1 = valueFactory.createLiteral("Fundamentals of Compiler Desing");
        final BigdataLiteral label2 = valueFactory.createLiteral("Fundamentals of Compiler Design");
        final BigdataURI bookstore = valueFactory.createURI("http://example/bookStore");

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteData op = new DeleteData();

            expected.addChild(op);

            final ISPO[] data = new ISPO[] { //
                    
                    valueFactory.createStatement(
                    book1, dcTitle, label1, bookstore, StatementEnum.Explicit),//
                                
            };
            op.setData(data);

        }

        {

            final InsertData op = new InsertData();

            expected.addChild(op);

            final ISPO[] data = new ISPO[] { //
                    
                    valueFactory.createStatement(
                    book1, dcTitle, label2, bookstore, StatementEnum.Explicit),//
                                
            };
            op.setData(data);
            
        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     * 
     * WITH <http://example/addresses>
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * WHERE
     *   { ?person foaf:givenName 'Bill'
     *   }
     * </pre>
     * 
     * TODO What about the WITH? That should be setting the data set, right?
     * 
     * TODO Tests with USING and USING NAMED.
     */
    @SuppressWarnings("rawtypes")
    public void test_delete_insert_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n"
                + "WITH <http://example/addresses>\n"//
                + "DELETE { ?person foaf:givenName 'Bill' }\n"//
                + "INSERT { ?person foaf:givenName 'William' }\n"//
                + "WHERE\n"//
                + "  { ?person foaf:givenName 'Bill' } ";
        
        final IV addresses = makeIV(valueFactory.createURI("http://example/addresses"));
        final IV givenName = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/givenName"));
        final IV label1 = makeIV(valueFactory.createLiteral("Bill"));
        final IV label2 = makeIV(valueFactory.createLiteral("William"));
//        final VarNode person = new VarNode("person");

        final UpdateRoot expected = new UpdateRoot();
        {

            final DeleteInsertGraph op = new DeleteInsertGraph();

            expected.addChild(op);

            {

                final QuadData deleteClause = new QuadData();
                
                deleteClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1)));

                op.setDeleteClause(deleteClause);

            }

            {

                final QuadData insertClause = new QuadData();

                insertClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label2)));

                op.setInsertClause(insertClause);

            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "person"), new ConstantNode(givenName),
                        new ConstantNode(label1)));

                op.setWhereClause(whereClause);
                
            }

        }

        final UpdateRoot actual = parseUpdate(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
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
    public void test_delete_insert_02() throws MalformedQueryException,
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

                op.setDeleteClause(deleteClause);

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
    public void test_delete_insert_03() throws MalformedQueryException,
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
     * <pre>
     * DELETE WHERE {?x foaf:name ?y }
     * </pre>
     * 
     * TODO We may need to run an AST optimizer on this to produce the DELETE
     * clause.
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

}
