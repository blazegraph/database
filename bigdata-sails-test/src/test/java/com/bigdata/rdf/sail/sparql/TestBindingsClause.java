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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.util.InnerCause;

/**
 * Test suite for the BINDINGS clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBigdataExprBuilder.java 5073 2011-08-23 00:33:54Z
 *          thompsonbry $
 */
public class TestBindingsClause extends AbstractBigdataExprBuilderTestCase {

    private static final Logger log = Logger
            .getLogger(TestBindingsClause.class);
    
    public TestBindingsClause() {
    }

    public TestBindingsClause(String name) {
        super(name);
    }

    /**
     * Unit test for the SPARQL 1.1 BINDINGS clause with one binding set having
     * one binding.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book {
     *  (:book1)
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_bindings_001() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book {\n" + //
                "  (:book1)\n" + //
                "}"//
                ;

        final IV<?,?> title = makeIV(valueFactory.createURI("http://example.org/book/title"));
        final IV<?,?> price = makeIV(valueFactory.createURI("http://example.org/book/price"));
        final IV<?,?> book1 = makeIV(valueFactory.createURI("http://example.org/book/book1"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("", "http://example.org/book/");
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("title"));
            projection.addProjectionVar(new VarNode("price"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(title)),
                    new VarNode("title"), null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(price)),
                    new VarNode("price"), null/* c */, Scope.DEFAULT_CONTEXTS));

            final LinkedHashSet<IVariable<?>> declaredVars = new LinkedHashSet<IVariable<?>>();
            declaredVars.add(Var.var("book"));
            
            final List<IBindingSet> bindings = new LinkedList<IBindingSet>();
            {
                final IBindingSet bset = new ListBindingSet();
                bset.set(Var.var("book"), new Constant<IV>(book1));
                bindings.add(bset);
            }
            
            final BindingsClause bindingsClause = new BindingsClause(
                    declaredVars, bindings);
            
            expected.setBindingsClause(bindingsClause);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the SPARQL 1.1 BINDINGS clause with an UNDEF binding.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book {
     *  (UNDEF)
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_bindings_002() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book {\n" + //
                "  (UNDEF)\n" + //
                "}"//
                ;

        final IV<?,?> title = makeIV(valueFactory.createURI("http://example.org/book/title"));
        final IV<?,?> price = makeIV(valueFactory.createURI("http://example.org/book/price"));
//        final IV<?,?> book1 = makeIV(valueFactory.createURI("http://example.org/book/book1"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("", "http://example.org/book/");
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("title"));
            projection.addProjectionVar(new VarNode("price"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(title)),
                    new VarNode("title"), null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(price)),
                    new VarNode("price"), null/* c */, Scope.DEFAULT_CONTEXTS));

            final LinkedHashSet<IVariable<?>> declaredVars = new LinkedHashSet<IVariable<?>>();
            declaredVars.add(Var.var("book"));
            
            final List<IBindingSet> bindings = new LinkedList<IBindingSet>();
            {
                final IBindingSet bset = new ListBindingSet();
//                bset.set(Var.var("book"), new Constant<IV>(book1));
                bindings.add(bset);
            }
            
            final BindingsClause bindingsClause = new BindingsClause(
                    declaredVars, bindings);
            
            expected.setBindingsClause(bindingsClause);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the SPARQL 1.1 BINDINGS clause with two binding sets, one
     * of which has no bound values.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book {
     *  (:book1)
     *  (UNDEF)
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_bindings_003() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book {\n" + //
                "  (:book1)\n" + //
                "  (UNDEF)\n" + //
                "}"//
                ;

        final IV<?,?> title = makeIV(valueFactory.createURI("http://example.org/book/title"));
        final IV<?,?> price = makeIV(valueFactory.createURI("http://example.org/book/price"));
        final IV<?,?> book1 = makeIV(valueFactory.createURI("http://example.org/book/book1"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("", "http://example.org/book/");
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("title"));
            projection.addProjectionVar(new VarNode("price"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(title)),
                    new VarNode("title"), null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(price)),
                    new VarNode("price"), null/* c */, Scope.DEFAULT_CONTEXTS));

            final LinkedHashSet<IVariable<?>> declaredVars = new LinkedHashSet<IVariable<?>>();
            declaredVars.add(Var.var("book"));
            
            final List<IBindingSet> bindings = new LinkedList<IBindingSet>();
            {
                final IBindingSet bset = new ListBindingSet();
                bset.set(Var.var("book"), new Constant<IV>(book1));
                bindings.add(bset);
            }
            {
                final IBindingSet bset = new ListBindingSet();
                bindings.add(bset);
            }
            
            final BindingsClause bindingsClause = new BindingsClause(
                    declaredVars, bindings);
            
            expected.setBindingsClause(bindingsClause);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the SPARQL 1.1 BINDINGS clause with two binding sets, one
     * of which does not bind all variables.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book ?title {
     *  (:book1 :title1)
     *  (:book2   UNDEF)
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_bindings_004() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book ?title {\n" + //
                "  (:book1 :title1)\n" + //
                "  (:book2   UNDEF)\n" + //
                "}"//
                ;

        final IV<?,?> title = makeIV(valueFactory.createURI("http://example.org/book/title"));
        final IV<?,?> price = makeIV(valueFactory.createURI("http://example.org/book/price"));
        final IV<?,?> book1 = makeIV(valueFactory.createURI("http://example.org/book/book1"));
        final IV<?,?> book2 = makeIV(valueFactory.createURI("http://example.org/book/book2"));
        final IV<?,?> title1 = makeIV(valueFactory.createURI("http://example.org/book/title1"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("", "http://example.org/book/");
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("title"));
            projection.addProjectionVar(new VarNode("price"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(title)),
                    new VarNode("title"), null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(price)),
                    new VarNode("price"), null/* c */, Scope.DEFAULT_CONTEXTS));

            final LinkedHashSet<IVariable<?>> declaredVars = new LinkedHashSet<IVariable<?>>();
            declaredVars.add(Var.var("book"));
            declaredVars.add(Var.var("title"));
            
            final List<IBindingSet> bindings = new LinkedList<IBindingSet>();
            {
                final IBindingSet bset = new ListBindingSet();
                bset.set(Var.var("book"), new Constant<IV>(book1));
                bset.set(Var.var("title"), new Constant<IV>(title1));
                bindings.add(bset);
            }
            {
                final IBindingSet bset = new ListBindingSet();
                bset.set(Var.var("book"), new Constant<IV>(book2));
//                bset.set(Var.var("title"), new Constant<IV>(title1));
                bindings.add(bset);
            }
            
            final BindingsClause bindingsClause = new BindingsClause(
                    declaredVars, bindings);
            
            expected.setBindingsClause(bindingsClause);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the SPARQL 1.1 BINDINGS clause with NO variables and NO
     * binding sets (the spec allows this).
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS {
     * }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_bindings_005() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS {\n" + //
                "}"//
                ;

        final IV<?,?> title = makeIV(valueFactory.createURI("http://example.org/book/title"));
        final IV<?,?> price = makeIV(valueFactory.createURI("http://example.org/book/price"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("", "http://example.org/book/");
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("title"));
            projection.addProjectionVar(new VarNode("price"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(title)),
                    new VarNode("title"), null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(new Constant<IV>(price)),
                    new VarNode("price"), null/* c */, Scope.DEFAULT_CONTEXTS));

            /*
             * Note: The BINDINGS clause is omitted from the AST by the visitor
             * when there are no binding sets.
             */
//            final LinkedHashSet<IVariable<?>> declaredVars = new LinkedHashSet<IVariable<?>>();
//            
//            final List<IBindingSet> bindings = new LinkedList<IBindingSet>();
//            
//            final BindingsClause bindingsClause = new BindingsClause(
//                    declaredVars, bindings);
//            
//            expected.setBindingsClause(bindingsClause);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Correct rejection test for the SPARQL 1.1 BINDINGS clause with no
     * variables and a non-empty binding set.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS {
     *  (:book1)
     * }
     * </pre>
     */
    public void test_bindings_006() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS {\n" + //
                "  (:book1)\n" + //
                "}"//
                ;

        try {
            parse(sparql, baseURI);
            fail("Expecting: " + VisitorException.class);
        } catch (Throwable t) {
            if (InnerCause.isInnerCause(t, VisitorException.class)) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + t, t);
                return;
            }
            fail("Expecting: " + VisitorException.class);
        }

    }

    /**
     * Correct rejection test for the SPARQL 1.1 BINDINGS clause with duplicate
     * variables and a non-empty set of binding sets.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book ?book {
     *  (:book1 :book2)
     * }
     * </pre>
     */
    public void test_bindings_007() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book ?book {\n" + //
                "  (:book1 :book2)\n" + //
                "}"//
                ;

        try {
            parse(sparql, baseURI);
            fail("Expecting: " + VisitorException.class);
        } catch (Throwable t) {
            if (InnerCause.isInnerCause(t, VisitorException.class)) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + t, t);
                return;
            }
            fail("Expecting: " + VisitorException.class);
        }

    }

    /**
     * Correct rejection test for the SPARQL 1.1 BINDINGS clause with duplicate
     * variables and an empty set of binding sets.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book ?book {
     * }
     * </pre>
     */
    public void test_bindings_008() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book ?book {\n" + //
                "}"//
                ;

        try {
            parse(sparql, baseURI);
            fail("Expecting: " + VisitorException.class);
        } catch (Throwable t) {
            if (InnerCause.isInnerCause(t, VisitorException.class)) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + t, t);
                return;
            }
            fail("Expecting: " + VisitorException.class);
        }

    }

    /**
     * Correct rejection test for the SPARQL 1.1 BINDINGS clause with too many
     * bindings in one binding set.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book {
     *   (:book1)
     *   (:book1 :book2)
     * }
     * </pre>
     */
    public void test_bindings_009() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book {\n" + //
                " (:book1) \n"+//
                " (:book1 :book2) \n"+//
                "}"//
                ;

        try {
            parse(sparql, baseURI);
            fail("Expecting: " + VisitorException.class);
        } catch (Throwable t) {
            if (InnerCause.isInnerCause(t, VisitorException.class)) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + t, t);
                return;
            }
            fail("Expecting: " + VisitorException.class);
        }

    }

    /**
     * Correct rejection test for the SPARQL 1.1 BINDINGS clause with too few
     * bindings in one binding set.
     * 
     * <pre>
     * PREFIX :     <http://example.org/book/> 
     * SELECT ?title ?price
     * {
     *    ?book :title ?title ;
     *          :price ?price .
     * }
     * BINDINGS ?book ?title {
     *   (:book1 :title1)
     *   (:book1)
     * }
     * </pre>
     */
    public void test_bindings_010() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX :     <http://example.org/book/>\n"+// 
                "SELECT ?title ?price\n" + //
                "{\n" + //
                "    ?book :title ?title ; \n" + //
                "          :price ?price . \n" + //
                "}\n" + //
                "BINDINGS ?book ?title {\n" + //
                " (:book1 :title1) \n"+//
                " (:book1) \n"+//
                "}"//
                ;

        try {
            parse(sparql, baseURI);
            fail("Expecting: " + VisitorException.class);
        } catch (Throwable t) {
            if (InnerCause.isInnerCause(t, VisitorException.class)) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + t, t);
                return;
            }
            fail("Expecting: " + VisitorException.class);
        }

    }

}
