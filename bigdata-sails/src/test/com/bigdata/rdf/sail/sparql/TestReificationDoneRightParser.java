/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for the proposed standardization of "reification done right".
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/526 (Reification done
 *      right)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestGroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 * 
 *          TODO When triple pattern reference is nested within a GRAPH ?g {}
 *          clause (verify that the graph context is inherited correctly).
 * 
 *          TODO Test recursive embedded at depth GT 1.
 */
public class TestReificationDoneRightParser extends
        AbstractBigdataExprBuilderTestCase {

	private static final Logger log = Logger
			.getLogger(TestReificationDoneRightParser.class);
	
    /**
     * 
     */
    public TestReificationDoneRightParser() {
    }

    /**
     * @param name
     */
    public TestReificationDoneRightParser(String name) {
        super(name);
    }

    
    /**
	 * Unit test for a triple reference pattern using an explicit BIND().
	 * 
	 * <pre>
	 * prefix : <http://example.com/>
	 * prefix dc: <http://purl.org/dc/elements/1.1/>
	 * SELECT ?src ?who {
	 *    BIND( <<?who :bought :sybase>> AS ?sid ) .
	 *    ?sid dc:source ?src .
	 * }
	 * </pre>
	 * 
	 * is translated as
	 * 
	 * <pre>
	 * SP( ?who, :bought, :sybase) AS ?sid .
	 * SP( ?sid, dc:source, ?src )
	 * </pre>
	 */
	public void test_triple_ref_pattern_var_const_const()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
				= "prefix : <http://example.com/>\n" //
				+ "prefix dc: <http://purl.org/dc/elements/1.1/>\n" //
				+ "select ?src ?who {\n"//
				+ "  BIND( <<?who :bought :sybase>> AS ?sid ) . \n"//
				+ "  ?sid dc:source ?src .\n"//
				+ "}";

		final QueryRoot expected = new QueryRoot(QueryType.SELECT);
		{

			final IV<?, ?> dcSource = makeIV(valueFactory
					.createURI("http://purl.org/dc/elements/1.1/source"));

			final IV<?, ?> bought = makeIV(valueFactory
					.createURI("http://example.com/bought"));

			final IV<?, ?> sybase = makeIV(valueFactory
					.createURI("http://example.com/sybase"));

			{
            final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//				final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
				expected.setPrefixDecls(prefixDecls);
				prefixDecls.put("", "http://example.com/");
				prefixDecls.put("dc", "http://purl.org/dc/elements/1.1/");
			}

			final ProjectionNode projection = new ProjectionNode();
			projection.addProjectionVar(new VarNode("src"));
			projection.addProjectionVar(new VarNode("who"));
			expected.setProjection(projection);

			final JoinGroupNode whereClause = new JoinGroupNode();
			expected.setWhereClause(whereClause);

			final StatementPatternNode sp1 = new StatementPatternNode(//
					new VarNode("who"),//
					new ConstantNode(bought),//
					new ConstantNode(sybase),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			final VarNode sid1 = new VarNode("sid");
//			sid1.setSID(true);
			sp1.setSid(sid1);
			
			final StatementPatternNode sp2 = new StatementPatternNode(//
					new VarNode("sid"),//
					new ConstantNode(dcSource),//
					new VarNode("src"),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);

			whereClause.addChild(sp1);

			whereClause.addChild(sp2);

		}

		final QueryRoot actual = parse(sparql, baseURI);

		assertSameAST(sparql, expected, actual);

	}

	/**
	 * Unit test verifies that blank nodes may not appear in a triple pattern
	 * reference (the semantics of this are explicitly ruled out by the
	 * extension).
	 */
	public void test_triple_ref_pattern_blankNodesAreNotAllowed_subjectPosition()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
			    = "prefix : <http://example.com/>\n" //
				+ "prefix dc: <http://purl.org/dc/elements/1.1/>\n" //
				+ "select ?src ?who {\n"//
				+ "  BIND( <<_:a :bought :sybase>> AS ?sid ) . \n"//
				+ "  ?sid dc:source ?src .\n"//
				+ "}";

		try {
			parse(sparql, baseURI);
			fail("This construction is not legal.");
		} catch (MalformedQueryException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * Unit test verifies that blank nodes may not appear in a triple pattern
	 * reference (the semantics of this are explicitly ruled out by the
	 * extension).
	 */
	public void test_triple_ref_pattern_blankNodesAreNotAllowed_objectPosition()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
			    = "prefix : <http://example.com/>\n" //
				+ "prefix dc: <http://purl.org/dc/elements/1.1/>\n" //
				+ "select ?src ?who {\n"//
				+ "  BIND( <<:SAP :bought _:c>> AS ?sid ) . \n"//
				+ "  ?sid dc:source ?src .\n"//
				+ "}";

		try {
			parse(sparql, baseURI);
			fail("This construction is not legal.");
		} catch (MalformedQueryException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

    /**
	 * Unit test for a triple reference pattern without variables. There is an
	 * implicit variable which must be assigned when we translate this
	 * construct.
	 * 
	 * <pre>
	 * prefix : <http://example.com/>
	 * prefix dc: <http://purl.org/dc/elements/1.1/>
	 * SELECT ?src ?who {
	 *    <<:SAP :bought :sybase>> dc:source ?src
	 * }
	 * </pre>
	 * 
	 * The triple reference pattern must be translated as if it had been
	 * expressed with an explicit variable using BIND(). The equivalent
	 * expression for that triple reference pattern is:
	 * 
	 * <pre>
	 * SP(:SAP :bought :sybase) as ?sid) .
	 * SP(?sid dc:source ?src) .
	 * </pre>
	 * 
	 * The BIND() of the triple reference pattern is itself translated into a
	 * {@link StatementPatternNode} where the <code>?sid</code> variable appears
	 * in the SID role of that {@link StatementPatternNode}.
	 */
    public void test_triple_ref_pattern_no_vars()
            throws MalformedQueryException, TokenMgrError, ParseException {
    
		final String sparql = //
				  "prefix : <http://example.com/>\n" //
				+ "prefix dc: <http://purl.org/dc/elements/1.1/>\n" //
				+ "select ?src ?who {\n"//
				+ "  <<:SAP :bought :sybase>> dc:source ?src \n"//
				+ "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

			final IV<?, ?> dcSource = makeIV(valueFactory
					.createURI("http://purl.org/dc/elements/1.1/source"));

			final IV<?, ?> SAP = makeIV(valueFactory
					.createURI("http://example.com/SAP"));

			final IV<?, ?> bought = makeIV(valueFactory
					.createURI("http://example.com/bought"));

			final IV<?, ?> sybase = makeIV(valueFactory
					.createURI("http://example.com/sybase"));

            {
               final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
             prefixDecls.put("", "http://example.com/");
               expected.setPrefixDecls(prefixDecls);
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
//                expected.setPrefixDecls(prefixDecls);
//                prefixDecls.put("", "http://example.com/");
//                prefixDecls.put("dc", "http://purl.org/dc/elements/1.1/");
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("src"));
            projection.addProjectionVar(new VarNode("who"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

			// SP(:SAP :bought :sybase) as ?sid1) .
            final StatementPatternNode sp1 = new StatementPatternNode(//
					new ConstantNode(SAP),//
					new ConstantNode(bought),//
					new ConstantNode(sybase),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
            final VarNode sid1 = new VarNode("-sid-1");
//            sid1.setSID(true);
            sp1.setSid(sid1);

			// SP(?sid1 dc:source ?src) .
            final StatementPatternNode sp2 = new StatementPatternNode(//
					sid1,//
					new ConstantNode(dcSource),//
					new VarNode("src"),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS); 

			whereClause.addChild(sp1);
			whereClause.addChild(sp2);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

	/**
	 * A unit test when the triple reference pattern recursively embeds another
	 * triple reference pattern in the subject position. The nesting needs to be
	 * unpacked into a series of SP( <<...>> AS ?sidVar) expressions.
	 * 
	 * <pre>
	 * prefix : <http://example.com/>
	 * SELECT ?a ?e {
	 *    BIND( << <<?a :b :c>> :d ?e >> as ?sid ) 
	 * }
	 * </pre>
	 * 
	 * Should be translated as :
	 * 
	 * <pre>
	 * SP(?a, :b, :c) as ?sid1 .
	 * SP(?sid1, :d, :e) as ?sid .
	 * </pre>
	 */
	public void test_triple_ref_pattern_nested_in_subject_position()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
				= "prefix : <http://example.com/>\n" //
				+ "select ?a ?e {\n"//
				+ " BIND( << <<?a :b :c>> :d ?e>> AS ?sid) \n"//
				+ "}";

		final QueryRoot expected = new QueryRoot(QueryType.SELECT);
		{
			
			final VarNode sid1 = new VarNode("-sid-1");
//			sid1.setSID(true);
			
			final VarNode sid2 = new VarNode("sid");
//			sid2.setSID(true);
			
			final IV<?, ?> b = makeIV(valueFactory
					.createURI("http://example.com/b"));

			final IV<?, ?> c = makeIV(valueFactory
					.createURI("http://example.com/c"));

			final IV<?, ?> d = makeIV(valueFactory
					.createURI("http://example.com/d"));

			{
            final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//				final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
				expected.setPrefixDecls(prefixDecls);
				prefixDecls.put("", "http://example.com/");
			}

			final ProjectionNode projection = new ProjectionNode();
			projection.addProjectionVar(new VarNode("a"));
			projection.addProjectionVar(new VarNode("e"));
			expected.setProjection(projection);

			final JoinGroupNode whereClause = new JoinGroupNode();
			expected.setWhereClause(whereClause);

			final StatementPatternNode sp1 = new StatementPatternNode(//
					new VarNode("a"),//
					new ConstantNode(b),//
					new ConstantNode(c),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			sp1.setSid(sid1);
			
			final StatementPatternNode sp2 = new StatementPatternNode(//
					sid1,//
					new ConstantNode(d),//
					new VarNode("e"),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			sp2.setSid(sid2);
			
			whereClause.addChild(sp1);
			
			whereClause.addChild(sp2);
			
		}

		final QueryRoot actual = parse(sparql, baseURI);

		assertSameAST(sparql, expected, actual);

	}

	/**
	 * A unit test when the triple reference pattern recursively embeds another
	 * triple reference pattern in the object position. The nesting needs to be
	 * unpacked into a series of SP(s,p,o) AS ?sidVar expressions.
	 * 
	 * <pre>
	 * prefix : <http://example.com/>
	 * SELECT ?a ?e {
	 *    BIND( << ?a :d <<?e :b :c>> >> as ?sid ) 
	 * }
	 * </pre>
	 * 
	 * Should be translated as :
	 * 
	 * <pre>
	 * SP(?e, :b, :c) as ?sid1 .
	 * SP(?a, :d, ?sid1) as ?sid .
	 * </pre>
	 */
	public void test_triple_ref_pattern_nested_in_object_position()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
				= "prefix : <http://example.com/>\n" //
				+ "select ?a ?e {\n"//
				+ " BIND( << ?a :d <<?e :b :c>> >> AS ?sid) \n"//
				+ "}";

		final QueryRoot expected = new QueryRoot(QueryType.SELECT);
		{
			
			final VarNode sid1 = new VarNode("-sid-1");
//			sid1.setSID(true);
			
			final VarNode sid2 = new VarNode("sid");
//			sid2.setSID(true);
			
			final IV<?, ?> b = makeIV(valueFactory
					.createURI("http://example.com/b"));

			final IV<?, ?> c = makeIV(valueFactory
					.createURI("http://example.com/c"));

			final IV<?, ?> d = makeIV(valueFactory
					.createURI("http://example.com/d"));

			{
            final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//				final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
				expected.setPrefixDecls(prefixDecls);
				prefixDecls.put("", "http://example.com/");
			}

			final ProjectionNode projection = new ProjectionNode();
			projection.addProjectionVar(new VarNode("a"));
			projection.addProjectionVar(new VarNode("e"));
			expected.setProjection(projection);

			final JoinGroupNode whereClause = new JoinGroupNode();
			expected.setWhereClause(whereClause);

			// SP(?e, :b, :c) as ?sid1 .
			final StatementPatternNode sp1 = new StatementPatternNode(//
					new VarNode("e"),//
					new ConstantNode(b),//
					new ConstantNode(c),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			sp1.setSid(sid1);
			
			// SP(?a, :d, ?sid1) as ?sid .
			final StatementPatternNode sp2 = new StatementPatternNode(//
					new VarNode("a"),//
					new ConstantNode(d),//
					sid1,//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			sp2.setSid(sid2);
			
			whereClause.addChild(sp1);
			
			whereClause.addChild(sp2);
			
		}

		final QueryRoot actual = parse(sparql, baseURI);

		assertSameAST(sparql, expected, actual);

	}

	/**
	 * A unit test when the triple reference pattern is a constant.
	 * 
	 * <pre>
	 * prefix : <http://example.com/>
	 * SELECT ?a {
	 *    BIND( <<:e :b :c>> >> as ?sid ) .
	 *    ?a :d ?sid . 
	 * }
	 * </pre>
	 * 
	 * Should be translated as :
	 * 
	 * <pre>
	 * SP(:e, :b, :c) as ?sid .
	 * SP(?a, :d, ?sid).
	 * </pre>
	 * 
	 * Note that the SP for the fully bound triple pattern will enforce the
	 * semantics that the triple must exist in the data in order for the query
	 * to succeed.
	 */
	public void test_triple_ref_pattern_is_constant()
			throws MalformedQueryException, TokenMgrError, ParseException {

		final String sparql //
				= "prefix : <http://example.com/>\n" //
				+ "select ?a {\n"//
				+ "  BIND( <<:e :b :c>> AS ?sid) .\n"//
				+ "  ?a :d ?sid.\n"
				+ "}";

		final QueryRoot expected = new QueryRoot(QueryType.SELECT);
		{
			
			final VarNode sid = new VarNode("sid");
//			sid.setSID(true);
			
			final IV<?, ?> b = makeIV(valueFactory
					.createURI("http://example.com/b"));

			final IV<?, ?> c = makeIV(valueFactory
					.createURI("http://example.com/c"));

			final IV<?, ?> d = makeIV(valueFactory
					.createURI("http://example.com/d"));

			final IV<?, ?> e = makeIV(valueFactory
					.createURI("http://example.com/e"));

			{
            final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//				final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
				expected.setPrefixDecls(prefixDecls);
				prefixDecls.put("", "http://example.com/");
			}

			final ProjectionNode projection = new ProjectionNode();
			projection.addProjectionVar(new VarNode("a"));
			expected.setProjection(projection);

			final JoinGroupNode whereClause = new JoinGroupNode();
			expected.setWhereClause(whereClause);

			// SP(:e, :b, :c) as ?sid .
			final StatementPatternNode sp1 = new StatementPatternNode(//
					new ConstantNode(e),//
					new ConstantNode(b),//
					new ConstantNode(c),//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			sp1.setSid(sid);
			
			// SP(?a, :d, ?sid) as ?-sid-1.
			final StatementPatternNode sp2 = new StatementPatternNode(//
					new VarNode("a"),//
					new ConstantNode(d),//
					sid,//
					null/* c */,//
					Scope.DEFAULT_CONTEXTS);
			
			whereClause.addChild(sp1);
			
			whereClause.addChild(sp2);
			
		}

		final QueryRoot actual = parse(sparql, baseURI);

		assertSameAST(sparql, expected, actual);

	}

    /**
     * A unit test when the triple reference pattern is a constant.
     * 
     * <pre>
     * prefix : <http://example.com/>
     * SELECT ?a {
     *    ?d ?e <<?a ?b ?c>> . 
     * }
     * </pre>
     * 
     * Should be translated as :
     * 
     * <pre>
     * SP(?a, ?b, ?c) as ?sid-1 .
     * SP(?d, ?e, ?-sid-1).
     * </pre>
     * 
     * Note that the SP for the first bound triple pattern will enforce the
     * semantics that the triple must exist in the data in order for the query
     * to succeed.
     */
    public void test_triple_ref_pattern_all_vars()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql //
                = "prefix : <http://example.com/>\n" //
                + "select ?a {\n"//
                + "  ?d ?e <<?a ?b ?c>> .\n"
                + "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final VarNode a = new VarNode("a");
            final VarNode b = new VarNode("b");
            final VarNode c = new VarNode("c");
            final VarNode d = new VarNode("d");
            final VarNode e = new VarNode("e");
            final VarNode sid = new VarNode("-sid-1");

            {
               final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.com/");
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("a"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // SP(?a, ?b, ?c) as ?sid .
            final StatementPatternNode sp1 = new StatementPatternNode(//
                    a,//
                    b,//
                    c,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            sp1.setSid(sid);
            
            // SP(?d, ?e, ?sid).
            final StatementPatternNode sp2 = new StatementPatternNode(//
                    d,//
                    e,//
                    sid,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            
            whereClause.addChild(sp1);
            
            whereClause.addChild(sp2);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A unit test when the triple reference pattern is a constant.
     * 
     * <pre>
     * prefix : <http://example.com/>
     * SELECT ?a {
     *    <<?a ?b ?c>> ?d ?e . 
     * }
     * </pre>
     * 
     * Should be translated as :
     * 
     * <pre>
     * SP(?a, ?b, ?c) as ?sid-1 .
     * SP(?-sid-1, ?d, ?e).
     * </pre>
     * 
     * Note that the SP for the first bound triple pattern will enforce the
     * semantics that the triple must exist in the data in order for the query
     * to succeed.
     */
    public void test_triple_ref_pattern_all_vars2()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql //
                = "prefix : <http://example.com/>\n" //
                + "select ?a {\n"//
                + "  <<?a ?b ?c>> ?d ?e .\n"
                + "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final VarNode a = new VarNode("a");
            final VarNode b = new VarNode("b");
            final VarNode c = new VarNode("c");
            final VarNode d = new VarNode("d");
            final VarNode e = new VarNode("e");
            final VarNode sid = new VarNode("-sid-1");

            {
               final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.com/");
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("a"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // SP(?a, ?b, ?c) as ?sid .
            final StatementPatternNode sp1 = new StatementPatternNode(//
                    a,//
                    b,//
                    c,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            sp1.setSid(sid);
            
            // SP(?sid, ?d, ?e).
            final StatementPatternNode sp2 = new StatementPatternNode(//
                    sid,//
                    d,//
                    e,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            
            whereClause.addChild(sp1);
            
            whereClause.addChild(sp2);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A unit test when the triple reference pattern is a constant.
     * 
     * <pre>
     * prefix : <http://example.com/>
     * SELECT ?a {
     *    BIND( <<?a ?b ?c>> as ?sid ) .
     *    ?d ?e ?sid . 
     * }
     * </pre>
     * 
     * Should be translated as :
     * 
     * <pre>
     * SP(?a, ?b, ?c) as ?sid .
     * SP(?d, ?e, ?sid).
     * </pre>
     * 
     * Note that the SP for the first bound triple pattern will enforce the
     * semantics that the triple must exist in the data in order for the query
     * to succeed.
     */
    public void test_triple_ref_pattern_all_vars_with_explicit_bind()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql //
                = "prefix : <http://example.com/>\n" //
                + "select ?a {\n"//
                + "  BIND( <<?a ?b ?c>> AS ?sid) .\n"//
                + "  ?d ?e ?sid.\n"
                + "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final VarNode a = new VarNode("a");
            final VarNode b = new VarNode("b");
            final VarNode c = new VarNode("c");
            final VarNode d = new VarNode("d");
            final VarNode e = new VarNode("e");
            final VarNode sid = new VarNode("sid");

            {
               final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>();
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.com/");
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("a"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // SP(?a, ?b, ?c) as ?sid .
            final StatementPatternNode sp1 = new StatementPatternNode(//
                    a,//
                    b,//
                    c,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            sp1.setSid(sid);
            
            // SP(?d, ?e, ?sid).
            final StatementPatternNode sp2 = new StatementPatternNode(//
                    d,//
                    e,//
                    sid,//
                    null/* c */,//
                    Scope.DEFAULT_CONTEXTS);
            
            whereClause.addChild(sp1);
            
            whereClause.addChild(sp2);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
