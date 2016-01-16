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
 * Created on August 31, 2015
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Iterator;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.explainhints.BottomUpSemanticsExplainHint;
import com.bigdata.rdf.sparql.ast.explainhints.ExplainHints;
import com.bigdata.rdf.sparql.ast.explainhints.IExplainHint;
import com.bigdata.rdf.sparql.ast.explainhints.JoinOrderExplainHint;
import com.bigdata.rdf.sparql.ast.explainhints.UnsatisfiableMinusExplainHint;

/**
 * Test suite for EXPLAIN hints.
 *
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestExplainHints extends AbstractDataDrivenSPARQLTestCase {

    public TestExplainHints() {
    }

    public TestExplainHints(String name) {
        super(name);
    }

    
    /**
     * Assert that the explain hint is attached for FILTER scope issues.
     *
     * SELECT ?s ?type WHERE {
     *   BIND(<http://example.com/Person> AS ?type)
     *   {
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     * }
     * 
     * @throws Exception
     */
    public void testBottomUpSemanticsExplainHint01() throws Exception {

    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup01",// name
			"explainHints-bottomup01.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup12345.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), BottomUpSemanticsExplainHint.class);
    }

    /**
     * Assert that the explain hint is attached for FILTER scope issues.
     *
     * SELECT ?s ?type WHERE {
     *   BIND(<http://example.com/Person> AS ?type) 
     *   {
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     *   UNION
     *   {
     *     ?s a ?o
     *     FILTER(?o!=?type)
     *   }
     * }
     * @throws Exception
     */
    public void testBottomUpSemanticsExplainHint02() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup02",// name
			"explainHints-bottomup02.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup12345.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), BottomUpSemanticsExplainHint.class);
    }
    
    /**
     * Assert that the explain hint is attached for BIND scope issues.
     *
     * SELECT ?s ?type WHERE {
     *   BIND("http://example.com/" AS ?typeBase)
     *   {
     *     BIND(?typeBase AS ?type)
     *     ?s a ?o
     *     FILTER(?o=?type)
     *    }
     *  }
     *  
     * @throws Exception
     */
    public void testBottomUpSemanticsExplainHint03() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup03",// name
			"explainHints-bottomup03.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup12345.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), BottomUpSemanticsExplainHint.class);
    }
    
    /**
     * Assert that the explain hint is attached for BIND scope issues.
     *
     * SELECT ?s ?type WHERE {
     *   BIND("http://example.com/" AS ?typeBase)
     *   {
     *     BIND(URI(CONCAT(?typeBase,"Person")) AS ?type)
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     *   UNION
     *   {
     *     BIND(URI(CONCAT(?typeBase,"Animal")) AS ?type)
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     * }
     * 
     * @throws Exception
     */
    public void testBottomUpSemanticsExplainHint04() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup04",// name
			"explainHints-bottomup04.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup12345.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), BottomUpSemanticsExplainHint.class);
    }
    
    /**
     * Assert that the explain hint is attached for VALUES scope issues.
     *
     * SELECT ?s ?type WHERE {
     *   {
     *     BIND(URI(CONCAT(?typeBase,"Person")) AS ?type)
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     *   UNION
     *   {
     *     BIND(URI(CONCAT(?typeBase,"Animal")) AS ?type)
     *     ?s a ?o
     *     FILTER(?o=?type)
     *   }
     * } VALUES (?typeBase) { ("http://example.com/") }
     * 
     * @throws Exception
     */
    public void testBottomUpSemanticsExplainHint05() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup05",// name
			"explainHints-bottomup05.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup12345.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), BottomUpSemanticsExplainHint.class);
    }
    
    /**
     * Assert that the explain hint is not attached, using an example with
     * BIND where no bottom-up semantics issues arise.
     * 
     * SELECT ?s ?type WHERE {
     *   BIND("http://example.com/" AS ?typeBase)
     *   BIND(URI(CONCAT(?typeBase,"Person")) AS ?type)
     *   ?s a ?o
     *   FILTER(?o=?type)
     * }
     * 
     * @throws Exception
     */    
    public void testBottomUpSemanticsExplainHint06() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-bottomup06",// name
			"explainHints-bottomup06.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-bottomup06.srx"// results URL
		).runTest();
    	
    	// there's no explain hint (no bottom up semantics issues)
    	final Iterator<BOp> explainHintAnnotatedBOps = 
    	   ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
    	assertFalse(explainHintAnnotatedBOps.hasNext());
    }
    
    
    /**
     * Assert that the explain hint is attached for non-reorderable join groups.
     *
     * SELECT * WHERE {
     *   OPTIONAL { ?person <http://example.com/image> ?image } .
     *   ?person rdf:type <http://example.com/Person>
     * }
     * 
     * @throws Exception
     */
    public void testJoinGroupOrderExplainHint01() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-joingrouporder01",// name
			"explainHints-joingrouporder01.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-joingrouporder01.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
        	container.getOptimizedAST(), JoinOrderExplainHint.class);
    }

    /**
     * Assert that the explain hint is not attached for reorderable join groups.
     * 
     * SELECT * WHERE {
     *   ?person rdf:type <http://example.com/Person>
     *   OPTIONAL { ?person <http://example.com/image> ?image } .
     *   ?person <http://example.com/name> ?name
     * }
     * 
     * @throws Exception
     */
    public void testJoinGroupOrderExplainHint02() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-joingrouporder02",// name
			"explainHints-joingrouporder02.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-joingrouporder02.srx"// results URL
		).runTest();
    	
    	// there's no explain hint, reordering works fine here
    	final Iterator<BOp> explainHintAnnotatedBOps = 
    	   ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
    	assertFalse(explainHintAnnotatedBOps.hasNext());
    }

    
    /**
     * Assert that the explain hint is attached for unsatisfiable MINUS:
     * 
     * SELECT * WHERE { ?s ?p ?o  MINUS { ?a ?b ?c } } 
     * 
     * @throws Exception
     */
    public void testUnsatisfiableMinusExplainHint01() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-unsatisfiableminus01",// name
			"explainHints-unsatisfiableminus01.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-unsatisfiableminus01.srx"// results URL
		).runTest();
    	
    	assertCarriesExactlyOneExplainHintOfType(
    		container.getOptimizedAST(), UnsatisfiableMinusExplainHint.class);
    			 
    }
    
    /**
     * Assert that the explain hint is not attached for satisfiable MINUS:
     * 
     * SELECT * WHERE { ?s ?p ?o  MINUS { ?s ?b ?c } } 
     * 
     * @throws Exception
     */
    public void testUnsatisfiableMinusExplainHint02() throws Exception {
    	
    	final ASTContainer container = new TestHelper(
			"explainHints-unsatisfiableminus02",// name
			"explainHints-unsatisfiableminus02.rq",// query URL
			"explainHints.trig",// data URL
			"explainHints-unsatisfiableminus02.srx"// results URL
		).runTest();

    	// there's no explain hint, the MINUS here is satisfiable
    	final Iterator<BOp> explainHintAnnotatedBOps = 
    	   ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
    	assertFalse(explainHintAnnotatedBOps.hasNext());
    }
    
    /**
     * Explain hint rendering & bottom up rewriting broken for subqueries,
     * see https://jira.blazegraph.com/browse/BLZG-1463.
     * 
     * @throws Exception
     */
    public void testTicketBlzg463a() throws Exception {
       
       final ASTContainer container = new TestHelper(
             "explainHints-blzg1463a",// name
             "explainHints-blzg1463a.rq",// query URL
             "empty.trig",// data URL
             "explainHints-blzg1463a.srx"// results URL
          ).runTest();

          final Iterator<BOp> explainHintAnnotatedBOps = 
             ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
          assertTrue(explainHintAnnotatedBOps.hasNext());
    }
    
    /**
     * Explain hint rendering & bottom up rewriting broken for subqueries,
     * see https://jira.blazegraph.com/browse/BLZG-1463.
     * 
     * @throws Exception
     */
    public void testTicketBlzg463b() throws Exception {
       
       final ASTContainer container = new TestHelper(
             "explainHints-blzg1463b",// name
             "explainHints-blzg1463b.rq",// query URL
             "empty.trig",// data URL
             "explainHints-blzg1463b.srx"// results URL
          ).runTest();

          final Iterator<BOp> explainHintAnnotatedBOps = 
             ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
          assertTrue(explainHintAnnotatedBOps.hasNext());
    }
    
    /**
     * Explain hint rendering & bottom up rewriting broken for subqueries,
     * see https://jira.blazegraph.com/browse/BLZG-1463.
     * 
     * @throws Exception
     */
    public void testTicketBlzg463c() throws Exception {
       
       final ASTContainer container = new TestHelper(
             "explainHints-blzg1463c",// name
             "explainHints-blzg1463c.rq",// query URL
             "empty.trig",// data URL
             "explainHints-blzg1463c.srx"// results URL
          ).runTest();

          final Iterator<BOp> explainHintAnnotatedBOps = 
             ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
          assertFalse(explainHintAnnotatedBOps.hasNext());
    }
    
    /**
     * Variant of testBottomUpSemanticsExplainHint06, motivated by the fix
     * made in https://jira.blazegraph.com/browse/BLZG-1463: the idea is to
     * test a non-complex BIND expression (not containing CONCAT etc.) and
     * whether this is reported correctly.
     * 
     * @throws Exception
     */
    public void testTicketBlzg463d() throws Exception {
       
       final ASTContainer container = new TestHelper(
             "explainHints-blzg1463d",// name
             "explainHints-blzg1463d.rq",// query URL
             "explainHints.trig",// data URL
             "explainHints-blzg1463d.srx"// results URL
          ).runTest();

          final Iterator<BOp> explainHintAnnotatedBOps = 
             ExplainHints.explainHintAnnotatedBOpIterator(container.getOptimizedAST());
          assertTrue(explainHintAnnotatedBOps.hasNext());
    }

    
    /**
     * Asserts that the given {@link QueryRoot} carries exactly one explain
     * hint of the specified type.
     * 
     * @param ast the AST to check
     * @param clazz the specified type
     * 
     * @throws AssertionError if the condition is violated
     */
    private static void assertCarriesExactlyOneExplainHintOfType(final QueryRoot ast, final Class<?> clazz) {
    	
    	final Iterator<BOp> explainHintAnnotatedBOps = 
    	    	   ExplainHints.explainHintAnnotatedBOpIterator(ast);
    	    	
    	    	assertTrue(explainHintAnnotatedBOps.hasNext());
    	    	final ASTBase astBase = (ASTBase)explainHintAnnotatedBOps.next();
    	    	final ExplainHints explainHints = astBase.getExplainHints();
    	    	
    	    	final Iterator<IExplainHint> explainHintIt = explainHints.iterator();
    	    	assertTrue(explainHintIt.hasNext());
    	    	assertTrue(explainHintIt.next().getClass().equals(clazz));
    	    	assertFalse(explainHintIt.hasNext());
    	    	
    	    	assertFalse(explainHintAnnotatedBOps.hasNext());
    	    			 
    }

}
