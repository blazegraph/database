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
 * Created on Oct 26, 2007
 */

package com.bigdata.relation.rule;

import java.util.Set;

/**
 * Test suite for basic {@link Rule} mechanisms.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRule extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRule() {
        super();
    }

    /**
     * @param name
     */
    public TestRule(String name) {
        super(name);
    }
    
    private final String relation = "test";

    /**
     * Verify constructor of a simple rule.
     */
    public void test_ctor() {

        final Var<Long> u = Var.var("u");

        final IRule r = new MyRule(
                // head
                new P(relation, u, rdfsSubClassOf, rdfsResource),
                // tail
                new IPredicate[] {//
                    new P(relation, u, rdfType, rdfsClass) //
                }
                );

        // write out the rule on the console.
        System.err.println(r.toString());

        assertEquals("variableCount", 1, r.getVariableCount());

        assertTrue("head", new P(relation, u, rdfsSubClassOf, rdfsResource)
                .equals(r.getHead()));

        assertTrue("tail[0]", new P(relation, u, rdfType, rdfsClass).equals(r
                .getTail(0)));

        assertSameIteratorAnyOrder(new Comparable[] { u }, r.getVariables());

        assertTrue(r.isDeclared(u));

        assertFalse(r.isDeclared(Var.var("x")));
        
    }

    /**
     * Test for computing the intersection of the variables in two predicates.
     */
    public void test_getSharedVars() {

        final IPredicate p1 = new P(relation,//
                Var.var("u"), rdfsSubClassOf, rdfsResource);

        final IPredicate p2 = new P(relation,//
                Var.var("x"), rdfType, Var.var("u"));

        Set<IVariable> actual = Rule.getSharedVars(p1, p2);

        assertEquals(1, actual.size());

        assertTrue(actual.contains(Var.var("u")));
        
    }
    
    /**
     * Test the ability to compute the variables shared between two {@link Pred}s
     * in a {@link Rule}.
     */
    public void test_getSharedVarsInTail() {

        final IRule r = new TestRuleRdfs9(relation);
        
        final Set<IVariable> shared = r.getSharedVars(0, 1);

        assertTrue(shared.contains(Rule.var("u")));

        assertFalse(shared.contains(Rule.var("v")));

        assertEquals(1,shared.size());

    }
    
    /**
     * Verify variable binding stuff for a rule.
     */
    public void test_ruleBindings() {

        final Var<Long> u = Var.var("u");

        final IPredicate head = new P(relation, u, rdfsSubClassOf, rdfsResource);

        final IPredicate[] body = new IPredicate[] {//

                new P(relation, u, rdfType, rdfsClass)//
                
        };

        final IRule r = new MyRule(head, body);

        final IBindingSet bindingSet = new HashBindingSet();
        
        // verify body[0] is not fully bound.
        assertFalse(r.isFullyBound(0,bindingSet));

        // verify you can overwrite a variable in the tail.
        bindingSet.set(u, new Constant<Long>(1L));

        assertTrue(r.isFullyBound(0,bindingSet)); // is fully bound.
        
        bindingSet.clearAll(); // restore the bindings.
        
        assertFalse(r.isFullyBound(0,bindingSet)); // no longer fully bound.
        
    }
    
    /**
     * Verify that constraint violations are being tested (specific
     * {@link IConstraint}s are tested elsewhere).
     */
    public void test_constraints() {

        /*
         *  (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
         *  
         *  Note: u != x
         */
        final IRule r = new TestRuleRdfs9(relation);

        final IBindingSet bindingSet = new HashBindingSet();
//        final IBindingSet bindingSet = new ArrayBindingSet(3);
        
        final IVariable u = Var.var("u");

        final IVariable x = Var.var("x");
        
        // set a binding
        bindingSet.set(u, rdfsClass);

        // write on the console.
        log.info(bindingSet.toString());
        log.info(r.toString(bindingSet));
        
        // verify bindings are Ok.
        assertTrue(r.isConsistent(bindingSet));

        /*
         * Now try to bind [x] to a different constant with a different value
         * and verify no violation is detected.
         */

        bindingSet.set(x, rdfsResource);
        
        // write on the console.
        log.info(bindingSet.toString());
        log.info(r.toString(bindingSet));
        
        // verify bindings are Ok.
        assertTrue(r.isConsistent(bindingSet));

        /*
         * Now try to re-bind [x] to the same constant as [u] and verify that
         * the violation of the [u != x] constraint is detected.
         */

        bindingSet.set(x, rdfsClass);
        
        // write on the console.
        log.info(bindingSet.toString());
        log.info(r.toString(bindingSet));
        
        // verify bindings are illegal.
        assertFalse(r.isConsistent(bindingSet));

        /*
         * Now try to bind [x] to a distinct constant having the same value and
         * verify that the violation of the [u != x] constraint is detected.
         */

        // spot check equals() for the Constant.
        assertTrue(rdfsClass.equals(new Constant<Long>(rdfsClass.get())));
        
        // re-bind [x].
        bindingSet.set(x, new Constant<Long>(rdfsClass.get()));
        
        // write on the console.
        log.info(bindingSet.toString());
        log.info(r.toString(bindingSet));
        
        // verify bindings are illegal.
        assertFalse(r.isConsistent(bindingSet));

        /*
         * Clear the binding for [u] and verify that the bindings are then legal.
         */
        bindingSet.clear(u);
        
        // write on the console.
        log.info(bindingSet.toString());
        log.info(r.toString(bindingSet));
        
        // verify bindings are Ok.
        assertTrue(r.isConsistent(bindingSet));
        
    }

    /**
     * Test case for specializing a rule by binding some of its variables.
     * 
     * @todo test adding constraints.
     */
    public void test_specializeRule() {

        // (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
        final IRule r = new TestRuleRdfs9(relation);

        log.info(r.toString());

        {

            /*
             * Verify we can override a variable with a constant.
             */

            final IBindingSet bindingSet = new ArrayBindingSet(//
                    new IVariable[] { Var.var("v") },//
                    new IConstant[] { rdfProperty }//
            );

            final IRule r1 = r
                    .specialize("r1", bindingSet, new IConstraint[] {});

            log.info(r1.toString());

            // verify "v" bound in body[1].
            assertTrue(r1.getTail(1).get(0).isConstant());

            assertTrue(rdfProperty == r1.getTail(1).get(0));

        }

        {
            /*
             * Verify we can override another variable with a constant.
             */

            final IBindingSet bindingSet = new ArrayBindingSet(//
                    new IVariable[] { Var.var("x") },//
                    new IConstant[] { rdfProperty }//
            );
            
            final IRule r2 = r
                    .specialize("r2", bindingSet, new IConstraint[] {});

            log.info(r2.toString());

            // verify "x" bound in body[0].
            assertTrue(r2.getTail(0).get(2).isConstant());
            
            assertTrue(rdfProperty == r2.getTail(0).get(2));

        }

    }

}
