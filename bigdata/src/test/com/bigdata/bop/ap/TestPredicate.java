/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 20, 2008
 */

package com.bigdata.bop.ap;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.Constant;
import com.bigdata.bop.EmptyBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;

/**
 * Test suite for {@link Predicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPredicate extends TestCase2 {

    /**
     * 
     */
    public TestPredicate() {
    }

    /**
     * @param name
     */
    public TestPredicate(String name) {
        super(name);
    }
    
    static private final String relation = "test";
    
    private final static Constant<Long> c1 = new Constant<Long>(1L);

    private final static Constant<Long> c2 = new Constant<Long>(2L);

    private final static Constant<Long> c3 = new Constant<Long>(3L);

    private final static Constant<Long> c4 = new Constant<Long>(4L);

    public void test_ctor() {

        {

            final Var<Long> u = Var.var("u");

            final IPredicate<?> p1 = new Predicate(new IVariableOrConstant[] {
                    u, c1, c2 }, new NV(Predicate.Annotations.RELATION_NAME,
                    new String[] { relation }));

            if (log.isInfoEnabled())
                log.info(p1.toString());

            assertEquals("arity", 3, p1.arity());
            
            // Note: test can not be written for getVariableCount(keyOrder) w/o a keyOrder impl.
//            assertEquals("variableCount", 1, p1.getVariableCount());

            assertEquals(u,p1.get(0));

            assertEquals(c1,p1.get(1));
            
            assertEquals(c2,p1.get(2));
            
        }

        {

            final Var<Long> u = Var.var("u");

            final Var<Long> v = Var.var("v");

            final IPredicate<?> p1 = new Predicate(
                    new IVariableOrConstant[] { u, c1, v }, 
                    new NV(Predicate.Annotations.RELATION_NAME,
                            new String[] { relation }));

            if (log.isInfoEnabled())
                log.info(p1.toString());

            assertEquals("arity", 3, p1.arity());

            // Note: test can not be written for getVariableCount(keyOrder) w/o a keyOrder impl.
//            assertEquals("variableCount", 2, p1.getVariableCount());

            assertEquals(u, p1.get(0));

            assertEquals(c1, p1.get(1));

            assertEquals(v, p1.get(2));
            
        }

    }
    
    /**
     * Verify equality testing with same impl.
     */
    public void test_equalsSameImpl() {

        final Var<Long> u = Var.var("u");

        final IPredicate<?> p1 = new Predicate(new IVariableOrConstant[] { u, c1, c2 },
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { relation }));

        final IPredicate<?> p2 = new Predicate(new IVariableOrConstant[] { u, c3, c4 },
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { relation }));

        if (log.isInfoEnabled()) {

            log.info(p1.toString());
            
            log.info(p2.toString());
            
        }

        assertFalse(p1.equals(p2));

        assertFalse(p2.equals(p1));

    }
    
    /**
     * Unit tests for {@link Predicate#asBound(com.bigdata.bop.IBindingSet)}
     */
    public void test_asBound_1() {

        final Var<Long> u = Var.var("u");

        final IPredicate<?> p1 = new Predicate(new IVariableOrConstant[] { u,
                c1, c2 }, new NV(Predicate.Annotations.RELATION_NAME,
                new String[] { relation }));

        assertEquals("arity", 3, p1.arity());

        // verify variables versus constants.
        assertTrue(p1.get(0).isVar());
        assertTrue(p1.get(1).isConstant());
        assertTrue(p1.get(2).isConstant());
        
        // verify object references.
        assertTrue(u == p1.get(0));
        assertTrue(c1 == p1.get(1));
        assertTrue(c2 == p1.get(2));

        // already bound on the predicate, not found in the binding set.
        doAsBoundTest(p1, EmptyBindingSet.INSTANCE);
        
        // already bound on predicate, but has different value in binding set.
        doAsBoundTest(p1, new ArrayBindingSet(new IVariable[] { u },
                new IConstant[] { c3 }));

        // not bound on the predicate, found in the binding set.
        doAsBoundTest(p1, new ArrayBindingSet(new IVariable[] { u },
                new IConstant[] { c3 }));

        // correct rejection tests.
        try {
            p1.asBound(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
    }

    private void doAsBoundTest(final IPredicate<?> p1,
            final IBindingSet bindingSet) {
        final IPredicate<?> p2 = p1.asBound(bindingSet);
        // distinct reference
        assertTrue(p1 != p2);
        // same arity.
        assertEquals(p1.arity(), p2.arity());
        // verify same data, but different references.
        for (int i = 0; i < p1.arity(); i++) {
            if(p1.get(i).isVar()) {
                /*
                 * A variable might have become bound so check the binding set
                 * for that variable.
                 */
                final IConstant<?> c = bindingSet.get((IVariable<?>) p1.get(i));
                if (c != null) {
                    /*
                     * The variable should have been bound to that constant.
                     */
                    assertTrue("i=" + i, p2.get(i).isConstant());
                    // equals (same data)
                    assertEquals("i=" + i, c, p2.get(i));
                    // same ref (no deep copy for constants).
                    assertTrue("i=" + i, c == p2.get(i));
                } else {
                    // p2 should still be a variable.
                    assertTrue("i=" + i, p2.get(i).isVar());
                    // the variable should be unchanged (same reference).
                    assertTrue("i=" + i, p1.get(i) == p2.get(i));
                }
            } else {
                /*
                 * Since not a variable in p1, the asBound variable is a
                 * constant (no deep copy for constants).
                 */
                assertTrue("i=" + i, p2.get(i).isConstant());
                // copy as equals.
                assertEquals("i=" + i, p1.get(i), p2.get(i));
                // the same references (no deep copy for constants).
                assertTrue("i=" + i, p1.get(i) == p2.get(i));
            }
        }
    }
    
    /**
     * Unit tests for {@link Predicate#asBound(int, com.bigdata.bop.IBindingSet)}
     */
    public void test_asBound_2() {

        final Var<Long> u = Var.var("u");

        final IPredicate<?> p1 = new Predicate(new IVariableOrConstant[] { u,
                c1, c2 }, new NV(Predicate.Annotations.RELATION_NAME,
                new String[] { relation }));

        assertEquals("arity", 3, p1.arity());

        // verify variables versus constants.
        assertTrue(p1.get(0).isVar());
        assertTrue(p1.get(1).isConstant());
        assertTrue(p1.get(2).isConstant());
        
        // verify object references.
        assertTrue(u == p1.get(0));
        assertTrue(c1 == p1.get(1));
        assertTrue(c2 == p1.get(2));

        // already bound on the predicate, not found in the binding set.
        assertEquals(c1.get(), p1.asBound(1, EmptyBindingSet.INSTANCE));
        
        // already bound on predicate, but has different value in binding set.
        assertEquals(c1.get(), p1.asBound(1, new ArrayBindingSet(
                new IVariable[] { u }, new IConstant[] { c3 })));

        // not bound on the predicate, found in the binding set.
        assertEquals(c3.get(), p1.asBound(0, new ArrayBindingSet(
                new IVariable[] { u }, new IConstant[] { c3 })));

        // not bound on the predicate, not found in the binding set.
        assertNull(p1.asBound(0, EmptyBindingSet.INSTANCE));

        // correct rejection tests.
        try {
            p1.asBound(-1, EmptyBindingSet.INSTANCE);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            p1.asBound(3, EmptyBindingSet.INSTANCE);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

}
