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

import com.bigdata.bop.Constant;
import com.bigdata.bop.Distinct;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.relation.accesspath.IElementFilter;

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

            final IPredicate<?> p1 = new P(
                    new IVariableOrConstant[] { u, c1, c2 }, relation);
            
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

            final IPredicate<?> p1 = new P(
                    new IVariableOrConstant[] { u, c1, v }, relation);

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

        final IPredicate<?> p1 = new P(new IVariableOrConstant[] { u, c1, c2 },
                relation);

        final IPredicate<?> p2 = new P(new IVariableOrConstant[] { u, c3, c4 },
                relation);

        if (log.isInfoEnabled()) {

            log.info(p1.toString());
            
            log.info(p2.toString());
            
        }

        assertFalse(p1.equals(p2));

        assertFalse(p2.equals(p1));

    }

    /**
     * Unit test for reading on an index of some relation.
     * 
     * @todo layering in {@link IElementFilter}s.
     * @todo layering in a {@link Distinct} filter.
     * @todo verify the correct reporting of access path statistics.
     * @todo write unit tests for the {@link Union} operator.
     */
    public void test_scan() {
        
        fail("implement");
        
    }
    
    static protected class P<E> extends Predicate<E> {

        public P(final IVariableOrConstant<?>[] values, final String relation) {

            super(values, relation);

        }

    }

}
