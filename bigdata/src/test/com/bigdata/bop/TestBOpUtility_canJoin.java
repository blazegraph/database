/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 20, 2011
 */

package com.bigdata.bop;

import com.bigdata.bop.ap.Predicate;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link BOpUtility#canJoin(IPredicate, IPredicate)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBOpUtility_canJoin extends TestCase2 {

    /**
     * 
     */
    public TestBOpUtility_canJoin() {
    }

    /**
     * @param name
     */
    public TestBOpUtility_canJoin(String name) {
        super(name);
    }


    /**
     * Correct rejection tests.
     * 
     * @see BOpUtility#canJoin(IPredicate, IPredicate).
     */
    @SuppressWarnings("unchecked")
    public void test_canJoin_correctRejection() {
        
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");
        
        final IPredicate<?> p1 = new Predicate(new BOp[]{x,y});
        final IPredicate<?> p2 = new Predicate(new BOp[]{y,z});
        
        // correct rejection w/ null arg.
        try {
            BOpUtility.canJoin(null,p2);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // correct rejection w/ null arg.
        try {
            BOpUtility.canJoin(p1,null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
    }

    /**
     * Semantics tests focused on shared variables in the operands.
     * 
     * @see BOpUtility#canJoin(IPredicate, IPredicate)
     */
    @SuppressWarnings("unchecked")
    public void test_canJoin() {
        
        final IVariable<?> u = Var.var("u");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");

        final IPredicate<?> p1 = new Predicate(new BOp[] { x, y });
        final IPredicate<?> p2 = new Predicate(new BOp[] { y, z });
        final IPredicate<?> p3 = new Predicate(new BOp[] { u, z });

        // share y
        assertTrue(BOpUtility.canJoin(p1, p2));
        
        // share z
        assertTrue(BOpUtility.canJoin(p2, p3));
        
        // share z
        assertFalse(BOpUtility.canJoin(p1, p3));

        // shares (x,y) with self.
        assertTrue(BOpUtility.canJoin(p1, p1));

    }

    /**
     * Verify that joins are not permitted when the variables are
     * only shared via an annotation.
     * 
     * @see BOpUtility#canJoin(IPredicate, IPredicate)
     */
    @SuppressWarnings("unchecked")
    public void test_canJoin_annotationsAreIngored() {
        
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");

        final IPredicate<?> p1 = new Predicate(new BOp[] { x, },//
                new NV("foo", y)//
                );
        final IPredicate<?> p2 = new Predicate(new BOp[] { z },//
                new NV("foo", y)
                );

        // verify that the variables in the annotations are ignored.
        assertFalse(BOpUtility.canJoin(p1, p2));

    }

}
