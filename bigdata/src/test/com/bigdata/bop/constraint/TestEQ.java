/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 17, 2010
 */

package com.bigdata.bop.constraint;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;

import junit.framework.TestCase2;

/**
 * Test suite for {@link EQ}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEQ extends TestCase2 {

    /**
     * 
     */
    public TestEQ() {
    }

    /**
     * @param name
     */
    public TestEQ(String name) {
        super(name);
    }

    public void test_ctor() {
        try {
            new EQ(null/*x*/, null/*y*/);
            fail("Excepting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        try {
            new EQ(Var.var("x"), null/*y*/);
            fail("Excepting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        try {
            new EQ(Var.var("x"), Var.var("x"));
            fail("Excepting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        new EQ(Var.var("x"), Var.var("y"));
    }

    /**
     * Correct acceptance.
     */
    public void test_eval() {

        final EQ op = new EQ(Var.var("x"), Var.var("y"));

        final IBindingSet bs1 = new ArrayBindingSet(//
                new IVariable[] { Var.var("x"), Var.var("y") }, //
                new IConstant[] { new Constant<String>("1"),
                        new Constant<String>("1") });

        assertTrue(op.accept(bs1));
        
    }

    /**
     * Correct rejection for when the variables have different bindings.
     */
    public void test_eval_correct_rejection() {

        final EQ op = new EQ(Var.var("x"), Var.var("y"));

        final IBindingSet bs1 = new ArrayBindingSet(//
                new IVariable[] { Var.var("x"), Var.var("y") }, //
                new IConstant[] { new Constant<String>("1"),
                        new Constant<String>("2") });

        assertFalse(op.accept(bs1));
        
    }

}
