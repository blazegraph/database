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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBindingSet extends TestCase2 {

    /**
     * 
     */
    public TestBindingSet() {
    }

    /**
     * @param name
     */
    public TestBindingSet(String name) {
        super(name);
    }

    /**
     * Unit test for {@link ArrayBindingSet#copy(IVariable[])}
     */
    public void test_copy_abs() {
        
        IVariable[] vars = new IVariable[] {
                Var.var("a"),
                Var.var("b"),
                Var.var("c"),
                Var.var("d"),
                Var.var("e")
        };
        
        IConstant[] vals = new IConstant[] {
                new Constant<Integer>(1),
                new Constant<Integer>(2),
                new Constant<Integer>(3),
                new Constant<Integer>(4),
                new Constant<Integer>(5)
        };
        
        ArrayBindingSet bs = new ArrayBindingSet(vars, vals);
        
        assertTrue(bs.size() == 5);
        for (IVariable v : vars) {
            assertTrue(bs.isBound(v));
        }
        
        IVariable[] varsToKeep = new IVariable[] {
                Var.var("a"),
                Var.var("c"),
                Var.var("e")
        };
        
        ArrayBindingSet bs2 = bs.copy(varsToKeep);
        assertTrue(bs2.size() == 3);
        for (IVariable v : varsToKeep) {
            assertTrue(bs2.isBound(v));
            assertTrue(bs2.get(v).equals(bs.get(v)));
        }
        assertFalse(bs2.isBound(Var.var("b")));
        assertFalse(bs2.isBound(Var.var("d")));
        
    }

    /**
     * Unit test for {@link HashBindingSet#copy(IVariable[])}
     */
    public void test_copy_hbs() {
        
        IVariable[] vars = new IVariable[] {
                Var.var("a"),
                Var.var("b"),
                Var.var("c"),
                Var.var("d"),
                Var.var("e")
        };
        
        IConstant[] vals = new IConstant[] {
                new Constant<Integer>(1),
                new Constant<Integer>(2),
                new Constant<Integer>(3),
                new Constant<Integer>(4),
                new Constant<Integer>(5)
        };
        
        HashBindingSet bs = new HashBindingSet();
        for (int i = 0; i < vars.length; i++) {
            bs.set(vars[i], vals[i]);
        }
        
        assertTrue(bs.size() == 5);
        for (IVariable v : vars) {
            assertTrue(bs.isBound(v));
        }
        
        IVariable[] varsToKeep = new IVariable[] {
                Var.var("a"),
                Var.var("c"),
                Var.var("e")
        };
        
        HashBindingSet bs2 = bs.copy(varsToKeep);
        assertTrue(bs2.size() == 3);
        for (IVariable v : varsToKeep) {
            assertTrue(bs2.isBound(v));
            assertTrue(bs2.get(v).equals(bs.get(v)));
        }
        assertFalse(bs2.isBound(Var.var("b")));
        assertFalse(bs2.isBound(Var.var("d")));
        
    }

    /**
     * @todo Write unit tests for equals which verify that binding sets are
     *       equals iff they have the same variables and those variables have
     *       the same bindings.
     */
    public void test_equals() {
        fail("write tests");
    }

    /**
     * @todo Write unit tests for the hash code which demonstrate that the hash
     *       code of binding sets with the same bindings on the same variables
     *       is the same regardless of the order in which those variables
     *       appear.
     */
    public void test_hashCode() {
        fail("write tests");
    }

    /**
     * @todo Write unit tests for the hash code which demonstrate that the hash
     *       code of the binding sets is invalidated and recomputed when there
     *       is a mutation to the binding set. Do this for all
     *       {@link IBindingSet} implementations.
     */
    public void test_hashCode_mutations() {
        fail("write tests");
    }

}
