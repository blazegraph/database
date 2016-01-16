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
 * Created on Sep 29, 2011
 */

package com.bigdata.bop.rdf;

import junit.framework.TestCase2;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Test suite for logic which "joins" two solutions, propagating bindings,
 * verifying constraints, and dropping bindings which are not to be kept.
 * 
 * @see BOpContext#bind(IBindingSet, IBindingSet, boolean, IConstraint[],
 *      IVariable[])
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit test for application of constraints.
 * 
 *          TODO Unit test for filtering of the variables.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestBind extends TestCase2 {

    /**
     * 
     */
    public TestBind() {
    }

    /**
     * @param name
     */
    public TestBind(String name) {
        super(name);
    }

    /**
     * Unit test for join of two empty solutions.
     */
    public void test_bind01() {
        
        final ListBindingSet expected = new ListBindingSet();
        final ListBindingSet left = new ListBindingSet();
        final ListBindingSet right = new ListBindingSet();
        
        final IBindingSet actual = BOpContext.bind(left, right, //true/*leftIsPipeline*/,
                null/*constraints*/, null/*varsToKeep*/);
        
        assertEquals(expected, actual);
        
    }

    /**
     * Unit test for join of an empty solution and a non-empty solution.
     */
    public void test_bind02() {

        final IVariable<?> x = Var.var("x");
        
        final IConstant<?> val = new Constant(new XSDNumericIV<BigdataLiteral>(
                1)); 
        
        final ListBindingSet expected = new ListBindingSet();
        expected.set(x, val);
        
        final ListBindingSet left = new ListBindingSet();
        left.set(x, val);
        
        final ListBindingSet right = new ListBindingSet();
        
        final IBindingSet actual = BOpContext
                .bind(left, right, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertEquals(expected, actual);

        // Test symmetry.
        final IBindingSet actual2 = BOpContext
                .bind(right, left, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertEquals(expected, actual2);
        
    }

    /**
     * Unit test for join of two non-empty solutions which are consistent.
     */
    public void test_bind03() {

        final IVariable<?> x = Var.var("x");

        final IConstant<?> val = new Constant(new XSDNumericIV<BigdataLiteral>(
                1));

        final ListBindingSet expected = new ListBindingSet();
        expected.set(x, val);

        final ListBindingSet left = new ListBindingSet();
        left.set(x, val);

        final ListBindingSet right = new ListBindingSet();
        right.set(x, val);

        final IBindingSet actual = BOpContext
                .bind(left, right, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertEquals(expected, actual);

        // Test symmetry.
        final IBindingSet actual2 = BOpContext
                .bind(left, right, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertEquals(expected, actual2);

    }

    /**
     * Unit test for join of two non-empty solutions which are not consistent.
     */
    public void test_bind04() {

        final IVariable<?> x = Var.var("x");

        final IConstant<?> val1 = new Constant(
                new XSDNumericIV<BigdataLiteral>(1));

        final IConstant<?> val2 = new Constant(
                new XSDNumericIV<BigdataLiteral>(2));

        final ListBindingSet left = new ListBindingSet();
        left.set(x, val1);

        final ListBindingSet right = new ListBindingSet();
        right.set(x, val2);

        final IBindingSet actual = BOpContext
                .bind(left, right, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertNull(actual);

        // Test symmetry.
        final IBindingSet actual2 = BOpContext
                .bind(left, right, //true/* leftIsPipeline */,
                        null/* constraints */, null/* varsToKeep */);

        assertNull(actual2);

    }

    /**
     * Unit test for join of two consistent solutions when only of them has the
     * {@link BigdataValue} cached on the {@link IV} and the other does not. The
     * cached reference should be propagated to the result.
     */
    public void test_bind05() {

        final BigdataValueFactory f = BigdataValueFactoryImpl
                .getInstance(getName());
        
        final BigdataLiteral lit = f.createLiteral(1);
        
        final IVariable<?> x = Var.var("x");
        
        final IV iv1 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv2 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv3 = new XSDNumericIV<BigdataLiteral>(1);
        final IConstant<?> val1 = new Constant(iv1);
        final IConstant<?> val2 = new Constant(iv2);
        final IConstant<?> val3 = new Constant(iv3);
        iv1.setValue((BigdataValue) lit);
        iv3.setValue((BigdataValue) lit);

        final ListBindingSet expected = new ListBindingSet();
        expected.set(x, val3);
        
        final ListBindingSet left = new ListBindingSet();
        left.set(x, val1);
        
        final ListBindingSet right = new ListBindingSet();
        right.set(x, val2);
        
        final IBindingSet actual = BOpContext.bind(left, right, //true/*leftIsPipeline*/,
                null/*constraints*/, null/*varsToKeep*/);
        
        assertEquals(expected, actual);
        
        assertEquals(iv3.getValue(), ((IV)actual.get(x).get()).getValue());
        
    }

    /**
     * A variant on {@link #test_bind05()} where the {@link IV} having the
     * cached {@link BigdataValue} is in the other source solution (test of
     * symmetry).
     */
    public void test_bind05b() {

        final BigdataValueFactory f = BigdataValueFactoryImpl
                .getInstance(getName());
        
        final BigdataLiteral lit = f.createLiteral(1);
        
        final IVariable<?> x = Var.var("x");
        
        final IV iv1 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv2 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv3 = new XSDNumericIV<BigdataLiteral>(1);
        final IConstant<?> val1 = new Constant(iv1);
        final IConstant<?> val2 = new Constant(iv2);
        final IConstant<?> val3 = new Constant(iv3);
        iv2.setValue((BigdataValue) lit);
        iv3.setValue((BigdataValue) lit);

        final ListBindingSet expected = new ListBindingSet();
        expected.set(x, val3);
        
        final ListBindingSet left = new ListBindingSet();
        left.set(x, val1);
        
        final ListBindingSet right = new ListBindingSet();
        right.set(x, val2);
        
        final IBindingSet actual = BOpContext.bind(left, right, //true/*leftIsPipeline*/,
                null/*constraints*/, null/*varsToKeep*/);
        
        assertEquals(expected, actual);
        
        assertEquals(iv3.getValue(), ((IV)actual.get(x).get()).getValue());
        
    }

    /**
     * Unit test for Constant/2 semantics. The value of the constant needs to be
     * propagated onto the named variable.
     */
    public void test_bind06() {

        final BigdataValueFactory f = BigdataValueFactoryImpl
                .getInstance(getName());
        
        final BigdataLiteral lit = f.createLiteral(1);
        
        final IVariable<?> x = Var.var("x");
        
        final IV iv1 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv2 = new XSDNumericIV<BigdataLiteral>(1);
        final IV iv3 = new XSDNumericIV<BigdataLiteral>(1);
        final IConstant<?> val1 = new Constant(iv1);
        final IConstant<?> val2 = new Constant(iv2);
        final IConstant<?> val3 = new Constant(iv3);
        iv2.setValue((BigdataValue) lit);
        iv3.setValue((BigdataValue) lit);

        final ListBindingSet expected = new ListBindingSet();
        expected.set(x, val3);
        
        final ListBindingSet left = new ListBindingSet();
        left.set(x, val1);
        
        final ListBindingSet right = new ListBindingSet();
        right.set(x, val2);
        
        final IBindingSet actual = BOpContext.bind(left, right, //true/*leftIsPipeline*/,
                null/*constraints*/, null/*varsToKeep*/);
        
        assertEquals(expected, actual);
        
        assertEquals(iv3.getValue(), ((IV)actual.get(x).get()).getValue());
        
    }

}
