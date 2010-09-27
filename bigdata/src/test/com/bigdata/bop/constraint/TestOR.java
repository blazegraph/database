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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop.constraint;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;

/**
 * Unit tests for {@link OR}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOR extends TestCase2 {

    /**
     * 
     */
    public TestOR() {
    }

    /**
     * @param name
     */
    public TestOR(String name) {
        super(name);
    }

    /**
     * Unit test for {@link OR#OR(IConstraint,IConstraint)}
     */
    public void testConstructor ()
    {
        IConstraint eq = new EQ ( Var.var ( "x" ), Var.var ( "y" ) ) ;
        IConstraint ne = new EQ ( Var.var ( "x" ), Var.var ( "y" ) ) ;

        try { assertTrue ( null != new OR ( null, eq ) ) ; fail ( "IllegalArgumentException expected, lhs was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new OR ( eq, null ) ) ; fail ( "IllegalArgumentException expected, rhs was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        assertTrue ( null != new OR ( eq, ne ) ) ;
    }

    /**
     * Unit test for {@link OR#accept(IBindingSet)}
     */
    public void testAccept ()
    {
        Var<?> x = Var.var ( "x" ) ;
        Var<?> y = Var.var ( "y" ) ;
        Constant<Integer> val1 = new Constant<Integer> ( 1 ) ;
        Constant<Integer> val2 = new Constant<Integer> ( 2 ) ;

        IConstraint eq = new EQ ( x, y ) ;
        IConstraint eqc = new EQConstant ( y, val2 ) ;

        OR op = new OR ( eq, eqc ) ;

        IBindingSet eqlhs = new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { val1, val1 } ) ;
        IBindingSet eqrhs = new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { val1, val2 } ) ;
        IBindingSet ne = new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { val2, val1 } ) ;

        assertTrue ( op.accept ( eqlhs ) ) ;
        assertTrue ( op.accept ( eqrhs ) ) ;
        assertFalse ( op.accept ( ne ) ) ;
    }    
}