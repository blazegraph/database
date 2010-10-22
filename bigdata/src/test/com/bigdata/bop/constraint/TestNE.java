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

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;

/**
 * Unit tests for {@link NE}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNE extends TestCase2 {

    /**
     * 
     */
    public TestNE() {
    }

    /**
     * @param name
     */
    public TestNE(String name) {
        super(name);
    }

    /**
     * Unit test for {@link NE#NE(IVariable,IVariable)}
     */
    public void testConstructor ()
    {
        try { assertTrue ( null != new NE ( null, Var.var ( "y" ) ) ) ; fail ( "IllegalArgumentException expected, lhs was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new NE ( Var.var ( "x" ), null ) ) ; fail ( "IllegalArgumentException expected, rhs was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new NE ( Var.var ( "x" ), Var.var ( "x" ) ) ) ; fail ( "IllegalArgumentException expected, lhs identical to rhs" ) ; }
        catch ( IllegalArgumentException e ) {}

        assertTrue ( null != new NE ( Var.var ( "x" ), Var.var ( "y" ) ) ) ;
    }

    /**
     * Unit test for {@link NE#accept(IBindingSet)}
     */
    public void testAccept ()
    {
        Var<?> x = Var.var ( "x" ) ;
        Var<?> y = Var.var ( "y" ) ;
        Var<?> vars [] = new Var<?> [] { x, y } ;

        NE op = new NE ( x, y ) ;

        IBindingSet eq = new ArrayBindingSet ( vars, new IConstant [] { new Constant<String> ( "1" ), new Constant<String> ( "1" ) } ) ;
        IBindingSet ne = new ArrayBindingSet ( vars, new IConstant [] { new Constant<String> ( "1" ), new Constant<String> ( "2" ) } ) ;
        IBindingSet nb = new ArrayBindingSet ( new IVariable<?> [] { x }, new IConstant [] { new Constant<String> ( "1" ) } ) ;

        assertTrue ( op.accept ( ne ) ) ;
        assertFalse ( op.accept ( eq ) ) ;
        assertTrue ( op.accept ( nb ) ) ;
    }
}