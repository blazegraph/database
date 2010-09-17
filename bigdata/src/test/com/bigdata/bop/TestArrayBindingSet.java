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


/**
 * Unit tests for {@link ArrayBindingSet}.
 * 
 * Note:
 * a) these tests assume that the values held for a given key are not cloned,
 *    i.e. comparison is done by '==' and not '.equals'
 * b) keys with the same 'name' are a unique object.
 * 
 * @author <a href="mailto:dmacgbr@users.sourceforge.net">David MacMillan</a>
 * @version $Id$
 */
public class TestArrayBindingSet extends TestIBindingSet
{
    /**
     * 
     */
    public TestArrayBindingSet () {}

    /**
     * @param name
     */
    public TestArrayBindingSet ( String name ) { super ( name ) ; }

    /**
     * Unit test for {@link ArrayBindingSet#ArrayBindingSet(ArrayBindingSet)}
     */
    public void testConstructorArrayBindingSet ()
    {
        try { assertTrue ( null != new ArrayBindingSet ( null ) ) ; fail ( "IllegalArgumentException expected, copy from was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        Var<?> var1 = Var.var ( "a" ) ;
        Var<?> var2 = Var.var ( "b" ) ;
        Constant<Integer> val1 = new Constant<Integer> ( 1 ) ;
        Constant<Integer> val2 = new Constant<Integer> ( 2 ) ;
        IVariable<?> vars [] = new IVariable [] { var1, var2 } ;
        IConstant<?> vals [] = new IConstant [] { val1, val2 } ;

        assertEqual ( new ArrayBindingSet ( new ArrayBindingSet ( vars, vals ) ), vars, vals ) ;
    }

    /**
     * Unit test for {@link ArrayBindingSet#ArrayBindingSet(IVariable[],IConstant[])}
     */
    public void testConstructorVariablesConstants ()
    {
        Var<?> var1 = Var.var ( "a" ) ;
        Var<?> var2 = Var.var ( "b" ) ;
        Constant<Integer> val1 = new Constant<Integer> ( 1 ) ;
        Constant<Integer> val2 = new Constant<Integer> ( 2 ) ;
        IVariable<?> vars [] = new IVariable [] { var1, var2 } ;
        IConstant<?> vals [] = new IConstant [] { val1, val2 } ;

        try { assertTrue ( null != new ArrayBindingSet ( null, vals ) ) ; fail ( "IllegalArgumentException expected, vars was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new ArrayBindingSet ( vars, null ) ) ; fail ( "IllegalArgumentException expected, vals was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new ArrayBindingSet ( vars, new IConstant [] { val1 } ) ) ; fail ( "IllegalArgumentException expected, vars and vals were different sizes" ) ; }
        catch ( IllegalArgumentException e ) {}

        assertEqual ( new ArrayBindingSet ( vars, vals ), vars, vals ) ;
    }

    /**
     * Unit test for {@link ArrayBindingSet#ArrayBindingSet(int)}
     */
    public void testConstructorInt ()
    {
        try { assertTrue ( null != new ArrayBindingSet ( -1 ) ) ; fail ( "IllegalArgumentException expected, capacity was negative" ) ; }
        catch ( IllegalArgumentException e ) {}

        assertEqual ( new ArrayBindingSet ( 2 ), new IVariable [] {}, new IConstant [] {} ) ;
    }

    @Override protected IBindingSet newBindingSet ( IVariable<?> vars [], IConstant<?> vals [] ) { return new ArrayBindingSet ( vars, vals ) ; }
    @Override protected IBindingSet newBindingSet ( int size ) { return new ArrayBindingSet ( size ) ; }
}