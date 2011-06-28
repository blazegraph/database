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

package com.bigdata.bop.bindingSet;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;

/**
 * Unit tests for {@link ListBindingSet}.
 * 
 * @author <a href="mailto:dmacgbr@users.sourceforge.net">David MacMillan</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestListBindingSet extends TestIBindingSet
{

    /**
     * 
     */
    public TestListBindingSet () {}

    /**
     * @param name
     */
    public TestListBindingSet ( String name ) { super ( name ) ; }

    /**
     * Unit test for {@link ListBindingSet#ListBindingSet()}
     */
    public void testConstructorListBindingSet ()
    {
        assertTrue ( null != new ListBindingSet () ) ;
    }

    /**
     * Unit test for {@link ListBindingSet#clone()}
     */
    public void testClone ()
    {

        Var<?> var1 = Var.var ( "a" ) ;
        Var<?> var2 = Var.var ( "b" ) ;
        Constant<Integer> val1 = new Constant<Integer> ( 1 ) ;
        Constant<Integer> val2 = new Constant<Integer> ( 2 ) ;
        IVariable<?> vars [] = new IVariable [] { var1, var2 } ;
        IConstant<?> vals [] = new IConstant [] { val1, val2 } ;

        assertEqual ( new ListBindingSet ( vars, vals ), vars, vals ) ;
        assertEqual ( new ListBindingSet ( vars, vals ).clone(), vars, vals ) ;
    }

    /**
     * Unit test for {@link ListBindingSet#ListBindingSet(IVariable[],IConstant[])}
     */
    public void testConstructorVariablesConstants ()
    {
        Var<?> var1 = Var.var ( "a" ) ;
        Var<?> var2 = Var.var ( "b" ) ;
        Constant<Integer> val1 = new Constant<Integer> ( 1 ) ;
        Constant<Integer> val2 = new Constant<Integer> ( 2 ) ;
        IVariable<?> vars [] = new IVariable [] { var1, var2 } ;
        IConstant<?> vals [] = new IConstant [] { val1, val2 } ;

        try { assertTrue ( null != new ListBindingSet ( null, vals ) ) ; fail ( "IllegalArgumentException expected, vars was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new ListBindingSet ( vars, null ) ) ; fail ( "IllegalArgumentException expected, vals was null" ) ; }
        catch ( IllegalArgumentException e ) {}

        try { assertTrue ( null != new ListBindingSet ( vars, new IConstant [] { val1 } ) ) ; fail ( "IllegalArgumentException expected, vars and vals were different sizes" ) ; }
        catch ( IllegalArgumentException e ) {}

        assertEqual ( new ListBindingSet ( vars, vals ), vars, vals ) ;
    }

	@Override
	protected IBindingSet newBindingSet(IVariable<?> vars[],
			IConstant<?> vals[]) {

		return new ListBindingSet(vars, vals);
	
	}

	@Override
	protected IBindingSet newBindingSet(int size) {
	
		return new ListBindingSet();
		
	}

}
