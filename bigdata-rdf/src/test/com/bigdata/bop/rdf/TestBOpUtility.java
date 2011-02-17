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
 * Created on Aug 27, 2010
 */

package com.bigdata.bop.rdf;

import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.Var;
import com.bigdata.bop.constraint.BOpConstraint;
import com.bigdata.bop.constraint.OR;

/**
 * Unit tests for {@link BOpUtility}.
 */
public class TestBOpUtility extends TestCase2 {

    /**
     * 
     */
    public TestBOpUtility() {
    }

    /**
     * @param name
     */
    public TestBOpUtility(String name) {
        super(name);
    }

    private void eatData(/*final int expectedLength, */final Iterator<?> itr) {
    	int i = 1;
        while (itr.hasNext()) {
            final Object t = itr.next();
//            System.err.print(i+" ");// + " : " + t);
//            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
//                    + t, expected[i].equals(t));
            i++;
        }
//        System.err.println("");
//        assertEquals("#visited", expectedLength, i);
    }

    private BOp generateBOp(final int count,final IValueExpression<?> a) {
    	
    	IConstraint bop = null;
    	
    	for (int i = 0; i < count; i++) {
    		
        	final IConstraint c = new DummyConstraint(
        			new BOp[] { a, new Constant<Integer>(i) }, 
        			null/*annotations*/); 
        	
    		if (bop == null) {
    			bop = c;
    		} else {
    			bop = new OR(c, bop);
    		}
    		
    	}
    	
    	return bop;
    	
    }
    
    /**
     * Unit test for {@link BOpUtility#getSpannedVariables(BOp)}.
     */
    public void test_getSpannedVariables() {

    	final IValueExpression<?> a = Var.var("a");

    	System.err.println("depth, millis");
		final int ntrials = 2000;
		for (int count = 1; count < ntrials; count++) {
			final BOp bop = generateBOp(count, a);
			final long begin = System.currentTimeMillis();
			System.err.print(count);
			eatData(BOpUtility.preOrderIterator(bop));
			final long elapsed = System.currentTimeMillis() - begin;
			System.err.print(", ");
			System.err.print(elapsed);
			System.err.print("\n");
		}

//        System.err.println("preOrderIteratorWithAnnotations");
//        eatData(BOpUtility.preOrderIteratorWithAnnotations(bop));
//
//        System.err.println("getSpannedVariables");
//        eatData(BOpUtility.getSpannedVariables(bop));
//
//        // @todo make the returned set distinct?
//        
//        final Object[] expected = new Object[]{//
//                a,//
//        };
//        // @todo verify the actual data visited.
//		assertSameIterator(expected, BOpUtility.getSpannedVariables(bop));
        
    }
    
    private static class DummyConstraint extends BOpConstraint {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1942393209821562541L;

		public DummyConstraint(BOp[] args, Map<String, Object> annotations) {
			super(args, annotations);
		}

		public DummyConstraint(BOpBase op) {
			super(op);
		}

		public boolean accept(IBindingSet bindingSet) {
    		throw new RuntimeException();
    	}
    }

}
