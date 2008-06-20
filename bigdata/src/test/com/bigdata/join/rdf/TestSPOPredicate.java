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

package com.bigdata.join.rdf;

import junit.framework.TestCase2;

import com.bigdata.join.Constant;
import com.bigdata.join.IBindingSet;
import com.bigdata.join.IVariableOrConstant;
import com.bigdata.join.Predicate;
import com.bigdata.join.Var;

/**
 * Test suite for {@link SPOPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOPredicate extends TestCase2 {

    /**
     * 
     */
    public TestSPOPredicate() {
    }

    /**
     * @param name
     */
    public TestSPOPredicate(String name) {
        super(name);
    }

    final static Constant<Long> rdfsSubClassOf = new Constant<Long>(
            1L);
    
    final static Constant<Long> rdfsResource = new Constant<Long>(
            2L);
    
    final static Constant<Long> rdfType = new Constant<Long>(
            3L);
    
    final static Constant<Long> rdfsClass = new Constant<Long>(
            4L);
    
    public void test_ctor() {

        {

            final Var<Long> u = Var.var("u");

            final SPOPredicate p1 = new SPOPredicate(u, rdfsSubClassOf,
                    rdfsResource);
            
            log.info(p1.toString());

            assertEquals("arity",3,p1.arity());
            
            assertEquals("variableCount", 1, p1.getVariableCount());

            assertEquals(u,p1.get(0));

            assertEquals(rdfsSubClassOf,p1.get(1));
            
            assertEquals(rdfsResource,p1.get(2));
            
        }

        {

            final Var<Long> u = Var.var("u");

            final Var<Long> v = Var.var("v");

            final SPOPredicate p1 = new SPOPredicate(u, rdfsSubClassOf, v);

            log.info(p1.toString());

            assertEquals("arity", 3, p1.arity());

            assertEquals("variableCount", 2, p1.getVariableCount());

            assertEquals(u, p1.get(0));

            assertEquals(rdfsSubClassOf, p1.get(1));

            assertEquals(v, p1.get(2));
            
        }

    }
    
    /**
     * Verify equality testing with same impl.
     */
    public void test_equalsSameImpl() {

        final Var<Long> u = Var.var("u");

        final SPOPredicate p1 = new SPOPredicate(u, rdfsSubClassOf, rdfsResource);

        final SPOPredicate p2 = new SPOPredicate(u, rdfType, rdfsClass);

        log.info(p1.toString());

        log.info(p2.toString());

        assertTrue(p1.equals(new SPOPredicate(u, rdfsSubClassOf, rdfsResource)));

        assertTrue(p2.equals(new SPOPredicate(u, rdfType, rdfsClass)));
        
        assertFalse(p1.equals(p2));

        assertFalse(p2.equals(p1));
        
    }
    
    public void test_equalsDifferentImpl() {
        
        final Var<Long> u = Var.var("u");

        final SPOPredicate p1 = new SPOPredicate(u, rdfType, rdfsClass);

        final Predicate p2 = new Predicate(new IVariableOrConstant[] { u,
                rdfType, rdfsClass }){

                    @Override
                    public Predicate asBound(IBindingSet bindingSet) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public void copyValues(Object e, IBindingSet bindingSet) {
                        // TODO Auto-generated method stub
                        
                    }};

        log.info(p1.toString());

        log.info(p2.toString());

        assertTrue(p1.equals(p2));

        assertTrue(p2.equals(p1));

    }
    
}
